import streamlit as st
import pandas as pd
import numpy as np
import io
import zipfile
import base64
import xlsxwriter
from datetime import datetime

# --- 1. GLOBAL CONFIGURATION ---
st.set_page_config(layout="wide", page_title="E-commerce Reconciliation Master Tool")

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("🔧 Navigation")
tool_selection = st.sidebar.selectbox("Select Platform:", ["Amazon Reconciliation", "Ajio Reconciliation"])
st.sidebar.markdown("---")

# ==========================================
# MODULE 1: AMAZON RECONCILIATION (GREEN EXPENSES FIXED)
# ==========================================
def run_amazon_tool():
    # --- HELPER FUNCTIONS ---
    @st.cache_data
    def create_cost_sheet_template():
        template_data = {
            'SKU': ['ExampleSKU-001', 'ExampleSKU-002'],
            'Product Cost': [150.50, 220.00]
        }
        df = pd.DataFrame(template_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Cost_Sheet_Template', index=False)
        return output.getvalue()

    def process_cost_sheet(uploaded_file):
        required_cols = ['SKU', 'Product Cost']
        try:
            filename = uploaded_file.name.lower()
            if filename.endswith(('.xlsx', '.xls')):
                df_cost = pd.read_excel(uploaded_file)
            elif filename.endswith(('.csv')):
                try:
                    df_cost = pd.read_csv(uploaded_file, encoding='utf-8')
                except UnicodeDecodeError:
                    df_cost = pd.read_csv(uploaded_file, encoding='latin-1')
                uploaded_file.seek(0)
            else:
                st.error(f"Error reading Cost Sheet: Unsupported file type.")
                return pd.DataFrame()
            
            df_cost.columns = [str(col).strip() for col in df_cost.columns]
            missing_cols = [col for col in required_cols if col not in df_cost.columns]
            if missing_cols:
                st.error(f"Cost Sheet Error: Missing columns: {', '.join(missing_cols)}")
                return pd.DataFrame()
            
            df_cost.rename(columns={'SKU': 'Sku'}, inplace=True)
            df_cost['Sku'] = df_cost['Sku'].astype(str)
            df_cost['Product Cost'] = pd.to_numeric(df_cost['Product Cost'], errors='coerce').fillna(0)
            return df_cost.groupby('Sku')['Product Cost'].mean().reset_index(name='Product Cost')
        except Exception as e:
            st.error(f"Error reading Cost Sheet: {e}")
            return pd.DataFrame()

    @st.cache_data
    def convert_to_excel(df):
        output = io.BytesIO()
        df_excel = df.copy()
        numeric_cols = [
            'MTR Invoice Amount', 'Net Payment', 'Total_Commission_Fee',
            'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee',
            'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee',
            'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 'Product Cost',
            'Product Profit/Loss', 'Quantity'
        ]
        for col in numeric_cols:
            if col in df_excel.columns:
                df_excel[col] = pd.to_numeric(df_excel[col], errors='coerce').fillna(0)
                if col != 'Quantity':
                    df_excel[col] = df_excel[col].round(2)
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_excel.to_excel(writer, sheet_name='Reconciliation_Summary', index=False)
        return output.getvalue()

    def calculate_fee_total(df, keyword, name):
        if 'amount-description' not in df.columns:
            return pd.DataFrame({'OrderID': pd.Series(dtype='str'), name: pd.Series(dtype='float')})
        df_filtered = df.dropna(subset=['amount-description'])
        df_fee = df_filtered[df_filtered['amount-description'].astype(str).str.contains(keyword, case=False, na=False)]
        if df_fee.empty:
            return pd.DataFrame({'OrderID': pd.Series(dtype='str'), name: pd.Series(dtype='float')})
        df_summary = df_fee.groupby('OrderID')['amount'].sum().reset_index(name=name)
        df_summary[name] = df_summary[name].abs()
        return df_summary

    def process_payment_zip_file(uploaded_zip_file):
        payment_files = []
        try:
            with zipfile.ZipFile(uploaded_zip_file, 'r') as zf:
                for name in zf.namelist():
                    if name.startswith('__MACOSX/') or name.endswith('/') or name.startswith('.'):
                        continue
                    if name.lower().endswith('.txt'):
                        file_content_bytes = zf.read(name)
                        pseudo_file = type('FileUploaderObject', (object,), {
                            'name': name,
                            'getvalue': lambda *args, b=file_content_bytes: b,
                            'read': lambda *args, b=file_content_bytes: b
                        })()
                        payment_files.append(pseudo_file)
        except Exception as e:
            st.error(f"Error unzipping {uploaded_zip_file.name}: {e}")
            return []
        return payment_files

    def process_payment_files(uploaded_payment_files):
        all_payment_data = []
        required_cols_lower = ['order-id', 'amount-description', 'amount']
        for file in uploaded_payment_files:
            try:
                file_content = None
                try:
                    file_content = file.getvalue().decode("utf-8")
                except UnicodeDecodeError:
                    try:
                        file_content = file.getvalue().decode("latin-1")
                    except:
                        continue
                if file_content is None: continue
                
                chunk_iter = pd.read_csv(io.StringIO(file_content), sep='\t', skipinitialspace=True, header=0, chunksize=100000)
                first_chunk = True
                for chunk in chunk_iter:
                    chunk.columns = [str(col).strip().lower() for col in chunk.columns]
                    if first_chunk:
                        if not all(col in chunk.columns for col in required_cols_lower):
                            st.error(f"Missing columns in {file.name}")
                            return pd.DataFrame(), pd.DataFrame()
                        first_chunk = False
                    if 'order-id' in chunk.columns: chunk.dropna(subset=['order-id'], inplace=True)
                    else: continue
                    if all(col in chunk.columns for col in required_cols_lower):
                        all_payment_data.append(chunk[required_cols_lower].copy())
            except Exception: continue

        if not all_payment_data: return pd.DataFrame(), pd.DataFrame()
        df_charge = pd.concat(all_payment_data, ignore_index=True)
        df_charge.rename(columns={'order-id': 'OrderID'}, inplace=True)
        df_charge['OrderID'] = df_charge['OrderID'].astype(str)
        df_charge['amount'] = pd.to_numeric(df_charge['amount'], errors='coerce').fillna(0)
        
        df_fin = df_charge.groupby('OrderID')['amount'].sum().reset_index(name='Net_Payment_Fetched')
        
        # Fees
        df_comm = calculate_fee_total(df_charge, 'Commission', 'Total_Commission_Fee')
        df_fixed = calculate_fee_total(df_charge, 'Fixed closing fee', 'Total_Fixed_Closing_Fee')
        df_pick = calculate_fee_total(df_charge, 'Pick & Pack Fee', 'Total_FBA_Pick_Pack_Fee')
        df_weight = calculate_fee_total(df_charge, 'Weight Handling Fee', 'Total_FBA_Weight_Handling_Fee')
        df_tech = calculate_fee_total(df_charge, 'Technology Fee', 'Total_Technology_Fee')
        df_tax = calculate_fee_total(df_charge, 'TCS|TDS|Tax', 'Total_Tax_TCS_TDS')

        for df_f in [df_comm, df_fixed, df_pick, df_weight, df_tech, df_tax]:
            if not df_f.empty: df_fin = pd.merge(df_fin, df_f, on='OrderID', how='left')
        
        df_fin.fillna(0, inplace=True)
        fee_cols = ['Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee']
        df_fin['Total_Fees_KPI'] = df_fin[[c for c in fee_cols if c in df_fin.columns]].sum(axis=1)
        
        return df_fin, df_charge

    def process_mtr_files(uploaded_mtr_files):
        all_mtr = []
        req_cols = ['Invoice Number', 'Invoice Date', 'Transaction Type', 'Order Id', 'Quantity', 'Sku', 'Invoice Amount']
        for file in uploaded_mtr_files:
            try:
                chunk_iter = pd.read_csv(file, chunksize=100000)
                for chunk in chunk_iter:
                    chunk.columns = [str(col).strip() for col in chunk.columns]
                    cols = [c for c in req_cols if c in chunk.columns]
                    if cols: all_mtr.append(chunk[cols].copy())
            except: return pd.DataFrame()
        
        if not all_mtr: return pd.DataFrame()
        df_mtr = pd.concat(all_mtr, ignore_index=True)
        df_mtr.rename(columns={'Order Id': 'OrderID', 'Invoice Amount': 'MTR Invoice Amount'}, inplace=True)
        df_mtr['OrderID'] = df_mtr['OrderID'].astype(str)
        df_mtr['MTR Invoice Amount'] = pd.to_numeric(df_mtr['MTR Invoice Amount'], errors='coerce').fillna(0)
        df_mtr['Sku'] = df_mtr['Sku'].astype(str)
        df_mtr['Quantity'] = pd.to_numeric(df_mtr['Quantity'], errors='coerce').fillna(1).astype(int)
        return df_mtr

    @st.cache_data(show_spinner="Merging...")
    def create_final_reconciliation_df(df_fin, df_log, df_cost):
        if df_log.empty or df_fin.empty: return pd.DataFrame()
        df_final = pd.merge(df_log, df_fin, on='OrderID', how='left')
        
        # Calculations
        df_final['Total_MTR_per_Order'] = df_final.groupby('OrderID')['MTR Invoice Amount'].transform('sum')
        df_final['Item_Count'] = df_final.groupby('OrderID')['OrderID'].transform('count')
        df_final['Proportion'] = np.where(df_final['Total_MTR_per_Order']!=0, df_final['MTR Invoice Amount']/df_final['Total_MTR_per_Order'], 1/df_final['Item_Count'])
        
        fin_cols = [c for c in df_fin.columns if c != 'OrderID' and c in df_final.columns]
        for c in fin_cols: df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0) * df_final['Proportion']

        if 'Net_Payment_Fetched' in df_final.columns: df_final.rename(columns={'Net_Payment_Fetched': 'Net Payment'}, inplace=True)
        
        if not df_cost.empty:
            df_final = pd.merge(df_final, df_cost, on='Sku', how='left')
        if 'Product Cost' not in df_final.columns: df_final['Product Cost'] = 0.0
        
        # Refund/Cancel Logic
        trans_type = df_final['Transaction Type'].astype(str).str.strip().str.lower() if 'Transaction Type' in df_final.columns else pd.Series()
        conditions = [trans_type.isin(['refund', 'freereplacement']), trans_type.str.contains('cancel', na=False)]
        choices = [-0.5 * df_final['Product Cost'], -0.8 * df_final['Product Cost']]
        df_final['Product Cost'] = np.select(conditions, choices, default=df_final['Product Cost'])
        
        df_final['Product Profit/Loss'] = df_final['Net Payment'] - (df_final['Product Cost'] * df_final['Quantity'])
        return df_final

    # --- AMAZON UI ---
    st.title("💰 Amazon Seller Central Reconciliation Dashboard")
    st.markdown("---")

    # Sidebar
    st.sidebar.subheader("Amazon Uploads")
    excel_template = create_cost_sheet_template()
    st.sidebar.download_button("Download Cost Template 📥", data=excel_template, file_name='cost_sheet_template.xlsx')
    
    cost_file = st.sidebar.file_uploader("1. Product Cost Sheet (.xlsx/.csv)", type=['xlsx', 'csv'], key="amz_cost")
    st.sidebar.markdown("---")
    payment_zip_files = st.sidebar.file_uploader("2. Payment Reports (.zip)", type=['zip'], accept_multiple_files=True, key="amz_zip")
    mtr_files = st.sidebar.file_uploader("3. MTR Reports (.csv)", type=['csv'], accept_multiple_files=True, key="amz_mtr")

    # Main Page Inputs
    st.subheader("Monthly Expenses (Manual Input)")
    c1, c2, c3, c4 = st.columns(4)
    storage = c1.number_input("Monthly Storage Fee (INR)", value=0.0, step=100.0, key="amz_store")
    ads = c2.number_input("Monthly Advertising Spends (INR)", value=0.0, step=100.0, key="amz_ads")
    salary = c3.number_input("Total Salary (INR)", value=0.0, step=1000.0, key="amz_sal")
    misc = c4.number_input("Miscellaneous Expenses (INR)", value=0.0, step=100.0, key="amz_misc")
    st.markdown("---")

    # Logic
    if payment_zip_files and mtr_files:
        df_cost = process_cost_sheet(cost_file) if cost_file else pd.DataFrame()
        
        all_zips = []
        for z in payment_zip_files: all_zips.extend(process_payment_zip_file(z))
        
        if not all_zips: st.stop()
        
        with st.spinner("Processing Amazon Data..."):
            df_fin, _ = process_payment_files(all_zips)
            df_log = process_mtr_files(mtr_files)
            
            if df_fin.empty or df_log.empty:
                st.error("Data processing failed.")
                st.stop()
            
            df_final = create_final_reconciliation_df(df_fin, df_log, df_cost)
            
            # KPIs
            total_items = df_final.shape[0]
            total_mtr = df_final['MTR Invoice Amount'].sum()
            total_pay = df_final['Net Payment'].sum()
            total_fees = df_final['Total_Fees_KPI'].sum()
            total_cost = (df_final['Product Cost'] * df_final['Quantity']).sum()
            
            profit_before_exp = df_final['Product Profit/Loss'].sum()
            total_exp = storage + ads + salary + misc
            final_profit = profit_before_exp - total_exp
            
            st.subheader("Key Business Metrics (Based on Item Reconciliation)")
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            k1.metric("Total Items", f"{total_items:,}")
            k2.metric("Total Net Payment", f"INR {total_pay:,.2f}")
            k3.metric("Total MTR Invoiced", f"INR {total_mtr:,.2f}")
            k4.metric("Total Amazon Fees", f"INR {total_fees:,.2f}")
            k5.metric("Total Product Cost", f"INR {total_cost:,.2f}")
            
            # Green Pill for Expenses
            k6.metric("TOTAL PROFIT/LOSS (Final)", f"INR {final_profit:,.2f}", delta=f"Other Expenses: INR {total_exp:,.2f}")
            
            st.markdown("**Monthly Expenses Breakdown:**")
            e1, e2, e3, e4 = st.columns(4)
            e1.metric("Storage Fee", f"INR {storage:,.2f}")
            e2.metric("Ads Spends", f"INR {ads:,.2f}")
            e3.metric("Total Salary", f"INR {salary:,.2f}")
            e4.metric("Miscellaneous Expenses", f"INR {misc:,.2f}")
            st.markdown("---")

            # Table
            st.header("1. Item-Level Reconciliation Summary (MTR Details + Classified Charges)")
            if 'OrderID' in df_final.columns:
                oids = ['All Orders'] + sorted(df_final['OrderID'].unique().tolist())
                sel_oid = st.selectbox("👉 Select Order ID to Filter Summary:", oids)
                if sel_oid != 'All Orders':
                    df_disp = df_final[df_final['OrderID'] == sel_oid].copy()
                else:
                    df_disp = df_final.sort_values('OrderID').copy()
            else: df_disp = df_final.copy()

            # Format
            num_cols = ['MTR Invoice Amount', 'Net Payment', 'Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee', 'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 'Product Cost', 'Product Profit/Loss']
            col_conf = {c: st.column_config.NumberColumn(format="INR %.2f") for c in num_cols}
            
            st.dataframe(df_disp, column_config=col_conf, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            excel_data = convert_to_excel(df_final)
            st.download_button("Download Full Excel Report", data=excel_data, file_name='amazon_reconciliation.xlsx')

    else:
        # No data state
        total_exp = storage + ads + salary + misc
        st.subheader("Current Other Expenses Input (No Sales Data)")
        k1, k2 = st.columns(2)
        k1.metric("TOTAL PROFIT/LOSS (Expected)", f"INR {-total_exp:,.2f}")
        k2.metric("Total Expenses Input", f"INR {total_exp:,.2f}")
        st.info("Upload files to see data.")

# ==========================================
# MODULE 2: AJIO RECONCILIATION (DETAILED & FIXED)
# ==========================================
def run_ajio_tool():
    st.markdown("""
        <style>
        div[data-testid="stMetric"] {
            background-color: var(--secondary-background-color) !important;
            border-left: 5px solid #FF4B4B;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0px 2px 4px rgba(0,0,0,0.1);
        }
        div[data-testid="stMetricLabel"] > div {
            opacity: 0.8;
            font-size: 0.9rem;
        }
        div[data-testid="stMetricValue"] > div {
            font-size: 1.5rem;
            font-weight: 700;
        }
        </style>
        """, unsafe_allow_html=True)

    st.title("📊 Ajio Seller Reconciliation")
    st.caption("Automated System | Consolidated View")
    
    # --- AJIO HELPERS ---
    def get_csv_download_link(df, filename="reconciliation_report.csv"):
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        return f'<a href="data:file/csv;base64,{b64}" download="{filename}" style="text-decoration:none; background-color:#FF4B4B; color:white; padding:10px 20px; border-radius:5px; font-weight:bold;">📥 Download Reconciliation Report</a>'

    def clean_currency(x):
        if isinstance(x, str):
            val = x.replace('₹', '').replace(',', '').strip()
            return float(val) if val else 0.0
        try: return float(x)
        except: return 0.0

    def parse_ajio_date(date_str):
        if not isinstance(date_str, str) or not date_str.strip(): return None 
        clean_str = date_str.replace(" IST", "").strip()
        try: return datetime.strptime(clean_str, "%a %b %d %H:%M:%S %Y")
        except ValueError:
            try: return pd.to_datetime(date_str)
            except: return None

    def get_first_val(series):
        valid = series.dropna()
        valid = valid[valid != '']
        return valid.iloc[0] if not valid.empty else None

    def find_col(df, candidates):
        col_map = {c.lower().strip(): c for c in df.columns}
        for cand in candidates:
            if cand.lower().strip() in col_map: return col_map[cand.lower().strip()]
        return None

    def clean_order_id(series):
        s = series.astype(str).str.replace(r'\.0$', '', regex=True)
        return s.str.strip().str.upper().replace(['NAN', 'NULL', 'NONE', '0', '', 'NAT'], np.nan)

    # --- AJIO INPUTS ---
    st.sidebar.subheader("Ajio Uploads")
    gst_file = st.sidebar.file_uploader("1. GST Report (Sales)", type=["csv", "xlsx"], key="ajio_gst")
    rtv_file = st.sidebar.file_uploader("2. RTV Report (Returns)", type=["csv", "xlsx"], key="ajio_rtv")
    payment_file = st.sidebar.file_uploader("3. Payment Report", type=["csv", "xlsx"], key="ajio_pay")
    st.sidebar.divider()
    run_btn = st.sidebar.button("🚀 Run Ajio Reconciliation", type="primary")

    if run_btn and gst_file and rtv_file and payment_file:
        with st.spinner("Processing Data..."):
            try:
                def load_file(f):
                    try: return pd.read_csv(f, encoding='utf-8-sig') if f.name.endswith('.csv') else pd.read_excel(f)
                    except: return pd.read_csv(f, encoding='latin1') if f.name.endswith('.csv') else pd.read_excel(f)

                df_gst = load_file(gst_file)
                df_rtv = load_file(rtv_file)
                df_pay = load_file(payment_file)

                # Cleanup
                df_gst.columns = df_gst.columns.str.strip()
                df_rtv.columns = df_rtv.columns.str.strip()
                df_pay.columns = df_pay.columns.str.strip()

                # --- 1. GST PROCESSING (FIXED: Added Date Aggregation) ---
                gst_order_col = find_col(df_gst, ['Cust Order No', 'Order No', 'Order ID'])
                gst_val_col = find_col(df_gst, ['Invoice Value', 'Total Value', 'Taxable Value'])
                gst_date_col = find_col(df_gst, ['Seller Invoice Date', 'Invoice Date']) # Fixed: Added Date Finding
                
                if not gst_order_col: st.error("GST Missing Order ID"); st.stop()
                
                df_gst_clean = pd.DataFrame()
                df_gst_clean['Cust Order No'] = clean_order_id(df_gst[gst_order_col])
                df_gst_clean['Invoice Value'] = df_gst[gst_val_col].apply(clean_currency)
                
                if gst_date_col:
                    df_gst_clean['Invoice Date'] = df_gst[gst_date_col].astype(str).apply(parse_ajio_date)
                else:
                    df_gst_clean['Invoice Date'] = None
                
                # Fixed: Aggregation dictionary to preserve Date
                gst_agg = {'Invoice Value': 'sum', 'Invoice Date': 'first'}
                df_gst_clean = df_gst_clean.groupby('Cust Order No', as_index=False).agg(gst_agg)

                # --- 2. RTV PROCESSING (FIXED: Added Type & Date Aggregation) ---
                rtv_order_col = find_col(df_rtv, ['Cust Order No', 'Order No'])
                rtv_val_col = find_col(df_rtv, ['Return Value', 'Refund Amount'])
                rtv_type_col = find_col(df_rtv, ['Return Type', 'Disposition', 'Reason']) # Fixed: Added Type Finding
                
                df_rtv_clean = pd.DataFrame()
                df_rtv_clean['Cust Order No'] = clean_order_id(df_rtv[rtv_order_col])
                df_rtv_clean['Return Value'] = df_rtv[rtv_val_col].apply(clean_currency)
                df_rtv_clean['Return Type'] = df_rtv[rtv_type_col] if rtv_type_col else ''

                # Fixed: Aggregation to preserve Type
                rtv_agg = {'Return Value': 'sum', 'Return Type': get_first_val}
                df_rtv_clean = df_rtv_clean.groupby('Cust Order No', as_index=False).agg(rtv_agg)

                # --- 3. PAYMENT PROCESSING (FIXED: Added Date Aggregation) ---
                pay_order_col = find_col(df_pay, ['Order No', 'Cust Order No'])
                pay_val_col = find_col(df_pay, ['Value', 'Payment Amount'])
                pay_date_col = find_col(df_pay, ['Expected settlement date', 'Settlement Date'])
                
                df_pay_clean = pd.DataFrame()
                df_pay_clean['Cust Order No'] = clean_order_id(df_pay[pay_order_col])
                df_pay_clean['Payment Received'] = df_pay[pay_val_col].apply(clean_currency)
                if pay_date_col:
                    df_pay_clean['Settlement Date'] = df_pay[pay_date_col].astype(str).apply(parse_ajio_date)
                else: df_pay_clean['Settlement Date'] = None
                
                # Fixed: Aggregation to preserve Date
                pay_agg = {'Payment Received': 'sum', 'Settlement Date': 'first'}
                df_pay_clean = df_pay_clean.groupby('Cust Order No', as_index=False).agg(pay_agg)

                # --- MERGE & CALCULATE ---
                df_recon = pd.merge(df_gst_clean, df_rtv_clean, on='Cust Order No', how='outer')
                df_recon = pd.merge(df_recon, df_pay_clean, on='Cust Order No', how='left')
                df_recon.fillna(0, inplace=True)

                # Calculations
                df_recon['Expected Payment'] = df_recon['Invoice Value'] - df_recon['Return Value']
                df_recon['Final Difference'] = np.where(
                    (df_recon['Invoice Value']>0) & (df_recon['Return Value']>0),
                    df_recon['Expected Payment'],
                    df_recon['Expected Payment'] - df_recon['Payment Received']
                )
                df_recon['Final Difference'] = df_recon['Final Difference'].round(2)

                # Status
                def get_status(row):
                    d = row['Final Difference']
                    if row['Payment Received']==0 and row['Expected Payment']>0 and row['Return Value']==0: return "🔴 Not Received"
                    if abs(d)<=10: return "🟢 Settled"
                    return "⚠️ Less Payment" if d>10 else "🔵 Over Payment"
                
                df_recon['Status'] = df_recon.apply(get_status, axis=1)
                df_recon['Remarks'] = df_recon.apply(lambda x: f"Type: {x['Return Type']}" if x['Return Type'] else "Standard", axis=1)

                # --- DISPLAY METRICS ---
                total_sales = df_recon['Invoice Value'].sum()
                total_ret = df_recon['Return Value'].sum()
                total_exp = total_sales - total_ret
                total_rec = df_recon['Payment Received'].sum()
                net_pend = df_recon['Final Difference'].sum()

                m1, m2, m3 = st.columns(3)
                m1.metric("Total Sales", f"₹{total_sales:,.0f}")
                m2.metric("Returns", f"₹{total_ret:,.0f}", delta_color="inverse")
                m3.metric("Expected", f"₹{total_exp:,.0f}")
                st.divider()
                m4, m5, m6 = st.columns(3)
                m4.metric("Received", f"₹{total_rec:,.2f}")
                m5.metric("Net Pending", f"₹{net_pend:,.2f}", delta="Receivable" if net_pend>0 else "Payable", delta_color="inverse" if net_pend>0 else "normal")

                # --- SETTLEMENT TABLE ---
                st.markdown("### 📅 Settlement Date-wise Analysis")
                if 'Settlement Date' in df_recon.columns and df_recon['Settlement Date'].notna().any():
                    # Filter rows with valid settlement date and ensure it's datetime
                    df_settle = df_recon.dropna(subset=['Settlement Date']).copy()
                    
                    # Safe conversion to datetime just in case
                    df_settle['Settlement Date'] = pd.to_datetime(df_settle['Settlement Date'], errors='coerce')
                    df_settle = df_settle.dropna(subset=['Settlement Date'])
                    
                    if not df_settle.empty:
                        df_settle['Settlement Date'] = df_settle['Settlement Date'].dt.date
                        grp = df_settle.groupby('Settlement Date').agg({'Cust Order No':'count', 'Expected Payment':'sum', 'Payment Received':'sum', 'Final Difference':'sum'}).reset_index()
                        grp = grp.sort_values('Settlement Date')
                        grp.columns = ['Date', 'Orders', 'Expected', 'Received', 'Diff']
                        
                        # Column Config for Settlement Table
                        st_conf = {
                            "Date": st.column_config.DateColumn("Settlement Date", format="DD-MMM-YYYY"),
                            "Expected": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                            "Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                            "Diff": st.column_config.NumberColumn("Diff", format="₹%.2f"),
                        }
                        st.dataframe(grp, column_config=st_conf, use_container_width=True, hide_index=True)
                    else:
                        st.info("No valid settlement dates found after processing.")
                else:
                    st.info("No Settlement Dates found to generate timeline.")

                # --- TABS & DATAFRAME DISPLAY ---
                t1, t2, t3 = st.tabs(["🚨 Action", "✅ Settled", "📄 All Data"])
                
                # Ensure these columns exist before selecting
                available_cols = ['Cust Order No', 'Invoice Date', 'Settlement Date', 'Invoice Value', 'Return Value', 'Expected Payment', 'Payment Received', 'Final Difference', 'Status', 'Remarks']
                final_cols = [c for c in available_cols if c in df_recon.columns]
                
                # Column Configuration
                col_config = {
                    "Cust Order No": st.column_config.TextColumn("Order ID"),
                    "Invoice Date": st.column_config.DateColumn("Inv Date", format="DD-MMM-YYYY"),
                    "Settlement Date": st.column_config.DateColumn("Settle Date", format="DD-MMM-YYYY"),
                    "Invoice Value": st.column_config.NumberColumn("Sales", format="₹%.2f"),
                    "Return Value": st.column_config.NumberColumn("Returns", format="₹%.2f"),
                    "Payment Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                    "Expected Payment": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                    "Final Difference": st.column_config.NumberColumn("Diff", format="₹%.2f"),
                }

                with t1: st.dataframe(df_recon[df_recon['Status'].isin(["🔴 Not Received", "⚠️ Less Payment", "🔵 Over Payment"])][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t2: st.dataframe(df_recon[df_recon['Status']=="🟢 Settled"][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t3: st.dataframe(df_recon[final_cols], column_config=col_config, use_container_width=True, hide_index=True)

                st.markdown(get_csv_download_link(df_recon), unsafe_allow_html=True)

            except Exception as e: st.error(f"Error: {e}")
    else: st.info("Upload files.")

# ==========================================
# MASTER EXECUTION
# ==========================================
if tool_selection == "Amazon Reconciliation":
    run_amazon_tool()
elif tool_selection == "Ajio Reconciliation":
    run_ajio_tool()
