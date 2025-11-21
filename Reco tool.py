import streamlit as st
import pandas as pd
import numpy as np
import io
import zipfile
import base64
import plotly.express as px
from datetime import datetime

# --- 1. GLOBAL CONFIGURATION ---
st.set_page_config(layout="wide", page_title="E-commerce Reconciliation Master Tool")

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("🔧 Navigation")
tool_selection = st.sidebar.selectbox("Select Platform:", ["Amazon Reconciliation", "Ajio Reconciliation"])
st.sidebar.markdown("---")

# ==========================================
# MODULE 1: AMAZON RECONCILIATION LOGIC
# ==========================================
def run_amazon_tool():
    st.title("💰 Amazon Seller Central Reconciliation Dashboard")
    st.markdown("---")

    # --- HELPER FUNCTIONS FOR AMAZON ---
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

    # --- AMAZON SIDEBAR INPUTS ---
    st.sidebar.subheader("Amazon Uploads")
    
    # Template Download
    excel_template = create_cost_sheet_template()
    st.sidebar.download_button("Download Cost Template 📥", data=excel_template, file_name='cost_sheet_template.xlsx')
    
    cost_file = st.sidebar.file_uploader("1. Product Cost Sheet (.xlsx/.csv)", type=['xlsx', 'csv'], key="amz_cost")
    st.sidebar.markdown("---")
    payment_zip_files = st.sidebar.file_uploader("2. Payment Reports (.zip)", type=['zip'], accept_multiple_files=True, key="amz_zip")
    mtr_files = st.sidebar.file_uploader("3. MTR Reports (.csv)", type=['csv'], accept_multiple_files=True, key="amz_mtr")

    # --- MAIN PAGE INPUTS (Amazon) ---
    st.subheader("Monthly Expenses (Manual Input)")
    c1, c2, c3, c4 = st.columns(4)
    storage = c1.number_input("Storage Fee", value=0.0, step=100.0, key="amz_store")
    ads = c2.number_input("Ads Spends", value=0.0, step=100.0, key="amz_ads")
    salary = c3.number_input("Total Salary", value=0.0, step=1000.0, key="amz_sal")
    misc = c4.number_input("Misc Expenses", value=0.0, step=100.0, key="amz_misc")
    st.markdown("---")

    # --- EXECUTION LOGIC ---
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
            total_profit_before = df_final['Product Profit/Loss'].sum()
            total_exp = storage + ads + salary + misc
            final_profit = total_profit_before - total_exp
            
            st.metric("TOTAL PROFIT/LOSS (Final)", f"INR {final_profit:,.2f}", delta=f"- Expenses: {total_exp:,.2f}")
            
            st.subheader("Reconciliation Data")
            st.dataframe(df_final, use_container_width=True)
            
            excel_data = convert_to_excel(df_final)
            st.download_button("Download Excel Report", data=excel_data, file_name='amazon_reconciliation.xlsx')

    else:
        st.info("Please upload Amazon Payment (Zip) and MTR (CSV) files in the sidebar.")

# ==========================================
# MODULE 2: AJIO RECONCILIATION LOGIC (Full Version)
# ==========================================
def run_ajio_tool():
    # --- Custom CSS to Restore Look ---
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
    
    # --- HELPER FUNCTIONS FOR AJIO ---
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
        if not isinstance(date_str, str) or not date_str.strip():
            return None 
        clean_str = date_str.replace(" IST", "").strip()
        try:
            dt_obj = datetime.strptime(clean_str, "%a %b %d %H:%M:%S %Y")
            return dt_obj 
        except ValueError:
            try:
                return pd.to_datetime(date_str)
            except:
                return None

    def get_first_val(series):
        valid = series.dropna()
        valid = valid[valid != '']
        return valid.iloc[0] if not valid.empty else None

    def find_col(df, candidates):
        col_map = {c.lower().strip(): c for c in df.columns}
        for cand in candidates:
            if cand.lower().strip() in col_map:
                return col_map[cand.lower().strip()]
        return None

    def clean_order_id(series):
        s = series.astype(str)
        s = s.str.replace(r'\.0$', '', regex=True)
        s = s.str.strip().str.upper()
        s = s.replace(['NAN', 'NULL', 'NONE', '0', '', 'NAT'], np.nan)
        return s

    # --- AJIO SIDEBAR INPUTS ---
    st.sidebar.subheader("Ajio Uploads")
    gst_file = st.sidebar.file_uploader("1. GST Report (Sales)", type=["csv", "xlsx"], key="ajio_gst")
    rtv_file = st.sidebar.file_uploader("2. RTV Report (Returns)", type=["csv", "xlsx"], key="ajio_rtv")
    payment_file = st.sidebar.file_uploader("3. Payment Report", type=["csv", "xlsx"], key="ajio_pay")
    
    st.sidebar.divider()
    run_btn = st.sidebar.button("🚀 Run Ajio Reconciliation", type="primary")

    if run_btn and gst_file and rtv_file and payment_file:
        with st.spinner("Processing Data & Mapping Dates..."):
            try:
                # 1. Load Data
                def load_file(f):
                    try:
                        return pd.read_csv(f, encoding='utf-8-sig') if f.name.endswith('.csv') else pd.read_excel(f)
                    except:
                        return pd.read_csv(f, encoding='latin1') if f.name.endswith('.csv') else pd.read_excel(f)

                df_gst = load_file(gst_file)
                df_rtv = load_file(rtv_file)
                df_pay = load_file(payment_file)

                # Strip Headers
                df_gst.columns = df_gst.columns.str.strip()
                df_rtv.columns = df_rtv.columns.str.strip()
                df_pay.columns = df_pay.columns.str.strip()

                # --- GST PROCESSING ---
                gst_order_col = find_col(df_gst, ['Cust Order No', 'Order No', 'Order ID'])
                gst_val_col = find_col(df_gst, ['Invoice Value', 'Total Value', 'Taxable Value'])
                gst_qty_col = find_col(df_gst, ['Shipped QTY', 'Qty'])
                gst_date_col = find_col(df_gst, ['Seller Invoice Date', 'Invoice Date'])

                if not gst_order_col or not gst_val_col:
                    st.error(f"❌ GST File Error: Missing columns. Found: {list(df_gst.columns)}")
                    st.stop()

                df_gst_clean = pd.DataFrame()
                df_gst_clean['Cust Order No'] = clean_order_id(df_gst[gst_order_col])
                df_gst_clean['Invoice Value'] = df_gst[gst_val_col].apply(clean_currency)
                df_gst_clean = df_gst_clean.dropna(subset=['Cust Order No'])
                
                if gst_date_col:
                    df_gst_clean['Invoice Date'] = df_gst[gst_date_col].astype(str).apply(parse_ajio_date)
                else:
                    df_gst_clean['Invoice Date'] = None

                gst_agg = {'Invoice Value': 'sum', 'Invoice Date': 'first'}
                if gst_qty_col:
                    df_gst_clean['Shipped QTY'] = df_gst[gst_qty_col]
                    gst_agg['Shipped QTY'] = 'sum'
                
                df_gst_clean = df_gst_clean.groupby('Cust Order No', as_index=False).agg(gst_agg)

                # --- RTV PROCESSING ---
                rtv_order_col = find_col(df_rtv, ['Cust Order No', 'Order No'])
                rtv_val_col = find_col(df_rtv, ['Return Value', 'Refund Amount'])
                rtv_type_col = find_col(df_rtv, ['Return Type', 'Disposition', 'Reason'])
                rtv_date_col = find_col(df_rtv, ['Return Created Date', 'Return Date'])
                
                if not rtv_order_col:
                    st.error("❌ RTV File Error: Missing 'Cust Order No'")
                    st.stop()

                df_rtv_clean = pd.DataFrame()
                df_rtv_clean['Cust Order No'] = clean_order_id(df_rtv[rtv_order_col])
                df_rtv_clean = df_rtv_clean.dropna(subset=['Cust Order No'])

                df_rtv_clean['Return Value'] = df_rtv[rtv_val_col].apply(clean_currency) if rtv_val_col else 0.0
                df_rtv_clean['Return Type'] = df_rtv[rtv_type_col] if rtv_type_col else ''
                
                if rtv_date_col:
                    df_rtv_clean['Return Date'] = df_rtv[rtv_date_col].astype(str).apply(parse_ajio_date)
                else:
                    df_rtv_clean['Return Date'] = None

                rtv_agg = {'Return Value': 'sum', 'Return Type': get_first_val, 'Return Date': 'first'}
                df_rtv_clean = df_rtv_clean.groupby('Cust Order No', as_index=False).agg(rtv_agg)

                # --- PAYMENT PROCESSING ---
                pay_order_col = find_col(df_pay, ['Order No', 'Cust Order No', 'Po Number'])
                pay_val_col = find_col(df_pay, ['Value', 'Payment Amount', 'Net Amount', 'Amt'])
                pay_date_col = find_col(df_pay, ['Expected settlement date', 'Settlement Date', 'Clearing date'])

                if not pay_order_col:
                    st.error("❌ Payment File Error: Missing 'Order No'")
                    st.stop()

                df_pay_clean = pd.DataFrame()
                df_pay_clean['Cust Order No'] = clean_order_id(df_pay[pay_order_col])
                df_pay_clean = df_pay_clean.dropna(subset=['Cust Order No'])

                df_pay_clean['Payment Received'] = df_pay[pay_val_col].apply(clean_currency) if pay_val_col else 0.0
                
                # PARSE SETTLEMENT DATE
                if pay_date_col:
                    df_pay_clean['Settlement Date'] = df_pay[pay_date_col].astype(str).apply(parse_ajio_date)
                else:
                    df_pay_clean['Settlement Date'] = None

                pay_agg = {'Payment Received': 'sum', 'Settlement Date': 'first'}
                df_pay_clean = df_pay_clean.groupby('Cust Order No', as_index=False).agg(pay_agg)

                # --- MERGE LOGIC ---
                df_recon = pd.merge(df_gst_clean, df_rtv_clean, on='Cust Order No', how='outer', suffixes=('_GST', '_RTV'))
                df_recon = pd.merge(df_recon, df_pay_clean, on='Cust Order No', how='left')

                # Fill values
                for col in ['Invoice Value', 'Return Value', 'Payment Received']:
                    df_recon[col] = df_recon[col].fillna(0.0)
                
                df_recon['Return Type'] = df_recon['Return Type'].fillna('')

                # --- CALCULATIONS ---
                df_recon['Expected Payment'] = df_recon['Invoice Value'] - df_recon['Return Value']
                
                def calculate_final_difference(row):
                    if row['Invoice Value'] > 0 and row['Return Value'] > 0:
                        return row['Expected Payment']
                    else:
                        return row['Expected Payment'] - row['Payment Received']

                df_recon['Final Difference'] = df_recon.apply(calculate_final_difference, axis=1)
                
                # Rounding
                df_recon['Final Difference'] = df_recon['Final Difference'].round(2)
                df_recon['Expected Payment'] = df_recon['Expected Payment'].round(2)

                # --- STATUS ---
                def get_status(row):
                    diff = row['Final Difference']
                    if row['Payment Received'] == 0 and row['Expected Payment'] > 0 and row['Return Value'] == 0:
                        return "🔴 Not Received"
                    if abs(diff) <= 10:
                        return "🟢 Settled"
                    if diff > 10:
                        return "⚠️ Less Payment"
                    if diff < -10:
                        return "🔵 Over Payment"
                    return "⚪ Check"

                df_recon['Status'] = df_recon.apply(get_status, axis=1)
                df_recon['Remarks'] = df_recon.apply(lambda x: f"Type: {x['Return Type']}" if x['Return Type'] else "Standard", axis=1)

                # --- DASHBOARD ---
                
                # Overview Metrics
                st.markdown("### 📋 Financial Statement")
                total_sales = df_recon['Invoice Value'].sum()
                total_returns = df_recon['Return Value'].sum()
                total_expected = total_sales - total_returns
                total_rec = df_recon['Payment Received'].sum()
                final_diff_total = df_recon['Final Difference'].sum()
                rec_against_order = total_expected + final_diff_total

                m1, m2, m3 = st.columns(3)
                m1.metric("1. Total Sales", f"₹{total_sales:,.0f}")
                m2.metric("2. Less: Returns", f"₹{total_returns:,.0f}", delta_color="inverse")
                m3.metric("3. Expected", f"₹{total_expected:,.0f}")
                
                st.divider()
                
                m4, m5, m6 = st.columns(3)
                m4.metric("4. Received in Date Range", f"₹{total_rec:,.2f}")
                m5.metric("5. Received Against Order & Return", f"₹{rec_against_order:,.2f}", delta="Adjusted Value")
                m6.metric("6. Net Pending", f"₹{final_diff_total:,.2f}", 
                        delta="Receivable" if final_diff_total > 0 else "Payable",
                        delta_color="inverse" if final_diff_total > 0 else "normal")

                st.divider()

                # --- SETTLEMENT ANALYSIS (TABLE ONLY) ---
                st.markdown("### 📅 Settlement Date-wise Analysis")
                
                if 'Settlement Date' in df_recon.columns and df_recon['Settlement Date'].notna().any():
                    # Filter rows with valid settlement date
                    df_settle = df_recon.dropna(subset=['Settlement Date']).copy()
                    
                    # CRITICAL FIX: Normalize Date (Remove Time)
                    df_settle['Settlement Date'] = pd.to_datetime(df_settle['Settlement Date']).dt.date
                    
                    # Group by Date (Now consolidating properly)
                    settle_group = df_settle.groupby('Settlement Date').agg({
                        'Cust Order No': 'count',
                        'Expected Payment': 'sum',
                        'Payment Received': 'sum',
                        'Final Difference': 'sum'
                    }).reset_index()
                    
                    settle_group = settle_group.sort_values('Settlement Date')
                    
                    settle_group.columns = ['Settlement Date', 'Order Count', 'Total Expected (Value)', 'Total Received', 'Difference']
                    
                    # Display Table with formatted date (NO CHART)
                    st.dataframe(
                        settle_group,
                        column_config={
                            "Settlement Date": st.column_config.DateColumn("Settlement Date", format="DD-MMM-YYYY"),
                            "Total Expected (Value)": st.column_config.NumberColumn("Expected Amt", format="₹%.2f"),
                            "Total Received": st.column_config.NumberColumn("Received Amt", format="₹%.2f"),
                            "Difference": st.column_config.NumberColumn("Diff", format="₹%.2f")
                        },
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("No Settlement Dates found in the Payment Report to generate timeline.")
                
                st.divider()

                # --- TABS ---
                tab_action, tab_settled, tab_all = st.tabs(["🚨 Action Required", "✅ Settled", "📄 All Data"])

                column_config = {
                    "Cust Order No": st.column_config.TextColumn("Order ID", width="medium"),
                    "Invoice Date": st.column_config.DateColumn("Inv Date", format="DD-MMM-YYYY"),
                    "Settlement Date": st.column_config.DateColumn("Settle Date", format="DD-MMM-YYYY"),
                    "Invoice Value": st.column_config.NumberColumn("Sales", format="₹%.2f"),
                    "Return Value": st.column_config.NumberColumn("Returns", format="₹%.2f"),
                    "Payment Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                    "Expected Payment": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                    "Final Difference": st.column_config.NumberColumn("Diff", format="₹%.2f"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                }
                show_cols = ['Cust Order No', 'Invoice Date', 'Settlement Date', 'Invoice Value', 'Return Value', 'Expected Payment', 'Payment Received', 'Final Difference', 'Status', 'Remarks']

                with tab_action:
                    st.subheader("Disputed Orders")
                    df_action = df_recon[df_recon['Status'].isin(["🔴 Not Received", "⚠️ Less Payment", "🔵 Over Payment"])]
                    st.dataframe(df_action[show_cols], column_config=column_config, use_container_width=True, hide_index=True)

                with tab_settled:
                    st.subheader("Settled Orders")
                    df_settled = df_recon[df_recon['Status'] == "🟢 Settled"]
                    st.dataframe(df_settled[show_cols], column_config=column_config, use_container_width=True, hide_index=True)

                with tab_all:
                    st.subheader("All Data")
                    search = st.text_input("Search Order ID:", placeholder="Type Order ID...")
                    df_view = df_recon.copy()
                    if search:
                        df_view = df_view[df_view['Cust Order No'].str.contains(search, case=False)]
                    st.dataframe(df_view[show_cols], column_config=column_config, use_container_width=True, hide_index=True)

                st.markdown("---")
                st.markdown(get_csv_download_link(df_recon), unsafe_allow_html=True)

            except Exception as e:
                st.error(f"Processing Error: {e}")

    else:
        st.info("👈 Upload GST, RTV, and Payment files in the sidebar.")

# ==========================================
# MASTER EXECUTION
# ==========================================
if tool_selection == "Amazon Reconciliation":
    run_amazon_tool()
elif tool_selection == "Ajio Reconciliation":
    run_ajio_tool()
