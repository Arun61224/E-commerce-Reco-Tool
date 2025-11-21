import streamlit as st
import pandas as pd
import numpy as np
import io
import zipfile
import base64
import xlsxwriter
from datetime import datetime

# --- 1. GLOBAL CONFIGURATION (TOP LEVEL) ---
st.set_page_config(layout="wide", page_title="E-commerce Reconciliation Master Tool")

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("🔧 Navigation")
tool_selection = st.sidebar.selectbox("Select Platform:", ["Amazon Reconciliation", "Ajio Reconciliation"])
st.sidebar.markdown("---")

# ==========================================
# MODULE 1: AMAZON RECONCILIATION (ORIGINAL LOGIC RESTORED)
# ==========================================
def run_amazon_tool():
    # --- HELPER FUNCTIONS (EXACT COPY FROM ORIGINAL) ---
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

    # Uncached function
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
                st.error(f"Error reading Cost Sheet ({uploaded_file.name}): Unsupported file type. Please upload .xlsx or .csv.")
                return pd.DataFrame()
            
            df_cost.columns = [str(col).strip() for col in df_cost.columns]
            missing_cols = [col for col in required_cols if col not in df_cost.columns]
            if missing_cols:
                st.error(f"Cost Sheet Error: Missing required columns: {', '.join(missing_cols)}. Please check the file header.")
                return pd.DataFrame()
            
            df_cost.rename(columns={'SKU': 'Sku'}, inplace=True)
            df_cost['Sku'] = df_cost['Sku'].astype(str)
            df_cost['Product Cost'] = pd.to_numeric(df_cost['Product Cost'], errors='coerce').fillna(0)

            df_cost_master = df_cost.groupby('Sku')['Product Cost'].mean().reset_index(name='Product Cost')

            return df_cost_master
        except Exception as e:
            st.error(f"Error reading Cost Sheet ({uploaded_file.name}): Please ensure the file is correctly formatted with 'SKU' and 'Product Cost' columns. Details: {e}")
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

    # Uncached helper for ZIP processing
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
        except zipfile.BadZipFile:
            st.error(f"Error: The uploaded file {uploaded_zip_file.name} is not a valid ZIP file.")
            return []
        except Exception as e:
            st.error(f"An unexpected error occurred during unzipping {uploaded_zip_file.name}: {e}")
            return []
        return payment_files

    # Uncached function
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
                    except Exception as decode_err:
                        st.error(f"Payment TXT Decode Error in {file.name}: Could not decode file content. Details: {decode_err}")
                        continue 
                
                if file_content is None:
                    continue
                
                chunk_iter = pd.read_csv(
                    io.StringIO(file_content),
                    sep='\t',
                    skipinitialspace=True,
                    header=0,
                    chunksize=100000
                )

                first_chunk = True
                for chunk in chunk_iter:
                    chunk.columns = [str(col).strip().lower() for col in chunk.columns]

                    if first_chunk:
                        missing_cols = [col for col in required_cols_lower if col not in chunk.columns]
                        if missing_cols:
                            st.error(f"Error in {file.name}: The file is missing essential columns: {', '.join(missing_cols)}.")
                            return pd.DataFrame(), pd.DataFrame()
                        first_chunk = False

                    if 'order-id' in chunk.columns:
                        chunk.dropna(subset=['order-id'], inplace=True)
                    else:
                        continue

                    if all(col in chunk.columns for col in required_cols_lower):
                        chunk_small = chunk[required_cols_lower].copy()
                        all_payment_data.append(chunk_small)

            except Exception as e:
                st.error(f"Error reading or processing chunks in {file.name} (Payment TXT): {e}")
                return pd.DataFrame(), pd.DataFrame()

        if not all_payment_data:
            st.error("No valid payment data was found or processed from the TXT files.")
            return pd.DataFrame(), pd.DataFrame()

        try:
            df_charge_breakdown = pd.concat(all_payment_data, ignore_index=True)
        except Exception as concat_err:
            st.error(f"Error combining payment data chunks: {concat_err}")
            return pd.DataFrame(), pd.DataFrame()

        if df_charge_breakdown.empty:
            st.error("Payment files were read, but no valid 'order-id' entries were found after processing.")
            return pd.DataFrame(), pd.DataFrame()

        df_charge_breakdown.rename(columns={'order-id': 'OrderID'}, inplace=True)
        df_charge_breakdown['OrderID'] = df_charge_breakdown['OrderID'].astype(str)
        df_charge_breakdown['amount'] = pd.to_numeric(df_charge_breakdown['amount'], errors='coerce').fillna(0)

        df_financial_master = df_charge_breakdown.groupby('OrderID')['amount'].sum().reset_index(name='Net_Payment_Fetched')

        # --- Fee Calculation ---
        df_comm = calculate_fee_total(df_charge_breakdown, 'Commission', 'Total_Commission_Fee')
        df_fixed = calculate_fee_total(df_charge_breakdown, 'Fixed closing fee', 'Total_Fixed_Closing_Fee')
        df_pick = calculate_fee_total(df_charge_breakdown, 'Pick & Pack Fee', 'Total_FBA_Pick_Pack_Fee')
        df_weight = calculate_fee_total(df_charge_breakdown, 'Weight Handling Fee', 'Total_FBA_Weight_Handling_Fee')
        df_tech = calculate_fee_total(df_charge_breakdown, 'Technology Fee', 'Total_Technology_Fee')
        tax_descriptions = ['TCS', 'TDS', 'Tax']
        df_tax_summary = calculate_fee_total(df_charge_breakdown, '|'.join(tax_descriptions), 'Total_Tax_TCS_TDS')

        for df_fee in [df_comm, df_fixed, df_pick, df_weight, df_tech, df_tax_summary]:
            if not df_fee.empty and 'OrderID' in df_fee.columns:
                df_financial_master = pd.merge(df_financial_master, df_fee, on='OrderID', how='left')
            else:
                # Add missing columns
                col_name = df_fee.columns[1] if len(df_fee.columns) > 1 else 'Unknown'
                if col_name != 'Unknown' and col_name not in df_financial_master.columns:
                    df_financial_master[col_name] = 0.0

        df_financial_master.fillna(0, inplace=True)

        fee_kpi_cols = ['Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee']
        df_financial_master['Total_Fees_KPI'] = df_financial_master[[col for col in fee_kpi_cols if col in df_financial_master.columns]].sum(axis=1)
        
        return df_financial_master, df_charge_breakdown

    # Uncached function
    def process_mtr_files(uploaded_mtr_files):
        all_mtr_data = []
        required_mtr_cols = [
            'Invoice Number', 'Invoice Date', 'Transaction Type', 'Order Id',
            'Quantity', 'Sku', 'Ship From City', 'Ship To City', 'Ship To State',
            'Invoice Amount'
        ]

        for file in uploaded_mtr_files:
            try:
                chunk_iter = pd.read_csv(file, chunksize=100000)
                for chunk in chunk_iter:
                    chunk = chunk.loc[:, ~chunk.columns.str.contains('^Unnamed')]
                    chunk.columns = [str(col).strip() for col in chunk.columns]

                    cols_to_keep = [col for col in required_mtr_cols if col in chunk.columns]
                    if cols_to_keep:
                        chunk_small = chunk[cols_to_keep].copy()
                        all_mtr_data.append(chunk_small)
                    else:
                        st.error(f"MTR Error in {file.name}: Could not find essential columns.")
                        return pd.DataFrame()
            except Exception as e:
                st.error(f"Error reading {file.name} (MTR CSV): {e}")
                return pd.DataFrame()

        if not all_mtr_data:
            st.error("No valid MTR data could be processed from the CSV files.")
            return pd.DataFrame()

        try:
            df_mtr_raw = pd.concat(all_mtr_data, ignore_index=True)
        except Exception as concat_err:
            st.error(f"Error combining MTR data chunks: {concat_err}")
            return pd.DataFrame()

        df_mtr_raw.rename(columns={'Order Id': 'OrderID', 'Invoice Amount': 'MTR Invoice Amount'}, inplace=True)
        
        final_cols = ['Invoice Number', 'Invoice Date', 'Transaction Type', 'OrderID', 'Quantity', 'Sku', 'Ship From City', 'Ship To City', 'Ship To State', 'MTR Invoice Amount']
        for col in final_cols:
            if col not in df_mtr_raw.columns:
                df_mtr_raw[col] = ''

        df_logistics_master = df_mtr_raw[final_cols].copy()
        df_logistics_master['OrderID'] = df_logistics_master['OrderID'].astype(str)
        df_logistics_master['MTR Invoice Amount'] = pd.to_numeric(df_logistics_master['MTR Invoice Amount'], errors='coerce').fillna(0)
        df_logistics_master['Sku'] = df_logistics_master['Sku'].astype(str)
        df_logistics_master['Quantity'] = pd.to_numeric(df_logistics_master['Quantity'], errors='coerce').fillna(1).astype(int)

        return df_logistics_master

    @st.cache_data(show_spinner="Merging data and finalizing calculations...")
    def create_final_reconciliation_df(df_financial_master, df_logistics_master, df_cost_master):
        if df_logistics_master.empty or df_financial_master.empty:
            return pd.DataFrame()

        try:
            df_final = pd.merge(df_logistics_master, df_financial_master, on='OrderID', how='left')
        except Exception as merge_err:
            st.error(f"Error during main merge: {merge_err}")
            return pd.DataFrame()

        numeric_cols_needed = ['MTR Invoice Amount', 'Net_Payment_Fetched', 'Quantity']
        for col in numeric_cols_needed:
            if col not in df_final.columns: df_final[col] = 0
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)

        df_final['Total_MTR_per_Order'] = df_final.groupby('OrderID')['MTR Invoice Amount'].transform('sum')
        df_final['Item_Count_per_Order'] = df_final.groupby('OrderID')['OrderID'].transform('count')
        df_final['Proportion'] = np.where(
            (df_final['Total_MTR_per_Order'] != 0),
            df_final['MTR Invoice Amount'] / df_final['Total_MTR_per_Order'],
            np.where(df_final['Item_Count_per_Order'] > 0, 1 / df_final['Item_Count_per_Order'], 0)
        )

        financial_cols_present = [col for col in df_financial_master.columns if col != 'OrderID' and col in df_final.columns]
        for col in financial_cols_present:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0) * df_final['Proportion']

        if 'Net_Payment_Fetched' in df_final.columns:
            df_final.rename(columns={'Net_Payment_Fetched': 'Net Payment'}, inplace=True)
        elif 'Net Payment' not in df_final.columns:
            df_final['Net Payment'] = 0.0

        if not df_cost_master.empty and 'Sku' in df_final.columns and 'Sku' in df_cost_master.columns:
            try:
                df_final = pd.merge(df_final, df_cost_master, on='Sku', how='left')
            except Exception:
                pass
        
        if 'Product Cost' not in df_final.columns:
            df_final['Product Cost'] = 0.0

        df_final.fillna(0, inplace=True)
        df_final['Product Cost'] = pd.to_numeric(df_final['Product Cost'], errors='coerce').fillna(0)

        refund_keywords = ['refund', 'freereplacement']
        cancel_keywords = ['cancel']

        if 'Transaction Type' in df_final.columns:
            standardized_transaction_type = df_final['Transaction Type'].astype(str).str.strip().str.lower()
            conditions = [
                standardized_transaction_type.isin(refund_keywords),
                standardized_transaction_type.str.contains('|'.join(cancel_keywords), na=False) 
            ]
            choices = [-0.5 * df_final['Product Cost'], -0.8 * df_final['Product Cost']]
            df_final['Product Cost'] = np.select(conditions, choices, default=df_final['Product Cost'])

        df_final['Net Payment'] = pd.to_numeric(df_final['Net Payment'], errors='coerce').fillna(0)
        df_final['Quantity'] = pd.to_numeric(df_final['Quantity'], errors='coerce').fillna(1).astype(int)
        df_final['Product Profit/Loss'] = (df_final['Net Payment'] - (df_final['Product Cost'] * df_final['Quantity']))

        return df_final

    # --- AMAZON UI & LOGIC START ---
    st.title("💰 Amazon Seller Central Reconciliation Dashboard (Detailed)")
    st.markdown("---")

    # Sidebar Inputs (Restored with unique keys)
    st.sidebar.subheader("Amazon Uploads")
    excel_template = create_cost_sheet_template()
    st.sidebar.download_button("Download Cost Template 📥", data=excel_template, file_name='cost_sheet_template.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    cost_file = st.sidebar.file_uploader("1. Upload Product Cost Sheet (.xlsx or .csv)", type=['xlsx', 'csv'], key='amz_cost')
    st.sidebar.markdown("---")
    payment_zip_files = st.sidebar.file_uploader("2. Upload ALL Payment Reports (.zip)", type=['zip'], accept_multiple_files=True, key='amz_zip')
    mtr_files = st.sidebar.file_uploader("3. Upload ALL MTR Reports (.csv)", type=['csv'], accept_multiple_files=True, key='amz_mtr')

    # Main Page Inputs
    st.subheader("Monthly Expenses (Mandatory Inputs)")
    col_exp_input1, col_exp_input2 = st.columns(2)
    col_exp_input3, col_exp_input4 = st.columns(2)

    with col_exp_input1:
        storage_fee = st.number_input("Monthly Storage Fee (INR)", min_value=0.0, value=0.0, step=100.0, key='amz_storage')
    with col_exp_input2:
        ads_spends = st.number_input("Monthly Advertising Spends (INR)", min_value=0.0, value=0.0, step=100.0, key='amz_ads')
    with col_exp_input3:
        total_salary = st.number_input("Total Salary (INR)", min_value=0.0, value=0.0, step=1000.0, key='amz_salary')
    with col_exp_input4:
        miscellaneous_expenses = st.number_input("Miscellaneous Expenses (INR)", min_value=0.0, value=0.0, step=100.0, key='amz_misc')
    st.markdown("---")

    if payment_zip_files and mtr_files:
        df_cost_master = pd.DataFrame()
        if cost_file:
            with st.spinner("Processing Cost Sheet..."):
                df_cost_master = process_cost_sheet(cost_file)
            if df_cost_master.empty and cost_file:
                st.stop()

        all_payment_file_objects = []
        with st.spinner("Unzipping Payment files..."):
            for zip_file in payment_zip_files:
                all_payment_file_objects.extend(process_payment_zip_file(zip_file))

        if not all_payment_file_objects:
            st.error("ZIP file(s) processed, but no Payment (.txt) files found.")
            st.stop()

        with st.spinner("Processing Payment files..."):
            df_financial_master, _ = process_payment_files(all_payment_file_objects)
            if df_financial_master.empty: st.stop()

        with st.spinner("Processing MTR files..."):
            df_logistics_master = process_mtr_files(mtr_files)
            if df_logistics_master.empty: st.stop()

        if not df_financial_master.empty and not df_logistics_master.empty:
            df_reconciliation = create_final_reconciliation_df(df_financial_master, df_logistics_master, df_cost_master)
        else:
            st.error("Data processing failed.")
            st.stop()

        if df_reconciliation.empty:
            st.error("Failed to create reconciliation report.")
            st.stop()

        excel_data = None
        try:
            excel_data = convert_to_excel(df_reconciliation)
        except Exception:
            excel_data = io.BytesIO(b"Error")

        # --- DASHBOARD DISPLAY ---
        try:
            total_items = df_reconciliation.shape[0]
            total_mtr_billed = df_reconciliation['MTR Invoice Amount'].sum() if 'MTR Invoice Amount' in df_reconciliation.columns else 0
            total_payment_fetched = df_reconciliation['Net Payment'].sum() if 'Net Payment' in df_reconciliation.columns else 0
            total_fees = df_reconciliation['Total_Fees_KPI'].sum() if 'Total_Fees_KPI' in df_reconciliation.columns else 0
            
            if 'Product Cost' in df_reconciliation.columns and 'Quantity' in df_reconciliation.columns:
                cost = pd.to_numeric(df_reconciliation['Product Cost'], errors='coerce').fillna(0)
                quantity = pd.to_numeric(df_reconciliation['Quantity'], errors='coerce').fillna(1)
                total_product_cost = (cost * quantity).sum()
            else: total_product_cost = 0

            total_profit_before_others = df_reconciliation['Product Profit/Loss'].sum() if 'Product Profit/Loss' in df_reconciliation.columns else 0
            total_other_expenses = storage_fee + ads_spends + total_salary + miscellaneous_expenses 
            total_profit_final = total_profit_before_others - total_other_expenses

            st.subheader("Key Business Metrics (Based on Item Reconciliation)")
            col_kpi1, col_kpi2, col_kpi3, col_kpi4, col_kpi5, col_kpi6 = st.columns(6)
            
            col_kpi1.metric("Total Items", f"{total_items:,}")
            col_kpi2.metric("Total Net Payment", f"₹{total_payment_fetched:,.2f}")
            col_kpi3.metric("Total MTR Invoiced", f"₹{total_mtr_billed:,.2f}")
            col_kpi4.metric("Total Amazon Fees", f"₹{total_fees:.2f}")
            col_kpi5.metric("Total Product Cost", f"₹{total_product_cost:,.2f}")
            col_kpi6.metric("TOTAL PROFIT/LOSS (Final)", f"₹{total_profit_final:,.2f}", delta=f"Exp: ₹{total_other_expenses:,.2f}", delta_color="inverse")
            
            st.markdown("**Monthly Expenses Breakdown:**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Storage", f"₹{storage_fee:,.2f}")
            c2.metric("Ads", f"₹{ads_spends:,.2f}")
            c3.metric("Salary", f"₹{total_salary:,.2f}")
            c4.metric("Misc", f"₹{miscellaneous_expenses:,.2f}")
            st.markdown("---")

            st.header("1. Item-Level Reconciliation Summary (MTR Details + Classified Charges)")
            
            if 'OrderID' in df_reconciliation.columns:
                order_id_list = ['All Orders'] + sorted(df_reconciliation['OrderID'].unique().tolist())
                selected_order_id = st.selectbox("👉 Select Order ID to Filter Summary:", order_id_list)
                if selected_order_id != 'All Orders':
                    df_display = df_reconciliation[df_reconciliation['OrderID'] == selected_order_id].copy()
                else:
                    df_display = df_reconciliation.sort_values(by='OrderID', ascending=True).copy()
            else:
                df_display = df_reconciliation.copy()

            column_config_dict = {}
            numeric_cols_to_format = ['MTR Invoice Amount', 'Net Payment', 'Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee', 'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 'Product Cost', 'Product Profit/Loss']
            
            cols_to_scale = ['Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee', 'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 'Net Payment']
            large_num_threshold = 1e12
            scaling_factor = 1e18

            for col in cols_to_scale:
                if col in df_display.columns:
                    df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0)
                    df_display[col] = np.where(np.abs(df_display[col]) > large_num_threshold, df_display[col] / scaling_factor, df_display[col])

            for col in numeric_cols_to_format:
                if col in df_display.columns:
                    column_config_dict[col] = st.column_config.NumberColumn(format="₹%.2f")

            st.dataframe(df_display, column_config=column_config_dict, use_container_width=True, hide_index=True)
            st.markdown("---")
            st.header("2. Download Full Reconciliation Report")
            if excel_data:
                st.download_button(label="Download Full Excel Report", data=excel_data, file_name='amazon_reconciliation_summary.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        except Exception as display_err:
            st.error(f"Dashboard Error: {display_err}")

    else:
        # State when no files are uploaded
        total_other = storage_fee + ads_spends + total_salary + miscellaneous_expenses
        st.subheader("Current Other Expenses Input (No Sales Data)")
        k_sum, k_exp = st.columns(2)
        k_sum.metric("TOTAL PROFIT/LOSS (Expected)", f"₹{-total_other:,.2f}")
        k_exp.metric("Total Expenses Input", f"₹{total_other:,.2f}")
        st.info("Please upload files in the sidebar.")

# ==========================================
# MODULE 2: AJIO RECONCILIATION (DETAILED VERSION)
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
