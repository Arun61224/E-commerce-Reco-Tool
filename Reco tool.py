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

# --- 2. GLOBAL HELPER FUNCTIONS (AMAZON) ---
@st.cache_data
def amz_create_cost_sheet_template():
    template_data = {
        'SKU': ['ExampleSKU-001', 'ExampleSKU-002'],
        'Product Cost': [150.50, 220.00]
    }
    df = pd.DataFrame(template_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Cost_Sheet_Template', index=False)
    return output.getvalue()

def amz_process_cost_sheet(uploaded_file):
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
        
        df_cost.columns = df_cost.columns.str.strip()
        df_cost.rename(columns={'Product Cost': 'Product_Cost'}, inplace=True)
        if not all(col in df_cost.columns for col in required_cols):
            st.error(f"Cost Sheet missing required columns: {required_cols}")
            return None
        
        df_cost['SKU'] = df_cost['SKU'].astype(str).str.strip()
        df_cost['Product_Cost'] = pd.to_numeric(df_cost['Product_Cost'], errors='coerce').fillna(0)
        return df_cost[['SKU', 'Product_Cost']].drop_duplicates(subset='SKU', keep='first')
        
    except Exception as e:
        st.error(f"Error processing Cost Sheet: {e}")
        return None

def amz_get_csv_download_link(df):
    csv = df.to_csv(index=False).encode('utf-8')
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="Amazon_Reconciliation_Output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv">📥 Download Final Reconciliation CSV</a>'
    return href

def amz_calculate_fee_total(df):
    """Calculates total fees and tax from the payment report by filtering description keywords."""
    
    # Ensure amount-description exists and handle case sensitivity
    df.columns = df.columns.str.lower()
    if 'amount-description' not in df.columns:
        st.warning("Payment report is missing 'amount-description' column. Cannot calculate fees.")
        return df

    # 1. Total Fees (excluding taxes)
    fee_keywords = {
        'Total_Commission_Fee': ['Commission'],
        'Total_Fixed_Closing_Fee': ['Fixed closing fee'],
        'Total_FBA_Pick_Pack_Fee': ['Pick & Pack Fee'],
        'Total_FBA_Weight_Handling_Fee': ['Weight Handling Fee'],
        'Total_Technology_Fee': ['Technology Fee'],
    }
    
    df['Description_Lower'] = df['amount-description'].str.lower()
    
    for col, keywords in fee_keywords.items():
        # Sum the amount for transactions matching the fee keywords
        df[col] = df[df['Description_Lower'].str.contains('|'.join(keywords), na=False, regex=True)]['amount'].sum()

    # 2. Total Tax/TCS/TDS
    df['Total_Tax_TCS_TDS'] = df[df['Description_Lower'].str.contains('tcs|tds|tax', na=False, regex=True)]['amount'].sum()
    
    # Sum up all identified fees for a single KPI
    fee_cols = list(fee_keywords.keys())
    df['Total_Fees_KPI'] = df[fee_cols].sum(axis=1)

    return df.drop(columns=['Description_Lower'])

def amz_process_payment_files(uploaded_files):
    all_payment_data = []
    
    for file in uploaded_files:
        if file.name.endswith('.zip'):
            with zipfile.ZipFile(file, 'r') as z:
                for filename in z.namelist():
                    if filename.endswith('.txt'):
                        try:
                            with z.open(filename) as f:
                                # Amazon uses tab-separated values
                                df = pd.read_csv(f, sep='\t', decimal=',')
                                df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()
                                
                                # Convert the 'amount' column to numeric
                                if 'amount' in df.columns:
                                    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                                
                                all_payment_data.append(df)
                        except Exception as e:
                            st.warning(f"Skipping file {filename} due to error: {e}")
    
    if not all_payment_data:
        st.error("No valid .txt files found inside the zipped payment reports.")
        return None

    df_full = pd.concat(all_payment_data, ignore_index=True)
    df_full['order_id'] = df_full['order_id'].astype(str).str.strip()
    
    # 1. Calculate Net Payment per Order ID
    df_payment = df_full.groupby('order_id')['amount'].sum().reset_index()
    df_payment.rename(columns={'order_id': 'OrderID', 'amount': 'Net Payment'}, inplace=True)
    
    # 2. Calculate Fee and Tax KPIs (based on the full transactional data)
    df_fees = amz_calculate_fee_total(df_full.copy())
    
    # The fees/taxes are the same for all entries in the payment report since they are aggregated totals.
    # Just take the first row's calculations, as they are order-agnostic totals for the report period.
    df_fees_summary = df_fees[['Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee', 
                               'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee', 'Total_Fees_KPI', 'Total_Tax_TCS_TDS']].iloc[:1].copy()

    df_fees_summary['Key'] = 1
    
    return df_payment, df_fees_summary

def amz_process_mtr_files(uploaded_files):
    all_mtr_data = []
    required_mtr_cols = ['Order ID', 'Invoice Number', 'Invoice Date', 'Transaction Type', 'Sku', 'Quantity', 'Invoice Amount']
    
    for file in uploaded_files:
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()
            
            # Standardize column names
            col_map = {
                'order_id': 'OrderID',
                'invoice_number': 'Invoice Number',
                'invoice_date': 'Invoice Date',
                'transaction_type': 'Transaction Type',
                'sku': 'Sku',
                'quantity': 'Quantity',
                'invoice_amount': 'MTR Invoice Amount' # Renaming for clarity in final output
            }
            
            df.rename(columns=col_map, inplace=True)
            
            # Check for required columns
            missing_cols = [c for c in required_mtr_cols if c not in df.columns]
            if missing_cols:
                st.error(f"MTR file '{file.name}' is missing columns: {missing_cols}")
                continue

            # Data type cleaning
            df['OrderID'] = df['OrderID'].astype(str).str.strip()
            df['Sku'] = df['Sku'].astype(str).str.strip()
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
            df['MTR Invoice Amount'] = pd.to_numeric(df['MTR Invoice Amount'], errors='coerce').fillna(0)
            
            all_mtr_data.append(df[required_mtr_cols])
            
        except Exception as e:
            st.error(f"Error processing MTR file '{file.name}': {e}")
            
    if not all_mtr_data:
        return None
    
    df_mtr_full = pd.concat(all_mtr_data, ignore_index=True)
    
    # Filter Logic: Remove 'Cancel' transaction if 'Shipment' exists for the same Order ID
    df_mtr_full = amz_filter_mtr_cancellations(df_mtr_full)

    return df_mtr_full

def amz_filter_mtr_cancellations(df_mtr):
    """
    Removes 'Cancel' transactions if a 'Shipment' transaction with the same Order ID exists.
    This prevents double-counting or confusion in reconciliation.
    """
    shipment_orders = df_mtr[df_mtr['Transaction Type'] == 'Shipment']['OrderID'].unique()
    
    # Create a boolean mask: True if it's NOT a 'Cancel' transaction OR if the OrderID is NOT in shipment_orders
    mask = ~((df_mtr['Transaction Type'] == 'Cancel') & (df_mtr['OrderID'].isin(shipment_orders)))
    
    df_filtered = df_mtr[mask].copy()
    st.info(f"Filtered out {len(df_mtr) - len(df_filtered)} 'Cancel' entries where a 'Shipment' transaction was also present for the same Order ID.")
    return df_filtered

def amz_create_final_reconciliation_df(df_log, df_fin, df_cost, monthly_expense):
    # 1. Merge MTR (Item-level log) with Payment (Order-level financial summary)
    df_final = pd.merge(df_log, df_fin, on='OrderID', how='left')
    
    # 2. Add Cost Sheet Data
    df_final = pd.merge(df_final, df_cost, on='Sku', how='left')
    df_final['Product_Cost'] = df_final['Product_Cost'].fillna(0) # Default cost is 0 if SKU not found
    
    # 3. Identify Orders Missing from Payment Report
    df_final['Remarks'] = np.where(df_final['Net Payment'].isnull(), 'Order ID is not in this Payment report', 'Settled/In-Progress')
    
    # Fill NaN payment/fee columns with 0 for orders where payment data is missing (to allow calculations to proceed)
    payment_cols = ['Net Payment']
    df_final[payment_cols] = df_final[payment_cols].fillna(0)
    
    # Add Fees/Tax data (Fees Summary has only one row, so we cross-join using a 'Key' column)
    df_fees_summary = df_fin['fees_summary'].iloc[0]
    df_fees_summary['Key'] = 1
    df_final['Key'] = 1
    
    df_final = pd.merge(df_final, df_fees_summary, on='Key', how='left').drop(columns=['Key']).fillna(0)

    # 4. Proportionate Fee and Payment Allocation (Crucial Step)
    
    # Calculate Total Order Value from MTR for each OrderID
    df_final['Order_Total_MTR'] = df_final.groupby('OrderID')['MTR Invoice Amount'].transform('sum')
    
    # Calculate Proportion: Item Value / Order Total Value
    # Handle division by zero for orders with 0 MTR value (should not happen, but for safety)
    df_final['Proportion'] = np.where(df_final['Order_Total_MTR'] > 0, 
                                     df_final['MTR Invoice Amount'] / df_final['Order_Total_MTR'], 
                                     0)
    
    # Distribute the Order-level financial metrics (Payment and Fees) based on Proportion
    # Note: If an order is missing from the Payment Report, its Net Payment is 0, so the allocated values will be 0.
    for col in payment_cols + list(df_fees_summary.drop(columns=['Key']).columns):
        if col in df_final.columns:
            # We are distributing the full *report-period total fees* across ALL items found in the MTR.
            # This assumes the total fees apply proportionally to all recorded MTR sales.
            df_final[col] = df_final[col] * df_final['Proportion']
    
    # 5. Adjusted Product Cost based on Transaction Type
    # Adjust Cost for Returns, Refunds, and Cancellations as per typical accounting rules
    
    # Create a column for adjusted cost
    df_final['Adjusted_Product_Cost'] = df_final['Product_Cost'] * df_final['Quantity']
    
    # Adjustments:
    # Refund/Return: Often cost is partially recovered/adjusted. Using 50% here as an example.
    df_final.loc[df_final['Transaction Type'].str.contains('Refund|Return', case=False, na=False), 
                 'Adjusted_Product_Cost'] = df_final['Adjusted_Product_Cost'] * 0.5
                 
    # Cancel: Sometimes a small fee is incurred, or cost is reversed. Using -20% of cost as reversal example.
    df_final.loc[df_final['Transaction Type'].str.contains('Cancel', case=False, na=False), 
                 'Adjusted_Product_Cost'] = df_final['Adjusted_Product_Cost'] * -0.2
                 
    # Free Replacement: Often 0 cost impact on the reconciliation for the original item.
    df_final.loc[df_final['Transaction Type'].str.contains('FreeReplacement', case=False, na=False), 
                 'Adjusted_Product_Cost'] = 0

    # 6. Final Calculation: Profit/Loss per Item
    # Profit/Loss = Net Payment - Total Fees - Adjusted Product Cost
    df_final['Product Profit/Loss'] = (df_final['Net Payment'] + 
                                     df_final['Total_Tax_TCS_TDS'] - # Tax/TCS is usually credited back in final payment
                                     df_final['Total_Fees_KPI'] - 
                                     df_final['Adjusted_Product_Cost'])
    
    # 7. Final KPI Totals
    total_net_payment = df_final['Net Payment'].sum()
    total_fees = df_final['Total_Fees_KPI'].sum()
    total_tax_tds_tcs = df_final['Total_Tax_TCS_TDS'].sum()
    total_product_cost = df_final['Adjusted_Product_Cost'].sum()
    total_profit_before_expense = df_final['Product Profit/Loss'].sum()
    
    final_profit_loss = total_profit_before_expense - monthly_expense
    
    kpis = {
        'Total_Net_Payment': total_net_payment,
        'Total_Tax_TDS_TCS': total_tax_tds_tcs,
        'Total_Fees': total_fees,
        'Total_Product_Cost': total_product_cost,
        'Monthly_Expense': monthly_expense,
        'Total_Profit_Before_Expense': total_profit_before_expense,
        'Final_Profit_Loss': final_profit_loss
    }

    # Clean up and select final columns for display
    final_display_cols = ['OrderID', 'Invoice Number', 'Invoice Date', 'Transaction Type', 'Sku', 'Quantity', 
                          'MTR Invoice Amount', 'Net Payment', 'Total_Commission_Fee', 
                          'Total_Fixed_Closing_Fee', 'Total_FBA_Pick_Pack_Fee', 'Total_FBA_Weight_Handling_Fee', 
                          'Total_Technology_Fee', 'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 
                          'Product_Cost', 'Adjusted_Product_Cost', 'Product Profit/Loss', 'Remarks']

    df_final = df_final.rename(columns={'Adjusted_Product_Cost': 'Product Cost'})
    
    return df_final[df_final.columns.intersection(final_display_cols)], kpis

# --- 3. GLOBAL HELPER FUNCTIONS (AJIO) ---

def ajio_clean_order_id(s):
    """Cleans Ajio Order IDs by removing .0 suffix and converting to uppercase."""
    return str(s).replace('.0', '').strip().upper()

def ajio_clean_currency(s):
    """Cleans currency strings for conversion to float."""
    return pd.to_numeric(str(s).replace('₹', '').replace(',', '').strip(), errors='coerce').fillna(0)

def ajio_parse_date(s):
    """Cleans date strings which often contain ' IST' and converts to datetime."""
    try:
        return pd.to_datetime(str(s).replace(' IST', '').strip(), errors='coerce')
    except Exception:
        return pd.NaT

def ajio_process_report(uploaded_file, report_type):
    try:
        df = pd.read_csv(uploaded_file)
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')
        
        df.rename(columns={
            'Customer_Order_Number': 'Cust Order No',
            'Order_ID': 'Cust Order No',
            'Order_ID.1': 'Cust Order No', # Handles slightly different formats
            'Invoice_Value': 'Invoice Value',
            'Return_Value': 'Return Value',
            'Payment_Received': 'Payment Received',
            'Settlement_Date': 'Settlement Date',
            'Transaction_Date': 'Transaction Date',
            'Financial_Transaction_Type': 'Financial Type'
        }, inplace=True)

        if 'Cust Order No' not in df.columns:
             st.error(f"{report_type} report is missing 'Cust Order No' column.")
             return None
             
        df['Cust Order No'] = df['Cust Order No'].apply(ajio_clean_order_id)
        
        if report_type == 'GST':
            df['Invoice Value'] = df['Invoice Value'].apply(ajio_clean_currency)
            df['Order_Date'] = df['Transaction Date'].apply(ajio_parse_date)
            df_agg = df.groupby('Cust Order No').agg(
                {'Invoice Value': 'sum', 'Order_Date': 'min'}
            ).reset_index()
            df_agg.rename(columns={'Invoice Value': 'GST Invoice Value'}, inplace=True)
            return df_agg
            
        elif report_type == 'RTV':
            df['Return Value'] = df['Return Value'].apply(ajio_clean_currency)
            df_agg = df.groupby('Cust Order No')['Return Value'].sum().reset_index()
            return df_agg
            
        elif report_type == 'Payment':
            df['Payment Received'] = df['Payment Received'].apply(ajio_clean_currency)
            df['Settlement Date'] = df['Settlement Date'].apply(ajio_parse_date)
            df_agg = df.groupby('Cust Order No').agg(
                {'Payment Received': 'sum', 'Settlement Date': 'min'}
            ).reset_index()
            return df_agg
        
    except Exception as e:
        st.error(f"Error processing {report_type} report: {e}")
        return None

def ajio_get_csv_download_link(df):
    csv = df.to_csv(index=False).encode('utf-8')
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="Ajio_Reconciliation_Output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv">📥 Download Final Reconciliation CSV</a>'
    return href

# --- 4. MASTER EXECUTION FUNCTIONS ---

def run_amazon_tool():
    st.title("🛒 Amazon Seller Central Reconciliation")
    st.markdown("---")
    
    # 1. Input Sidebars
    with st.sidebar:
        st.header("Upload Reports")
        cost_sheet = st.file_uploader("1. Product Cost Sheet (SKU, Cost)", type=['xlsx', 'csv'], key='amz_cost')
        payment_reports = st.file_uploader("2. Payment Reports (.zip file(s))", type=['zip'], accept_multiple_files=True, key='amz_payment')
        mtr_reports = st.file_uploader("3. MTR Reports (CSV file(s))", type=['csv'], accept_multiple_files=True, key='amz_mtr')
        
        st.header("Financial Inputs")
        monthly_expense = st.number_input("Monthly Overhead Expense (Storage, Ads, etc.)", min_value=0.0, value=0.0, step=100.0, format="%.2f")

    if cost_sheet and payment_reports and mtr_reports:
        try:
            # Process Data
            df_cost = amz_process_cost_sheet(cost_sheet)
            df_payment, df_fees_summary = amz_process_payment_files(payment_reports)
            df_mtr = amz_process_mtr_files(mtr_reports)
            
            if df_cost is None or df_payment is None or df_mtr is None:
                st.error("One or more essential files failed to process. Check error messages above.")
                return

            # Add fees summary to payment df for merge
            df_payment['fees_summary'] = df_fees_summary.apply(lambda x: x.to_dict(), axis=1)

            # Create Final Reconciliation Table
            df_final, kpis = amz_create_final_reconciliation_df(df_mtr, df_payment, df_cost, monthly_expense)
            
            # --- DISPLAY DASHBOARD ---
            st.header("Summary of Financial Performance")
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Net Payment Received", f"INR {kpis['Total_Net_Payment']:,.2f}")
            c2.metric("Total Fees & Charges", f"INR {kpis['Total_Fees']:,.2f}")
            c3.metric("Total Product Cost", f"INR {kpis['Total_Product_Cost']:,.2f}")
            
            # Final P&L with color coding
            profit_style = "green" if kpis['Final_Profit_Loss'] >= 0 else "red"
            c4.markdown(
                f'<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; text-align: center;">'
                f'<div style="font-size: 14px; color: grey;">Final Profit/Loss (After Overhead)</div>'
                f'<div style="font-size: 28px; font-weight: bold; color: {profit_style};">INR {kpis["Final_Profit_Loss"]:,.2f}</div>'
                f'</div>', 
                unsafe_allow_html=True
            )

            st.markdown("---")
            
            # --- NEW: MISSING ORDERS SECTION ---
            st.subheader("🚨 Un-reconciled Orders (MTR found, Payment missing)")
            df_missing = df_final[df_final['Remarks'] == 'Order ID is not in this Payment report'].copy()
            
            missing_orders_count = df_missing['OrderID'].nunique()
            missing_orders_total_mtr = df_missing['MTR Invoice Amount'].sum()

            m1, m2 = st.columns(2)
            m1.metric("Missing Order IDs Count", f"{missing_orders_count:,}")
            m2.metric("Total MTR Value Missing Payment", f"INR {missing_orders_total_mtr:,.2f}")

            if df_missing.empty:
                st.info("🎉 All MTR orders have been found in the uploaded Payment Reports!")
            else:
                st.warning("The orders below are in the MTR report but are missing corresponding payment/fee details. **These require investigation.**")
                follow_up_cols = ['OrderID', 'Invoice Number', 'Invoice Date', 'Transaction Type', 'Sku', 'Quantity', 'MTR Invoice Amount', 'Remarks']
                
                missing_col_conf = {
                     "MTR Invoice Amount": st.column_config.NumberColumn("MTR Value", format="INR %.2f"),
                     "Quantity": st.column_config.NumberColumn("Qty", format="%d")
                }
                
                st.dataframe(df_missing[follow_up_cols], column_config=missing_col_conf, use_container_width=True, hide_index=True)

            st.markdown("---")
            
            # --- ITEM-LEVEL DETAIL SECTION ---
            st.header("1. Item-Level Reconciliation Detail")
            
            # Filtered View (as is)
            if 'OrderID' in df_final.columns:
                oids = ['All Orders'] + sorted(df_final['OrderID'].unique().tolist())
                sel_oid = st.selectbox("👉 Select Order ID to Filter Summary:", oids)
                
                if sel_oid != 'All Orders':
                    df_disp = df_final[df_final['OrderID'] == sel_oid].copy()
                else:
                    df_disp = df_final.sort_values('OrderID').copy()
            else: 
                df_disp = df_final.copy()
            
            # Define the columns for display configuration
            num_cols = ['MTR Invoice Amount', 'Net Payment', 'Total_Commission_Fee', 'Total_Fixed_Closing_Fee', 
                        'Total_FBA_Pick_Pack_Fee', 'Total_FBA_Weight_Handling_Fee', 'Total_Technology_Fee', 
                        'Total_Fees_KPI', 'Total_Tax_TCS_TDS', 'Product Cost', 'Product Profit/Loss']
            
            col_conf = {c: st.column_config.NumberColumn(format="INR %.2f") for c in num_cols}

            if 'Remarks' in df_disp.columns:
                col_conf['Remarks'] = st.column_config.TextColumn("Status/Remarks", help="Checks if order is found in Payment Report")

            # Use Tabs for Organization
            t1, t2 = st.tabs(["📄 All Item Data", "🔗 Total Fees Split (Based on Report)"])
            
            with t1:
                # Display the main table
                st.dataframe(df_disp, column_config=col_conf, use_container_width=True, hide_index=True)
            
            with t2:
                # Display the total fees applied across the whole reconciliation period (from the Payment Report)
                fees_df = pd.DataFrame({
                    'Fee Component': [
                        'Total Commission Fee', 
                        'Total Fixed Closing Fee', 
                        'Total FBA Pick & Pack Fee', 
                        'Total FBA Weight Handling Fee', 
                        'Total Technology Fee',
                        'Total Tax/TDS/TCS'
                    ],
                    'Amount': [
                        df_fees_summary['Total_Commission_Fee'].iloc[0],
                        df_fees_summary['Total_Fixed_Closing_Fee'].iloc[0],
                        df_fees_summary['Total_FBA_Pick_Pack_Fee'].iloc[0],
                        df_fees_summary['Total_FBA_Weight_Handling_Fee'].iloc[0],
                        df_fees_summary['Total_Technology_Fee'].iloc[0],
                        df_fees_summary['Total_Tax_TCS_TDS'].iloc[0],
                    ]
                })
                fees_df['Amount'] = fees_df['Amount'].round(2)
                st.subheader(f"Total Report Period Fees/Tax: INR {df_fees_summary['Total_Fees_KPI'].iloc[0]:,.2f}")
                st.dataframe(fees_df, hide_index=True, use_container_width=True, 
                             column_config={'Amount': st.column_config.NumberColumn(format="INR %.2f")})

            st.markdown(amz_get_csv_download_link(df_final), unsafe_allow_html=True)

        except Exception as e:
            st.error(f"An unexpected error occurred during processing: {e}")
            st.exception(e)

    else:
        st.info("Please upload the Cost Sheet, Payment Reports, and MTR Reports in the sidebar to begin Amazon reconciliation.")
        
def run_ajio_tool():
    st.title("👗 Ajio Reconciliation (Sales vs. Payment)")
    st.markdown("---")
    
    # 1. Input Sidebars
    with st.sidebar:
        st.header("Upload Ajio Reports")
        gst_report = st.file_uploader("1. GST Report (Sales)", type=['csv'], key='ajio_gst')
        rtv_report = st.file_uploader("2. RTV Report (Returns)", type=['csv'], key='ajio_rtv')
        payment_report = st.file_uploader("3. Payment Report (Settlements)", type=['csv'], key='ajio_payment')
        
    if gst_report or rtv_report or payment_report:
        try:
            # Process Data
            df_gst = ajio_process_report(gst_report, 'GST') if gst_report else pd.DataFrame({'Cust Order No': [], 'GST Invoice Value': [], 'Order_Date': []})
            df_rtv = ajio_process_report(rtv_report, 'RTV') if rtv_report else pd.DataFrame({'Cust Order No': [], 'Return Value': []})
            df_payment = ajio_process_report(payment_report, 'Payment') if payment_report else pd.DataFrame({'Cust Order No': [], 'Payment Received': [], 'Settlement Date': []})

            # Merge all three reports on Cust Order No
            df_recon = pd.merge(df_gst, df_rtv, on='Cust Order No', how='outer').fillna(0)
            df_recon = pd.merge(df_recon, df_payment, on='Cust Order No', how='outer').fillna(0)
            
            if df_recon.empty:
                st.warning("No data to process after combining reports.")
                return

            # Final Calculations
            df_recon['Expected Payment'] = df_recon['GST Invoice Value'] - df_recon['Return Value']
            df_recon['Final Difference'] = df_recon['Expected Payment'] - df_recon['Payment Received']
            
            # Status Assignment
            def get_status(diff):
                if diff == 0: return "🟢 Settled"
                elif diff > 0: return "⚠️ Less Payment"
                else: return "🔵 Over Payment"

            df_recon['Status'] = df_recon.apply(
                lambda row: "🔴 Not Received" if row['Payment Received'] == 0 and row['Expected Payment'] > 0 
                            else get_status(row['Final Difference']), 
                axis=1
            )
            
            # --- DISPLAY DASHBOARD ---
            total_sales = df_recon['GST Invoice Value'].sum()
            total_returns = df_recon['Return Value'].sum()
            total_expected = df_recon['Expected Payment'].sum()
            total_received = df_recon['Payment Received'].sum()
            net_pending = total_expected - total_received

            st.header("Overall Reconciliation Summary")
            
            d1, d2, d3, d4, d5 = st.columns(5)
            d1.metric("Total Sales (GST)", f"₹ {total_sales:,.2f}")
            d2.metric("Total Returns (RTV)", f"₹ {total_returns:,.2f}")
            d3.metric("Net Expected Payment", f"₹ {total_expected:,.2f}")
            d4.metric("Total Received", f"₹ {total_received:,.2f}")
            d5.metric("Net Pending/Difference", f"₹ {net_pending:,.2f}")
            
            st.markdown("---")
            
            st.header("Order-Level Reconciliation Detail")

            final_cols = ['Cust Order No', 'Order_Date', 'Settlement Date', 'GST Invoice Value', 'Return Value', 
                          'Payment Received', 'Expected Payment', 'Final Difference', 'Status']
                          
            # Table configuration for currency columns
            col_config = {
                "Order_Date": st.column_config.DateColumn("Order Date"),
                "Settlement Date": st.column_config.DateColumn("Settlement Date"),
                "GST Invoice Value": st.column_config.NumberColumn("Sales", format="₹%.2f"),
                "Return Value": st.column_config.NumberColumn("Returns", format="₹%.2f"),
                "Payment Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                "Expected Payment": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                "Final Difference": st.column_config.NumberColumn("Diff", format="₹%.2f"),
            }

            t1, t2, t3 = st.tabs(["🔴 Pending/Short Payment", "🟢 Settled Orders", "📄 All Orders"])
            
            with t1: st.dataframe(df_recon[df_recon['Status'].isin(["🔴 Not Received", "⚠️ Less Payment", "🔵 Over Payment"])][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
            with t2: st.dataframe(df_recon[df_recon['Status']=="🟢 Settled"][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
            with t3: st.dataframe(df_recon[final_cols], column_config=col_config, use_container_width=True, hide_index=True)

            st.markdown(ajio_get_csv_download_link(df_recon), unsafe_allow_html=True)

        except Exception as e: st.error(f"Error: {e}")
    else: st.info("Upload files in the sidebar to begin Ajio reconciliation.")

# ==========================================
# MASTER EXECUTION
# ==========================================
st.sidebar.title("🔧 Navigation")
tool_selection = st.sidebar.selectbox("Select Platform:", ["Amazon Reconciliation", "Ajio Reconciliation"])

if tool_selection == "Amazon Reconciliation":
    run_amazon_tool()
elif tool_selection == "Ajio Reconciliation":
    run_ajio_tool()
