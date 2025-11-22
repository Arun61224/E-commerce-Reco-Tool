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
    """Generates an Excel template for product cost sheet."""
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
    """Reads and validates the uploaded cost sheet."""
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
            st.error("Invalid file format for Cost Sheet. Please upload Excel (.xlsx, .xls) or CSV (.csv) files.")
            return None

        # Standardize column names
        df_cost.columns = df_cost.columns.str.strip().str.replace(' ', '_')
        df_cost = df_cost[['SKU', 'Product_Cost']].copy()
        df_cost.rename(columns={'Product_Cost': 'Product Cost'}, inplace=True)

        if not all(col in df_cost.columns for col in required_cols):
            st.error(f"Cost Sheet missing required columns: {required_cols}")
            return None

        df_cost['SKU'] = df_cost['SKU'].astype(str).str.strip().str.upper()
        df_cost['Product Cost'] = pd.to_numeric(df_cost['Product Cost'], errors='coerce')
        df_cost.dropna(subset=['SKU', 'Product Cost'], inplace=True)
        df_cost = df_cost.drop_duplicates(subset=['SKU'])

        if df_cost.empty:
            st.error("Cost Sheet is empty or contains no valid data.")
            return None

        st.success(f"Cost Sheet loaded with {len(df_cost)} unique SKUs.")
        return df_cost

    except Exception as e:
        st.error(f"Error reading or processing Cost Sheet: {e}")
        return None

@st.cache_data
def amz_process_settlement_report(uploaded_file):
    """Reads and preprocesses the Amazon Settlement Report."""
    try:
        df_settlement = pd.read_csv(uploaded_file, compression='zip', encoding='latin-1')
    except zipfile.BadZipFile:
        st.error("Error: This does not appear to be a valid ZIP file. Please upload the compressed (.zip) file directly from Amazon.")
        return None
    except Exception as e:
        st.error(f"Error reading Settlement Report: {e}")
        return None

    # Rename columns for clarity and consistency
    col_mapping = {
        'transaction-type': 'Transaction Type',
        'settlement-id': 'Settlement ID',
        'settlement-start-date': 'Settlement Start Date',
        'settlement-end-date': 'Settlement End Date',
        'deposit-date': 'Deposit Date',
        'order-id': 'Order ID',
        'sku': 'SKU',
        'marketplace-name': 'Marketplace',
        'amount-type': 'Amount Type',
        'amount-description': 'Amount Description',
        'amount': 'Amount',
        'fulfillment-id': 'Fulfillment ID'
    }

    df_settlement.columns = [col.lower() for col in df_settlement.columns]
    df_settlement.rename(columns=col_mapping, inplace=True)

    required_cols = ['Transaction Type', 'Order ID', 'SKU', 'Amount Type', 'Amount Description', 'Amount']
    if not all(col in df_settlement.columns for col in required_cols):
        st.error(f"Settlement Report missing required columns: {required_cols}")
        return None

    # Preprocessing
    df_settlement['SKU'] = df_settlement['SKU'].astype(str).str.strip().str.upper()
    df_settlement['Amount'] = pd.to_numeric(df_settlement['Amount'], errors='coerce').fillna(0)

    st.success(f"Settlement Report loaded with {len(df_settlement)} transactions.")
    return df_settlement

@st.cache_data
def amz_reconcile_sales(df_settlement, df_cost):
    """Performs the core Amazon reconciliation logic."""
    df_sales = df_settlement[df_settlement['Transaction Type'] == 'Order'].copy()

    # Calculate Total Sale Amount and Total Commission/Fee per Order ID
    df_sales_pivot = df_sales.pivot_table(
        index=['Order ID', 'SKU'],
        columns=['Amount Type', 'Amount Description'],
        values='Amount',
        aggfunc='sum'
    ).fillna(0).reset_index()

    # Flatten column names for easier access
    df_sales_pivot.columns = [' '.join(col).strip() if col[0] else col[1] for col in df_sales_pivot.columns.values]
    
    # Identify relevant columns (these might vary slightly, so we check for presence)
    
    # Sales/Revenue
    revenue_cols = [c for c in df_sales_pivot.columns if 'item-price principal' in c.lower()]
    df_sales_pivot['Sales Value'] = df_sales_pivot[revenue_cols].sum(axis=1) if revenue_cols else 0

    # Commissions/Referral Fee
    comm_cols = [c for c in df_sales_pivot.columns if 'fee referral fee' in c.lower()]
    df_sales_pivot['Commission'] = df_sales_pivot[comm_cols].sum(axis=1) if comm_cols else 0

    # FBA Fees (if applicable)
    fba_cols = [c for c in df_sales_pivot.columns if 'fee fba' in c.lower()]
    df_sales_pivot['FBA Fee'] = df_sales_pivot[fba_cols].sum(axis=1) if fba_cols else 0
    
    # Fixed Closing Fee (if applicable)
    fixed_cols = [c for c in df_sales_pivot.columns if 'fee fixed closing fee' in c.lower()]
    df_sales_pivot['Fixed Closing Fee'] = df_sales_pivot[fixed_cols].sum(axis=1) if fixed_cols else 0
    
    # Shipping Charges collected from customer
    shipping_collected_cols = [c for c in df_sales_pivot.columns if 'item-price shipping' in c.lower()]
    df_sales_pivot['Shipping Collected'] = df_sales_pivot[shipping_collected_cols].sum(axis=1) if shipping_collected_cols else 0

    # Final Reconciliation Columns
    df_sales_recon = df_sales_pivot[['Order ID', 'SKU', 'Sales Value', 'Commission', 'FBA Fee', 'Fixed Closing Fee', 'Shipping Collected']].copy()
    
    # Merge with Cost Sheet
    df_sales_recon = pd.merge(df_sales_recon, df_cost, on='SKU', how='left')
    df_sales_recon['Product Cost'] = df_sales_recon['Product Cost'].fillna(0) # Assume 0 cost if SKU not found

    # Calculate Net Realization
    df_sales_recon['Total Fees'] = df_sales_recon['Commission'] + df_sales_recon['FBA Fee'] + df_sales_recon['Fixed Closing Fee']
    
    # The 'Amount' in the report is typically the net total credited/debited for that specific transaction line.
    # For a simple reconciliation, we'll calculate the expected payout based on the extracted components.

    # Expected Payout = Sales Value + Shipping Collected - Total Fees - Product Cost (for P&L view)
    df_sales_recon['Expected Realization'] = df_sales_recon['Sales Value'] + df_sales_recon['Shipping Collected'] + df_sales_recon['Total Fees'] 
    
    # We need the actual total amount credited for the order to compare.
    # Group the original settlement report by Order ID and sum the 'Amount' for 'Order' transactions
    df_actual_payout = df_sales[df_sales['Transaction Type'] == 'Order'].groupby('Order ID')['Amount'].sum().reset_index()
    df_actual_payout.rename(columns={'Amount': 'Actual Payment Received'}, inplace=True)
    
    # Merge actual payment back (assuming one product per order for simplicity, otherwise this needs more complex mapping)
    # Since pivot already grouped by SKU and Order ID, we need the total order amount.
    
    # Re-group the original settlement report for the *total* paid out per order ID (all transaction types related to the order)
    df_total_order_payout = df_settlement.groupby('Order ID')['Amount'].sum().reset_index()
    df_total_order_payout.rename(columns={'Amount': 'Actual Total Payout'}, inplace=True)
    
    df_recon = pd.merge(df_sales_recon, df_total_order_payout, on='Order ID', how='left')
    
    # Calculate expected final net realization (Sales + Shipping - Fees)
    df_recon['Expected Payment'] = df_recon['Sales Value'] + df_recon['Shipping Collected'] + df_recon['Total Fees']
    
    # Rename for clarity
    df_recon.rename(columns={'Actual Total Payout': 'Actual Payment Received'}, inplace=True)
    
    # Calculate Difference
    df_recon['Final Difference'] = df_recon['Actual Payment Received'] - df_recon['Expected Payment']
    
    # Determine Status
    df_recon['Status'] = '🟢 Settled'
    df_recon.loc[df_recon['Final Difference'] < -0.5, 'Status'] = '⚠️ Less Payment'
    df_recon.loc[df_recon['Final Difference'] > 0.5, 'Status'] = '🔵 Over Payment'
    df_recon.loc[df_recon['Actual Payment Received'] == 0, 'Status'] = '🔴 Not Received'

    return df_recon


def amz_get_csv_download_link(df):
    """Generates a downloadable link for the reconciliation data."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Amazon Reconciliation', index=False)
    
    b64 = base64.b64encode(output.getvalue()).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" download="Amazon_Reconciliation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx">📥 Download Reconciliation Excel File</a>'
    return href

# --- 3. AMAZON RECONCILIATION UI/LOGIC ---
def amazon_reconciliation_tool():
    """Main function for Amazon Reconciliation UI."""
    st.header("Amazon Reconciliation Tool (Settlement Report)")
    st.info("Upload the Amazon Settlement Report (Zipped CSV) and your Product Cost Sheet (Excel/CSV).")

    # Template Download
    st.download_button(
        label="Download Cost Sheet Template (Excel)",
        data=amz_create_cost_sheet_template(),
        file_name='Amazon_Cost_Sheet_Template.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    col1, col2 = st.columns(2)
    with col1:
        amz_settlement_file = st.file_uploader("Upload Amazon Settlement Report (ZIPPED .csv)", type=['zip'], key='amz_settlement')
    with col2:
        amz_cost_file = st.file_uploader("Upload Product Cost Sheet (.xlsx or .csv)", type=['xlsx', 'xls', 'csv'], key='amz_cost')

    if amz_settlement_file and amz_cost_file:
        df_settlement = amz_process_settlement_report(amz_settlement_file)
        df_cost = amz_process_cost_sheet(amz_cost_file)

        if df_settlement is not None and df_cost is not None:
            st.markdown("---")
            st.subheader("Reconciliation Summary")

            try:
                # Perform reconciliation
                df_recon = amz_reconcile_sales(df_settlement, df_cost)

                # Summary Metrics
                total_orders = df_recon['Order ID'].nunique()
                total_settled = df_recon[df_recon['Status']=='🟢 Settled']['Order ID'].nunique()
                total_less_payment = df_recon[df_recon['Status']=='⚠️ Less Payment']['Order ID'].nunique()
                total_not_received = df_recon[df_recon['Status']=='🔴 Not Received']['Order ID'].nunique()
                
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                col_m1.metric("Total Order Lines", len(df_recon))
                col_m2.metric("Settled Orders (🟢)", total_settled)
                col_m3.metric("Less/Not Received (🔴+⚠️)", total_less_payment + total_not_received, delta=f"Total Diff: ₹{df_recon['Final Difference'].sum():.2f}")
                col_m4.metric("Total Sales Value", f"₹{df_recon['Sales Value'].sum():.2f}")
                
                st.markdown("---")
                st.subheader("Reconciliation Details")

                t1, t2, t3 = st.tabs(["Issues (🔴/⚠️/🔵)", "Settled (🟢)", "Full Data"])
                
                final_cols = ['Status', 'Order ID', 'SKU', 'Sales Value', 'Commission', 'FBA Fee', 'Fixed Closing Fee', 'Expected Payment', 'Actual Payment Received', 'Final Difference', 'Product Cost']
                
                col_config = {
                    "Status": st.column_config.TextColumn("Status"),
                    "Order ID": "Order ID",
                    "SKU": "SKU",
                    "Sales Value": st.column_config.NumberColumn("Sale Value", format="₹%.2f"),
                    "Commission": st.column_config.NumberColumn("Comm", format="₹%.2f"),
                    "FBA Fee": st.column_config.NumberColumn("FBA Fee", format="₹%.2f"),
                    "Fixed Closing Fee": st.column_config.NumberColumn("Fixed Fee", format="₹%.2f"),
                    "Product Cost": st.column_config.NumberColumn("Cost", format="₹%.2f"),
                    "Expected Payment": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                    "Actual Payment Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                    "Final Difference": st.column_config.NumberColumn("Diff", format="₹%.2f"),
                }


                with t1: st.dataframe(df_recon[df_recon['Status'].isin(["🔴 Not Received", "⚠️ Less Payment", "🔵 Over Payment"])][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t2: st.dataframe(df_recon[df_recon['Status']=="🟢 Settled"][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t3: st.dataframe(df_recon[final_cols], column_config=col_config, use_container_width=True, hide_index=True)

                st.markdown(amz_get_csv_download_link(df_recon), unsafe_allow_html=True)

            except Exception as e:
                st.error(f"Error during reconciliation: {e}")
                st.exception(e) # Show full traceback for debugging
    else:
        st.info("Upload files to begin reconciliation.")

# --- 4. GLOBAL HELPER FUNCTIONS (AJIO) ---
@st.cache_data
def ajio_process_ajio_report(uploaded_file):
    """Reads and preprocesses the Ajio report."""
    try:
        filename = uploaded_file.name.lower()
        if filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif filename.endswith(('.csv')):
            try:
                df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(uploaded_file, encoding='latin-1')
            uploaded_file.seek(0)
        else:
            st.error("Invalid file format for Ajio Report. Please upload Excel (.xlsx, .xls) or CSV (.csv) files.")
            return None
    except Exception as e:
        st.error(f"Error reading Ajio Report: {e}")
        return None

    # Standardize and select required columns
    df.columns = df.columns.str.strip().str.replace('[^A-Za-z0-9]+', '_', regex=True)
    
    # Expected Ajio columns based on common reports
    col_mapping = {
        'Order_ID': 'Order ID',
        'SKU_Code': 'SKU',
        'Invoice_Amount': 'Invoice Amount',
        'Payment_Received': 'Payment Received',
        'Total_Return_Value': 'Return Value',
        'Commission_Amount': 'Commission',
        'GST_on_Commission': 'GST on Commission',
        'Shipping_Charges': 'Shipping Fee',
        'GST_on_Shipping_Charges': 'GST on Shipping Fee',
        # Add more mappings if needed based on the user's specific Ajio file
    }
    
    # Try to find common columns even if the header names are slightly different
    df.rename(columns=lambda x: col_mapping.get(x, x), inplace=True)
    
    # Define required core columns for reconciliation
    required_core_cols = ['Order ID', 'SKU', 'Invoice Amount', 'Payment Received']
    
    if not all(col in df.columns for col in required_core_cols):
        missing = [col for col in required_core_cols if col not in df.columns]
        st.error(f"Ajio Report missing required columns: {missing}. Please check column headers.")
        return None

    # Convert essential columns to numeric, coercion to handle non-numeric data gracefully
    for col in ['Invoice Amount', 'Payment Received']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # Handle optional columns, assume 0 if missing
    df['Return Value'] = pd.to_numeric(df.get('Return Value', 0), errors='coerce').fillna(0)
    df['Commission'] = pd.to_numeric(df.get('Commission', 0), errors='coerce').fillna(0)
    df['GST on Commission'] = pd.to_numeric(df.get('GST on Commission', 0), errors='coerce').fillna(0)
    df['Shipping Fee'] = pd.to_numeric(df.get('Shipping Fee', 0), errors='coerce').fillna(0)
    df['GST on Shipping Fee'] = pd.to_numeric(df.get('GST on Shipping Fee', 0), errors='coerce').fillna(0)
    
    df['SKU'] = df['SKU'].astype(str).str.strip()
    df.dropna(subset=['Order ID', 'SKU'], inplace=True)
    
    st.success(f"Ajio Report loaded with {len(df)} transactions.")
    return df

@st.cache_data
def ajio_reconcile_sales(df_ajio):
    """Performs the core Ajio reconciliation logic."""
    df_recon = df_ajio.copy()
    
    # Ajio reports typically show the final amount received, but we need to calculate
    # the expected payment based on the invoice value and deductions.
    
    # Total deductions (Fees + GST on Fees + Returns)
    df_recon['Total Deductions'] = (
        df_recon['Return Value'].abs() + 
        df_recon['Commission'].abs() + 
        df_recon['GST on Commission'].abs() + 
        df_recon['Shipping Fee'].abs() + 
        df_recon['GST on Shipping Fee'].abs()
    )
    
    # Expected Payment = Invoice Amount - Total Deductions
    # NOTE: Ajio's Payment Received should ideally match this.
    df_recon['Expected Payment'] = df_recon['Invoice Amount'] - df_recon['Total Deductions']
    
    # Calculate Difference
    df_recon['Final Difference'] = df_recon['Payment Received'] - df_recon['Expected Payment']
    
    # Determine Status
    df_recon['Status'] = '🟢 Settled'
    df_recon.loc[df_recon['Final Difference'] < -0.5, 'Status'] = '⚠️ Less Payment'
    df_recon.loc[df_recon['Final Difference'] > 0.5, 'Status'] = '🔵 Over Payment'
    df_recon.loc[df_recon['Payment Received'] == 0, 'Status'] = '🔴 Not Received'
    
    return df_recon


def ajio_get_csv_download_link(df):
    """Generates a downloadable link for the reconciliation data."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Ajio Reconciliation', index=False)
    
    b64 = base64.b64encode(output.getvalue()).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" download="Ajio_Reconciliation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx">📥 Download Reconciliation Excel File</a>'
    return href

# --- 5. AJIO RECONCILIATION UI/LOGIC ---
def ajio_reconciliation_tool():
    """Main function for Ajio Reconciliation UI."""
    st.header("Ajio Reconciliation Tool")
    st.info("Upload the Ajio Order/Settlement Report (Excel/CSV).")

    ajio_report_file = st.file_uploader("Upload Ajio Report (.xlsx or .csv)", type=['xlsx', 'xls', 'csv'], key='ajio_report')

    if ajio_report_file:
        df_ajio = ajio_process_ajio_report(ajio_report_file)

        if df_ajio is not None:
            st.markdown("---")
            st.subheader("Reconciliation Summary")

            try:
                # Perform reconciliation
                df_recon = ajio_reconcile_sales(df_ajio)

                # Summary Metrics
                total_orders = df_recon['Order ID'].nunique()
                total_settled = df_recon[df_recon['Status']=='🟢 Settled']['Order ID'].nunique()
                total_less_payment = df_recon[df_recon['Status']=='⚠️ Less Payment']['Order ID'].nunique()
                total_not_received = df_recon[df_recon['Status']=='🔴 Not Received']['Order ID'].nunique()
                total_over_payment = df_recon[df_recon['Status']=='🔵 Over Payment']['Order ID'].nunique()
                
                col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
                col_m1.metric("Total Order Lines", len(df_recon))
                col_m2.metric("Settled Orders (🟢)", total_settled)
                col_m3.metric("Less Payment (⚠️)", total_less_payment)
                col_m4.metric("Not Received (🔴)", total_not_received)
                col_m5.metric("Total Diff", f"₹{df_recon['Final Difference'].sum():.2f}")
                
                st.markdown("---")
                st.subheader("Reconciliation Details")

                t1, t2, t3 = st.tabs(["Issues (🔴/⚠️/🔵)", "Settled (🟢)", "Full Data"])
                
                # Selecting relevant columns for display
                final_cols = [
                    'Status', 'Order ID', 'SKU', 'Invoice Amount', 'Commission', 'GST on Commission', 
                    'Shipping Fee', 'GST on Shipping Fee', 'Return Value', 'Expected Payment', 
                    'Payment Received', 'Final Difference'
                ]
                
                col_config = {
                    "Status": st.column_config.TextColumn("Status"),
                    "Order ID": "Order ID",
                    "SKU": "SKU",
                    "Invoice Amount": st.column_config.NumberColumn("Invoice Amt", format="₹%.2f"),
                    "Commission": st.column_config.NumberColumn("Comm", format="₹%.2f"),
                    "GST on Commission": st.column_config.NumberColumn("Comm GST", format="₹%.2f"),
                    "Shipping Fee": st.column_config.NumberColumn("Shipping Fee", format="₹%.2f"),
                    "GST on Shipping Fee": st.column_config.NumberColumn("Shipping GST", format="₹%.2f"),
                    "Return Value": st.column_config.NumberColumn("Returns", format="₹%.2f"),
                    "Payment Received": st.column_config.NumberColumn("Received", format="₹%.2f"),
                    "Expected Payment": st.column_config.NumberColumn("Expected", format="₹%.2f"),
                    "Final Difference": st.column_config.NumberColumn("Diff", format="₹%.2f"),
                }

                with t1: st.dataframe(df_recon[df_recon['Status'].isin(["🔴 Not Received", "⚠️ Less Payment", "🔵 Over Payment"])][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t2: st.dataframe(df_recon[df_recon['Status']=="🟢 Settled"][final_cols], column_config=col_config, use_container_width=True, hide_index=True)
                with t3: st.dataframe(df_recon[final_cols], column_config=col_config, use_container_width=True, hide_index=True)

                st.markdown(ajio_get_csv_download_link(df_recon), unsafe_allow_html=True)

            except Exception as e: st.error(f"Error: {e}")
    else: st.info("Upload files.")

# ==========================================
# MASTER EXECUTION
# ==========================================
st.sidebar.title("🔧 Navigation")
tool_selection = st.sidebar.selectbox("Select Platform:", ["Amazon Reconciliation", "Ajio Reconciliation"])

if tool_selection == "Amazon Reconciliation":
    amazon_reconciliation_tool()
elif tool_selection == "Ajio Reconciliation":
    ajio_reconciliation_tool()
