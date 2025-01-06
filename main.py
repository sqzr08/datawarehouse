import os
from google.cloud import storage, bigquery
import pandas as pd
from io import StringIO

def etl_process(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Processes CSV files and transforms them into a star schema, then loads them into BigQuery.
    """
    try:
        # Extract bucket and file name from the event
        bucket_name = event['bucket']
        file_name = event['name']

        # Initialize Google Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # List of required files
        required_files = [
            "ordersdf.csv",
            "customersdf.csv",
            "orderdetailsdf.csv",
            "productsdf.csv"
        ]
        
        # Ensure the uploaded file is part of the required files
        if file_name not in required_files:
            print(f"Ignored file: {file_name}. Not part of the required ETL files.")
            return

        # Download and read all required CSV files into DataFrames
        dataframes = {}
        for file in required_files:
            blob = bucket.blob(file)
            csv_data = blob.download_as_text()
            df_name = file.split(".")[0]
            dataframes[df_name] = pd.read_csv(StringIO(csv_data))

        # Extract DataFrames
        orders_df = dataframes["ordersdf"]
        customers_df = dataframes["customersdf"]
        order_details_df = dataframes["orderdetailsdf"]
        products_df = dataframes["productsdf"]

        # Start Transformations
        print("Starting transformations...")

        # Feature Engineering
        customer_sales = order_details_df.groupby('Customer ID')['Sales'].sum().reset_index()
        customer_sales.rename(columns={'Sales': 'TotalSalesPerCustomer'}, inplace=True)

        order_sales = order_details_df.groupby('Order ID')['Sales'].sum().reset_index()
        order_sales.rename(columns={'Sales': 'TotalSalesPerOrder'}, inplace=True)

        product_sales = order_details_df.groupby('Product ID')['Sales'].sum().reset_index()
        product_sales.rename(columns={'Sales': 'TotalSalesPerProduct'}, inplace=True)

        product_avg_profit = order_details_df.groupby('Product ID')['Profit'].mean().reset_index()
        product_avg_profit.rename(columns={'Profit': 'AverageProfitPerProduct'}, inplace=True)

        print("Feature engineering completed. Creating star schema...")

        # Combine unique dates from both 'Order Date' and 'Ship Date'
        time_dim = pd.concat([orders_df[['Order Date']], orders_df[['Ship Date']]]) \
        .drop_duplicates().reset_index(drop=True)

        # Rename the date column to 'Date' for consistency
        time_dim.rename(columns={time_dim.columns[0]: 'Date'}, inplace=True)

        # Generate a unique identifier for each date
        time_dim['TimeID'] = range(1, len(time_dim) + 1)

        # Extract time attributes
        time_dim['Day'] = pd.to_datetime(time_dim['Date'], format="%d/%m/%Y").dt.day
        time_dim['Month'] = pd.to_datetime(time_dim['Date'], format="%d/%m/%Y").dt.month
        time_dim['Year'] = pd.to_datetime(time_dim['Date'], format="%d/%m/%Y").dt.year
        time_dim['Quarter'] = pd.to_datetime(time_dim['Date'], format="%d/%m/%Y").dt.quarter

        customer_dim = customers_df.drop_duplicates().reset_index(drop=True)
        product_dim = products_df.drop_duplicates().reset_index(drop=True)

        # Create Sales Fact Table
        sales_fact = order_details_df.merge(customer_sales, on='Customer ID', how='left') \
                                     .merge(order_sales, on='Order ID', how='left') \
                                     .merge(product_sales, on='Product ID', how='left') \
                                     .merge(product_avg_profit, on='Product ID', how='left')
        sales_fact = sales_fact.merge(orders_df[['Order ID', 'Order Date', 'Ship Date']], on='Order ID', how='left')

        # Add Time Dimension IDs
        # Merge Order Date with Time Dimension
        sales_fact = sales_fact.merge(time_dim, left_on='Order Date', right_on='Date', how='left')
        sales_fact.rename(columns={'TimeID': 'OrderDateID'}, inplace=True)
        sales_fact.drop(columns=['Date'], inplace=True)

        # Merge Ship Date with Time Dimension
        sales_fact = sales_fact.merge(time_dim, left_on='Ship Date', right_on='Date', how='left')
        sales_fact.rename(columns={'TimeID': 'ShipDateID'}, inplace=True)
        sales_fact.drop(columns=['Date'], inplace=True)

        # Final selection of columns for the sales_fact table
        sales_fact = sales_fact[[
            'Product ID', 'Customer ID', 'OrderDateID', 'ShipDateID', 'Order ID', 
            'Quantity', 'Discount', 'Sales', 'Profit', 
            'TotalSalesPerCustomer', 'TotalSalesPerOrder',
            'TotalSalesPerProduct', 'AverageProfitPerProduct'
        ]]


        print("Star schema created successfully.")

        # Load Data into BigQuery
        print("Loading data into BigQuery...")
        bq_client = bigquery.Client()
        dataset_id = "refined-area-442505-p5.etl_output" 

        tables = {
            "time_dim": time_dim,
            "customer_dim": customer_dim,
            "product_dim": product_dim,
            "sales_fact": sales_fact
        }

        for table_name, table_data in tables.items():
            table_id = f"{dataset_id}.{table_name}"
            print(f"Loading {table_name} to {table_id}...")
            job = bq_client.load_table_from_dataframe(table_data, table_id)
            job.result()  # Wait for the load job to complete
            print(f"Table {table_name} loaded successfully.")

        print("ETL process completed successfully.")
        return "ETL process completed successfully.", 200

    except Exception as e:
        print(f"Error during ETL process: {e}")
        return f"Error: {e}", 500
