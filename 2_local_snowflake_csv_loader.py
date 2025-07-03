#!/usr/bin/env python3
"""
Simple Data Loader for Crypto Broker Analytics

Loads CSV files to existing Snowflake tables using staging and INFER_SCHEMA.

Update the script with the correct information for your Snowflake environment.

"""

import os, sys
import snowflake.connector

def create_snowflake_connection(test_mode=False):
    """Create Snowflake connection with optional test configuration"""
    config = {
        'user': 'YOUR_SNOWFLAKE_USER',
        'password': 'YOUR_SNOWFLAKE_PASSWORD',
        'account': 'YOUR_SNOWFLAKE_ACCOUNT',
    }
    
    if test_mode:
        config.update({
            'enable_connection_diag': True,
            'connection_diag_log_path': "./logs"
        })
    else: 
        config.update({
            'warehouse': 'TRADING_ANALYTICS_WH',
            'database': 'TRADING_ANALYTICS',
            'schema': 'RAW_DATA'
        })
    
    return snowflake.connector.connect(**config)

def upload_file(cursor, file_path):
    """Upload file to Snowflake stage"""
    cursor.execute(f"PUT file://{file_path} @CSV_STAGE")

def load_user_profiles(cursor, filename):
    """Load user profiles"""
    
    cursor.execute(f"""
        COPY INTO USER_PROFILES 
        FROM '@CSV_STAGE/{filename}'
        FILE_FORMAT = CSV_FORMAT
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
            ON_ERROR = 'CONTINUE'
            INCLUDE_METADATA = (
                _loaded_at = METADATA$START_SCAN_TIME, 
                _file_name = METADATA$FILENAME
            )        
    """)
    
    print(f"User profiles loaded from {filename}")

def load_order_book(cursor, filename):
    """Load order book data """
    cursor.execute(f"""
        COPY INTO ORDER_BOOK 
        FROM '@CSV_STAGE/{filename}'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
            ON_ERROR = 'CONTINUE'
            INCLUDE_METADATA = (
                _loaded_at = METADATA$START_SCAN_TIME, 
                _file_name = METADATA$FILENAME
            )
    """)
    
    print(f"Order book data loaded from {filename}")

def load_user_trades(cursor, filename):
    """Load user trades """
    cursor.execute(f"""
        COPY INTO USER_TRADES
        FROM '@CSV_STAGE/{filename}'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
            ON_ERROR = 'CONTINUE'
            INCLUDE_METADATA = (
                _loaded_at = METADATA$START_SCAN_TIME, 
                _file_name = METADATA$FILENAME
            )
    """)
    
    print(f"User trades loaded from {filename}")

def load_file(cursor, file_path):
    """Load a CSV file based on its name pattern"""
    filename = os.path.basename(file_path)

    # Upload files to stage
    upload_file(cursor, file_path)
    
    # Route based on filename
    if 'user_profile' in filename.lower():
        load_user_profiles(cursor, filename)
    elif 'order_book' in filename.lower():
        load_order_book(cursor, filename)
    elif 'trade' in filename.lower():
        load_user_trades(cursor, filename)
    else:
        print(f"Unknown file type: {filename}")

def main():

    if sys.argv[1] == "test": 
        conn = create_snowflake_connection(test_mode=True)
        cursor = conn.cursor()

        # Test the connection to Snowflake with a simple query
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        print(f"✅ Connection successful! Snowflake version: {result[0]}")

        cursor.close()
        conn.close()
        return

    # Load the data into Snowflake from the sample_data directory
    conn = create_snowflake_connection()
    cursor = conn.cursor()

    try:
        sample_dir = "sample_data"
        full_sample_dir = sample_dir + "/" + sys.argv[1]
        print(f"Loading {full_sample_dir}...")
        if os.path.exists(full_sample_dir):
            for filename in os.listdir(full_sample_dir):
                if filename.endswith('.csv'):
                    file_path = os.path.join(full_sample_dir, filename)
                    print(f"Loading {filename}...")
                    load_file(cursor, file_path)
        
        print("Data loading completed!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main() 