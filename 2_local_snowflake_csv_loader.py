#!/usr/bin/env python3
"""
Simple Data Loader for Crypto Broker Analytics

Loads CSV files to existing Snowflake tables using staging and INFER_SCHEMA.

Update the script with the correct information for your Snowflake environment.

"""

import os
import snowflake.connector

def create_connection():
    """Create and return Snowflake connection"""
    return snowflake.connector.connect( ### Update with your Snowflake environment!!!
        user='YOUR_USER',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT',
        warehouse='YOUR_WAREHOUSE',
        database='CRYPTO_ANALYTICS',
        schema='RAW_DATA'
    )

def upload_file(cursor, file_path):
    """Upload file to Snowflake stage"""
    cursor.execute(f"PUT file://{file_path} @CSV_STAGE")

def load_user_profiles(cursor, filename):
    """Load user profiles"""
    
    cursor.execute(f"""
        COPY INTO USER_PROFILES (
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE,
            INCLUDE_METADATA = (
                ingestdate = METADATA$START_SCAN_TIME, 
                filename = METADATA$FILENAME
            )
        FROM '@CSV_STAGE/{filename}'
        FILE_FORMAT = CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """)
    
    print(f"User profiles loaded from {filename}")

def load_order_book(cursor, filename):
    """Load order book data """
    cursor.execute(f"""
        COPY INTO ORDER_BOOK (
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE,
            INCLUDE_METADATA = (
                ingestdate = METADATA$START_SCAN_TIME, 
                filename = METADATA$FILENAME
            )
        )
        FROM '@CSV_STAGE/{filename}'
        FILE_FORMAT = CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """)
    
    print(f"Order book data loaded from {filename}")

def load_user_trades(cursor, filename):
    """Load user trades """
    cursor.execute(f"""
        COPY INTO USER_TRADES (
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE,
            INCLUDE_METADATA = (
                ingestdate = METADATA$START_SCAN_TIME, 
                filename = METADATA$FILENAME
            )
        )
        FROM '@CSV_STAGE/{filename}'
        FILE_FORMAT = CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """)
    
    print(f"User trades loaded from {filename}")

def load_file(cursor, file_path):
    """Load a CSV file based on its name pattern"""
    filename = os.path.basename(file_path)
    
    # Upload file to stage
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
    """Load all CSV files from sample_data directory"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        # Load sample files
        sample_dir = "sample_data"
        if os.path.exists(sample_dir):
            for filename in os.listdir(sample_dir):
                if filename.endswith('.csv'):
                    file_path = os.path.join(sample_dir, filename)
                    print(f"Loading {filename}...")
                    load_file(cursor, file_path)
        
        print("Data loading completed!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main() 