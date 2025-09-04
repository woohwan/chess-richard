import sqlite3
import pandas as pd
from pathlib import Path
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_database_description(db_path: str):
    """
    Connects to a SQLite database and generates a 'database_description' directory
    containing a CSV file for each table's schema.

    Args:
        db_path (str): The path to the SQLite database file (e.g., 'chinook.db').
    """
    db_file = Path(db_path)
    if not db_file.exists():
        logging.error(f"Database file does not exist: {db_path}")
        return

    db_directory_path = db_file.parent
    description_path = db_directory_path / "database_description"

    # Create the description directory if it doesn't exist
    if not description_path.exists():
        description_path.mkdir(exist_ok=True)
        logging.info(f"Created directory: {description_path}")
    else:
        # Clear existing description files
        for f in description_path.glob("*.csv"):
            os.remove(f)
        logging.info(f"Cleared existing files in {description_path}")
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        logging.info(f"Found tables: {tables}")

        for table_name in tables:
            logging.info(f"Processing table: {table_name}")
            
            # Get column information for the current table
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns_info = cursor.fetchall()
            
            # Get foreign key information
            cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            fk_info = cursor.fetchall()
            
            # Create a dictionary to hold foreign key details
            fk_map = {}
            for fk in fk_info:
                from_column = fk[3]
                to_table = fk[2]
                to_column = fk[4]
                fk_map[from_column] = f"Foreign key referencing '{to_table}' table on column '{to_column}'."

            # Prepare data for DataFrame
            data = []
            for col_info in columns_info:
                cid, original_column_name, data_type, not_null, default_value, is_pk = col_info
                
                # Default descriptions for columns
                column_description = ""
                value_description = ""
                
                # Check for primary key
                if is_pk:
                    column_description = "Primary key."
                
                # Check for foreign key
                if original_column_name in fk_map:
                    column_description += " " + fk_map[original_column_name]
                
                data.append({
                    "original_column_name": original_column_name,
                    "column_name": original_column_name,  # Simplified, can be more descriptive
                    "column_description": column_description.strip(),
                    "data_format": data_type,
                    "value_description": value_description
                })

            df = pd.DataFrame(data)
            output_csv_path = description_path / f"{table_name.lower()}.csv"
            df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            logging.info(f"Successfully generated {output_csv_path}")

    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # You need to specify the path to your database file here
    # Assuming your db file is at data/dev/dev_databases/chinook/chinook.db
    db_root_dir = os.getenv("DB_ROOT_DIRECTORY", "./data/dev/dev_databases")
    db_id = "chinook" # Or any other db_id
    db_file_path = Path(db_root_dir) / db_id / f"{db_id}.sqlite" # Assumes .sqlite extension
    
    # Check for other common extensions if .sqlite does not exist
    if not db_file_path.exists():
        db_file_path = Path(db_root_dir) / db_id / f"{db_id}.db"
    
    logging.info(f"Attempting to generate descriptions for: {db_file_path}")
    create_database_description(str(db_file_path))