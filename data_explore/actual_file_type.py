import os
import sqlite3
import mimetypes
from tqdm import tqdm

def add_column_if_not_exists(db_path, table_name, column_name, column_type):
    """
    Add a new column to the table if it does not already exist.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if column exists
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        if column_name not in columns:
            # Add the column
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")
            print(f"Column '{column_name}' added to table '{table_name}'.")
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error adding column: {e}")
    finally:
        if conn:
            conn.close()

def detect_file_type(file_path):
    """
    Detect the actual file type using mimetypes or magic.
    """
    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type:
        return mime_type
    # Fallback: If mimetypes fails, use file extension or manual inspection
    return "unknown"

def update_actual_file_types(db_path):
    """
    Update the actual file types in the database and display progress.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch all records where actual_file_type is NULL
        cursor.execute("SELECT id, file_path FROM patient_data WHERE actual_file_type IS NULL OR actual_file_type = '';")
        records = cursor.fetchall()

        total_files = len(records)
        print(f"Total files to process: {total_files}")

        # Progress bar
        for record in tqdm(records, desc="Processing files", unit="file"):
            record_id, file_path = record
            if os.path.exists(file_path):
                file_type = detect_file_type(file_path)
                # Update the database with the detected file type
                cursor.execute("UPDATE patient_data SET actual_file_type = ? WHERE id = ?", (file_type, record_id))
            else:
                print(f"File not found: {file_path}")

        conn.commit()
        print("File types updated successfully.")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# Main script

# Path to the SQLite database
database_path = "/home/molloi-lab-linux2/Desktop/Andrew/Iconic/patient_data.db"

# Ensure the column exists
add_column_if_not_exists(database_path, "patient_data", "actual_file_type", "TEXT")

# Update actual file types
update_actual_file_types(database_path)
