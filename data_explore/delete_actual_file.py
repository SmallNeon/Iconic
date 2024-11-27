import sqlite3

def clear_actual_file_types(db_path):
    """
    Clear the contents of the actual_file_type column in the database.

    :param db_path: Path to the SQLite database file
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # SQL query to clear actual_file_type column
        query = "UPDATE patient_data SET actual_file_type = NULL;"
        cursor.execute(query)

        # Commit changes
        conn.commit()
        print("Cleared all contents in the 'actual_file_type' column.")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# Path to the SQLite database
database_path = "/home/molloi-lab-linux2/Desktop/Andrew/Iconic/patient_data.db"

# Call the function
clear_actual_file_types(database_path)
