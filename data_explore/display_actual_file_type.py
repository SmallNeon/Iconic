import sqlite3

def count_actual_file_types(db_path):
    """
    Count the number of each actual file type in the SQLite database.

    :param db_path: Path to the SQLite database file
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # SQL query to count each actual file type
        query = """
        SELECT actual_file_type, COUNT(*) AS count
        FROM patient_data
        GROUP BY actual_file_type
        ORDER BY count DESC;
        """

        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()

        # Display the results
        print("Actual File Type | Count")
        print("-" * 40)
        for file_type, count in results:
            print(f"{file_type:<20} | {count}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# Path to the SQLite database
database_path = "/home/molloi-lab-linux2/Desktop/Andrew/Iconic/patient_data.db"

# Call the function
count_actual_file_types(database_path)
