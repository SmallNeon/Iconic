import sqlite3


def display_file_types(db_path):
    """
    Display file types and their counts from the SQLite database.

    :param db_path: Path to the SQLite database file
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # SQL query to get file types and their counts
        query = """
        SELECT file_type, COUNT(*) AS count
        FROM patient_data
        GROUP BY file_type
        ORDER BY count DESC;
        """

        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()

        # Display the results
        print("File Type | Count")
        print("-" * 25)
        for file_type, count in results:
            print(f"{file_type:<10} | {count}")

    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if conn:
            conn.close()

# Path to the SQLite database
database_path = "/home/molloi-lab-linux2/Desktop/Andrew/Iconic/patient_data.db"

# Call the function
display_file_types(database_path)
