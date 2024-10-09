import psycopg2

# Database connection parameters
db_params = {
    'dbname': '1245',
    'user': 'lectura',
    'password': 'ncorrea#2022',
    'host': '138.100.82.184',
    'port': '5432'
}

try:
    # Establish the connection
    conn = psycopg2.connect(**db_params)
    print("Connection successful")

    # Create a cursor object
    cursor = conn.cursor()

    # Execute a simple query
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"Database version: {db_version}")

    # Close the cursor and connection
    cursor.close()
    conn.close()
    print("Connection closed")

except Exception as e:
    print(f"An error occurred: {e}")