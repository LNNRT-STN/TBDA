import db_connection

def export_schema():
    conn = db_connection.connect()
    if conn is None:
        print("Failed to connect to the database.")
        return

    cursor = conn.cursor()
    schema = {}

    # Get tables
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        schema[table_name] = {}

        # Get columns
        cursor.execute(f"SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name='{table_name}'")
        columns = cursor.fetchall()
        schema[table_name]['columns'] = columns

        # Get indexes
        cursor.execute(f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{table_name}'
        """)
        indexes = cursor.fetchall()
        schema[table_name]['indexes'] = indexes

        # Get constraints
        cursor.execute(f"""
            SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
            FROM pg_catalog.pg_constraint r
            WHERE r.conrelid = (
                SELECT oid FROM pg_catalog.pg_class WHERE relname = '{table_name}'
            )
        """)
        constraints = cursor.fetchall()
        schema[table_name]['constraints'] = constraints

    # Close the connection
    cursor.close()
    conn.close()
    return schema

if __name__ == "__main__":
    schema = export_schema()
    if schema:
        for table, details in schema.items():
            print(f"Table: {table}")
            print("  Columns:")
            for column in details['columns']:
                print(f"    {column}")
            print("  Indexes:")
            for index in details['indexes']:
                print(f"    {index}")
            print("  Constraints:")
            for constraint in details['constraints']:
                print(f"    {constraint}")