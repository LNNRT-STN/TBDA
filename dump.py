import subprocess

def export_schema():
    db_params = {
        'dbname': '1245',
        'user': 'lectura',
        'password': 'ncorrea#2022',
        'host': '138.100.82.184',
        'port': '5432'
    }

    # Construct the pg_dump command
    pg_dump_command = [
        "pg_dump",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        f"--dbname=postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    ]

    try:
        # Run the pg_dump command and capture the output
        result = subprocess.run(pg_dump_command, capture_output=True, text=True, check=True)
        schema = result.stdout
        return schema
    except subprocess.CalledProcessError as e:
        print(f"Error exporting schema: {e}")
        return None

if __name__ == "__main__":
    schema = export_schema()
    if schema:
        with open("schema_export.sql", "w") as file:
            file.write(schema)
        print("Schema exported successfully to schema_export.sql")