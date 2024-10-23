import pandas as pd
from db_connection import DatabaseConnection

def explore_database():
    db = DatabaseConnection()
    db.connect()
    if db.conn:
        try:
            # Fetch all table names
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
            tables = db.execute_query(tables_query)
            if tables:
                print("Tables in the database:")
                for table in tables:
                    print(f" - {table[0]}")

            # Fetch column details for each table
            for table in tables:
                columns_query = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '{table[0]}'
                """
                columns = db.execute_query(columns_query)
                if columns:
                    print(f"\nColumns in table {table[0]}:")
                    for column in columns:
                        print(f" - {column[0]}: {column[1]}, Nullable: {column[2]}, Default: {column[3]}")

            # Fetch row count for each table
            for table in tables:
                count_query = f"SELECT COUNT(*) FROM {table[0]}"
                count = db.execute_query(count_query)
                if count:
                    print(f"\nRow count for table {table[0]}: {count[0][0]}")

            # Fetch first few rows for each table
            for table in tables:
                first_entries_query = f"SELECT * FROM {table[0]} LIMIT 6"
                first_entries = db.execute_query(first_entries_query)
                if first_entries:
                    print(f"\nFirst entries in table {table[0]}:")
                    for entry in first_entries:
                        print(entry)

            # Identify operation periods of the machine
            operation_var_query = """
            SELECT * FROM variable WHERE name LIKE '%operation%' OR name LIKE '%status%' OR name LIKE '%machine%'
            """
            operation_vars = db.execute_query(operation_var_query)
            if operation_vars:
                print("\nOperation-related variables:")
                for var in operation_vars:
                    print(f" - {var}")

            # Determine when the machine is working
            for var in operation_vars:
                id_var = var[0]
                if var[2] == 'float':
                    working_query = f"SELECT date, value FROM variable_log_float WHERE id_var = {id_var} AND value = 1"
                else:
                    working_query = f"SELECT date, value FROM variable_log_string WHERE id_var = {id_var} AND value = 'RUNNING'"
                working_periods = db.execute_query(working_query)
                if working_periods:
                    print(f"\nWorking periods for variable {var[1]}:")
                    for period in working_periods:
                        print(period)

            # Determine energy demands per program name
            energy_var_query = """
            SELECT * FROM variable WHERE name LIKE '%energy%' OR name LIKE '%power%'
            """
            energy_vars = db.execute_query(energy_var_query)
            if energy_vars:
                print("\nEnergy-related variables:")
                for var in energy_vars:
                    print(f" - {var}")

            program_var_query = """
            SELECT * FROM variable WHERE name LIKE '%program%'
            """
            program_vars = db.execute_query(program_var_query)
            if program_vars:
                print("\nProgram-related variables:")
                for var in program_vars:
                    print(f" - {var}")

            for energy_var in energy_vars:
                for program_var in program_vars:
                    energy_id_var = energy_var[0]
                    program_id_var = program_var[0]
                    energy_program_query = f"""
                    SELECT vls.date, vls.value AS program_name, vlf.value AS energy_consumption
                    FROM variable_log_string vls
                    JOIN variable_log_float vlf ON vls.date = vlf.date
                    WHERE vls.id_var = {program_id_var} AND vlf.id_var = {energy_id_var}
                    """
                    energy_program_data = db.execute_query(energy_program_query)
                    if energy_program_data:
                        print(f"\nEnergy consumption per program for variables {program_var[1]} and {energy_var[1]}:")
                        for data in energy_program_data:
                            print(data)

            # Identify and contextualize alerts
            alert_var_query = """
            SELECT * FROM variable WHERE name LIKE '%alert%' OR name LIKE '%error%' OR name LIKE '%warning%'
            """
            alert_vars = db.execute_query(alert_var_query)
            if alert_vars:
                print("\nAlert-related variables:")
                for var in alert_vars:
                    print(f" - {var}")

            for alert_var in alert_vars:
                id_var = alert_var[0]
                if alert_var[2] == 'float':
                    alert_query = f"SELECT date, value FROM variable_log_float WHERE id_var = {id_var}"
                else:
                    alert_query = f"SELECT date, value FROM variable_log_string WHERE id_var = {id_var}"
                alerts = db.execute_query(alert_query)
                if alerts:
                    print(f"\nAlerts for variable {alert_var[1]}:")
                    for alert in alerts:
                        print(alert)

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            db.close()

def export_first_20_rows_to_csv():
    db = DatabaseConnection()
    db.connect()
    if db.conn:
        try:
            # Fetch all table names
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
            tables = db.execute_query(tables_query)
            if tables:
                for table in tables:
                    table_name = table[0]
                    first_20_rows_query = f"SELECT * FROM {table_name} LIMIT 20"
                    first_20_rows = db.execute_query(first_20_rows_query)
                    if first_20_rows:
                        df = pd.DataFrame(first_20_rows)
                        df.to_csv(f"{table_name}_first_20_rows.csv", index=False)
                        print(f"Exported first 20 rows of table {table_name} to {table_name}_first_20_rows.csv")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            db.close()

def export_variable_table_to_csv():
    db = DatabaseConnection()
    db.connect()
    if db.conn:
        try:
            variable_query = "SELECT * FROM variable"
            variable_data = db.execute_query(variable_query)
            if variable_data:
                df = pd.DataFrame(variable_data)
                df.to_csv("variable_table.csv", index=False)
                print("Exported variable table to variable_table.csv")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            db.close()

if __name__ == "__main__":
    explore_database()
    export_first_20_rows_to_csv()
    export_variable_table_to_csv()