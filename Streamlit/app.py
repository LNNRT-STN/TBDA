import streamlit as st
import pandas as pd
from db_connection import DatabaseConnection

# Title for the app
st.title("Real-Time Machine Operation Periods Visualization")

# Establish a database connection
config_path = 'c:/Users/Lennart/Desktop/TBDA/Streamlit/db_config.yaml'
db = DatabaseConnection(config_file=config_path)
db.connect()

if db.conn:
    try:
        # Query for operation-related variables
        operation_var_query = """
        SELECT id, name FROM variable 
        WHERE name LIKE '%operation%' OR name LIKE '%status%' OR name LIKE '%machine%'
        """
        operation_vars = db.execute_query(operation_var_query)
        operation_ids = [var[0] for var in operation_vars]
        
        # Check if we have operation-related IDs
        if operation_ids:
            # Format IDs for the SQL query
            id_list = ', '.join(map(str, operation_ids))
            
            # Query for operation periods in `variable_log_float` where value indicates "running"
            query_float = f"""
            SELECT date, value, id_var FROM variable_log_float
            WHERE id_var IN ({id_list}) AND value = 1
            """
            operation_data = db.execute_query(query_float)
            
            if operation_data:
                # Convert the query result into a DataFrame
                df = pd.DataFrame(operation_data, columns=['timestamp', 'value', 'variable_id'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                
                # Display raw data for verification
                st.write("Machine Operation Data:")
                st.write(df)
                
                # Visualize the data
                st.write("Machine Operation Status Over Time:")
                df['status'] = df['value'].apply(lambda x: 'Running' if x == 1 else 'Stopped')
                df.set_index('timestamp', inplace=True)
                st.line_chart(df['value'])

            else:
                st.warning("No operational data found for the specified variables.")
        else:
            st.warning("No operation-related variables found in the database.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        db.close()
else:
    st.error("Failed to connect to the database.")
