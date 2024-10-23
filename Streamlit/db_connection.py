import psycopg2
import yaml

class DatabaseConnection:
    def __init__(self, config_file='db_config.yaml'):
        self.config_file = config_file
        self.conn = None
        self.load_db_config()

    def load_db_config(self):
        """
        Load database configuration from a YAML file.
        """
        with open(self.config_file, 'r') as file:
            self.db_params = yaml.safe_load(file)

    def connect(self):
        """
        Establish a connection to the PostgreSQL database.
        """
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print("Connection successful")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            self.conn = None

    def close(self):
        """
        Close the connection to the PostgreSQL database.
        """
        try:
            if self.conn:
                self.conn.close()
                print("Connection closed")
        except Exception as e:
            print(f"An error occurred: {e}")

    def execute_query(self, query, params=None):
        """
        Execute a query on the PostgreSQL database.
        """
        if self.conn:
            try:
                cur = self.conn.cursor()
                cur.execute(query, params)
                result = cur.fetchall()
                cur.close()
                return result
            except Exception as e:
                print(f"An error occurred: {e}")
                return None
        else:
            print("No active database connection.")
            return None

    def execute_update(self, query, params=None):
        """
        Execute an update/insert/delete operation on the PostgreSQL database.
        """
        if self.conn:
            try:
                cur = self.conn.cursor()
                cur.execute(query, params)
                self.conn.commit()
                cur.close()
                print("Operation successful")
            except Exception as e:
                print(f"An error occurred: {e}")
                self.conn.rollback()
        else:
            print("No active database connection.")

    def begin_transaction(self):
        """
        Begin a transaction.
        """
        if self.conn:
            try:
                self.conn.autocommit = False
                print("Transaction started")
            except Exception as e:
                print(f"An error occurred: {e}")

    def commit_transaction(self):
        """
        Commit the current transaction.
        """
        if self.conn:
            try:
                self.conn.commit()
                self.conn.autocommit = True
                print("Transaction committed")
            except Exception as e:
                print(f"An error occurred: {e}")

    def rollback_transaction(self):
        """
        Rollback the current transaction.
        """
        if self.conn:
            try:
                self.conn.rollback()
                self.conn.autocommit = True
                print("Transaction rolled back")
            except Exception as e:
                print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    db = DatabaseConnection()
    db.connect()
    if db.conn:
        result = db.execute_query("SELECT version();")
        if result:
            print(f"Database version: {result[0]}")
        db.close()
