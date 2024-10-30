import yaml
import psycopg2
from psycopg2 import sql
import os

class DatabaseConnection:
    def __init__(self, config_file='db_config.yaml'):
        self.config_file = config_file
        self.conn = None
        self.load_db_config()

    def load_db_config(self):
        if not os.path.isfile(self.config_file):
            raise FileNotFoundError(f"Configuration file {self.config_file} not found.")
        try:
            with open(self.config_file, 'r') as file:
                self.db_config = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise Exception(f"Error parsing YAML file: {e}")

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )
            print("Connection successful")
        except psycopg2.Error as e:
            raise Exception(f"Error connecting to the database: {e}")

    def execute_query(self, query, params=None):
        if self.conn is None:
            raise Exception("Database connection is not established.")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql.SQL(query), params)
                if query.strip().lower().startswith("select"):
                    return cursor.fetchall()
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Error executing query: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            print("Connection closed")

# Example usage:
if __name__ == "__main__":
    # Provide the full path to the db_config.yaml file
    config_path = 'c:/Users/Lennart/Desktop/TBDA/Streamlit/db_config.yaml'
    db = DatabaseConnection(config_file=config_path)
    db.connect()
    try:
        result = db.execute_query("SELECT version();")
        print(f"Database version: {result}")
    finally:
        db.close()