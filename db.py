import mysql.connector
from mysql.connector import Error

def create_database_connection():
    """Establish a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root1",
            password="Rootuser!123",
            database="sip_database"
        )
        if connection.is_connected():
            print("Successfully connected to the database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def drop_tables(cursor):
    """Drop existing tables if they exist."""
    try:
        cursor.execute("DROP TABLE IF EXISTS sip_transactions")
        cursor.execute("DROP TABLE IF EXISTS portfolio_holdings")
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("DROP TABLE IF EXISTS mutual_funds")
        print("Existing tables dropped successfully")
    except Error as e:
        print(f"Error while dropping tables: {e}")

def create_tables(cursor):
    """Create new tables."""
    try:
        cursor.execute("""
            CREATE TABLE users (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE mutual_funds (
                fund_id INT AUTO_INCREMENT PRIMARY KEY,
                fund_name VARCHAR(300) NOT NULL,
                fund_code VARCHAR(20) UNIQUE NOT NULL,
                category VARCHAR(50),
                current_nav DECIMAL(10, 2),
                last_updated TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE sip_transactions (
                transaction_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                       
                fund_id INT,
                amount DECIMAL(10, 2) NOT NULL,
                transaction_date DATE NOT NULL,
                nav_on_purchase DECIMAL(10, 2),
                units_allotted DECIMAL(10, 4),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (fund_id) REFERENCES mutual_funds(fund_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE portfolio_holdings (
                holding_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                fund_id INT,
                total_units DECIMAL(10, 4) NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (fund_id) REFERENCES mutual_funds(fund_id)
            )
        """)
        print("Tables created successfully")
    except Error as e:
        print(f"Error while creating tables: {e}")

def show_tables(cursor):
    """Display the tables in the database."""
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table)
    except Error as e:
        print(f"Error while showing tables: {e}")

def main():
    connection = create_database_connection()
    if connection is not None:
        cursor = connection.cursor()
        drop_tables(cursor)
        create_tables(cursor)
        show_tables(cursor)
        cursor.close()
        connection.close()
        print("Database connection closed")

if __name__ == "__main__":
    main()

