import mysql.connector
import pandas as pd
# Replace these values with your own database credentials
db_config = {
    'host': 'database-analytics.c3y0yk8mqy0g.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'admin1234',
    'database': 'mydb'
}

def conn_closed(connection):
    # Close the connection in the finally block to ensure it's always closed
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print('Connection closed')

def create_table():
    try:
        # Establish a connection to the database
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor=connection.cursor()
            cursor.execute("""CREATE TABLE artist (
        artist_id INT PRIMARY KEY,
        name VARCHAR(50),
        popularity INT,
        genere VARCHAR(50),
        fetched_on DATE,
        followers INT);""")
            print('Query executed...& table creted')

            # Perform database operations here

    except mysql.connector.Error as e:
        print(f'Error connecting to MySQL database: {e}')

    finally:
        conn_closed(connection)

# def insert_records(value1, value2, value3,value4, value5, value6):
def insert_records():
    try:
        # Establish a connection to the database
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor=connection.cursor()
            cursor.execute("""INSERT INTO mydb.artist VALUES (3, 'test', 80, 'Pop', '2024-01-06', 500000);
                            """)
            connection.commit()
#             INSERT INTO your_table_name (column1, column2, column3)
# VALUES ('value1', 'value2', 'value3')
            print('records inserted...')

            # Perform database operations here

    except mysql.connector.Error as e:
        print(f'Error connecting to MySQL database: {e}')

    finally:
        conn_closed(connection)

# insert_records()
def query_executor():
    try:
        # Establish a connection to the database
        connection = mysql.connector.connect(**db_config)
        query = "SELECT * FROM mydb.artist"
        df = pd.read_sql_query(query, connection)
        print(df)

    except mysql.connector.Error as e:
        print(f'Error connecting to MySQL database: {e}')

    finally:
        conn_closed(connection)

insert_records()
query_executor()