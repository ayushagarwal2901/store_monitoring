
import mysql.connector

# Database connection configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = ''
DB_NAME = 'restaurant_monitoring'

# Create a connection to the database
db_connection = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    allow_local_infile=True
)

# Create a cursor to execute SQL queries
db_cursor = db_connection.cursor()

# Define the schema for the 'store_status' table
CREATE_STORE_STATUS_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS store_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    status VARCHAR(10) NOT NULL,
    timestamp_utc DATETIME NOT NULL,
    UNIQUE (id)
) ENGINE=InnoDB;
'''

# Define the schema for the 'business_hours' table
CREATE_BUSINESS_HOURS_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS business_hours (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    day INT NOT NULL,
    start_time_local TIME NOT NULL,
    end_time_local TIME NOT NULL,
    UNIQUE (id)
) ENGINE=InnoDB;
'''

CREATE_UPDATED_BUSINESS_HOURS_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS updated_business_hours (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    day INT NOT NULL,
    start_time_local TIME NOT NULL,
    end_time_local TIME NOT NULL,
    timezone_str VARCHAR(50) NOT NULL,
    start_time_utc TIME NOT NULL,
    end_time_utc TIME NOT NULL,
    UNIQUE (id)
) ENGINE=InnoDB;
'''

# Define the schema for the 'store_timezone' table
CREATE_STORE_TIMEZONE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS store_timezone (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    timezone_str VARCHAR(50) NOT NULL,
    UNIQUE (id)
) ENGINE=InnoDB;
'''

CREATE_UPDATED_STORE_TIMEZONE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS updated_store_timezone (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    timezone_str VARCHAR(50) NOT NULL,
    UNIQUE (id)
) ENGINE=InnoDB;
'''

# Function to create all tables in the database
def create_tables():
    db_cursor.execute(CREATE_STORE_STATUS_TABLE_QUERY)
    db_cursor.execute(CREATE_BUSINESS_HOURS_TABLE_QUERY)
    db_cursor.execute(CREATE_STORE_TIMEZONE_TABLE_QUERY)
    db_cursor.execute(CREATE_UPDATED_STORE_TIMEZONE_TABLE_QUERY)
    db_cursor.execute(CREATE_UPDATED_BUSINESS_HOURS_TABLE_QUERY)
    db_connection.commit()

def drop_tables():
    # Drop existing tables
    db_cursor.execute("DROP TABLE IF EXISTS store_status;")
    db_cursor.execute("DROP TABLE IF EXISTS business_hours;")
    db_cursor.execute("DROP TABLE IF EXISTS store_timezone;")
    db_cursor.execute("DROP TABLE IF EXISTS updated_store_timezone;")
    db_cursor.execute("DROP TABLE IF EXISTS updated_business_hours;")
    db_connection.commit()

