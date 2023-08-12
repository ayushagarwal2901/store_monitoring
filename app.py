# app.py
import pandas as pd
from model import db_connection, db_cursor, create_tables, drop_tables
import os
from flask import Flask, jsonify, request, render_template, Response
import uuid
from datetime import datetime, timedelta, time
import pytz
import threading
import time as tm
from uptime_downtime import (uptime_downtime_last_hour, uptime_downtime_last_day, uptime_downtime_last_week)

app = Flask(__name__)


# Get the absolute path of the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

#Drop all prestored tables
drop_tables()

# Create the necessary tables in the database
create_tables()

# Function to read and store data from store_status.csv using LOAD DATA INFILE
def store_store_status():
    csv_file_path = os.path.join(current_dir, 'store_status.csv')
    query = """
    LOAD DATA INFILE %s
    INTO TABLE store_status
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (store_id, status, timestamp_utc)
    """
    db_cursor.execute(query, (csv_file_path,))
    db_connection.commit()

# Function to read and store data from business_hours.csv using LOAD DATA INFILE
def store_business_hours():
    csv_file_path = os.path.join(current_dir, 'business_hours.csv')
    query = """
    LOAD DATA INFILE %s
    INTO TABLE business_hours
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (store_id, day, start_time_local, end_time_local)
    """
    db_cursor.execute(query, (csv_file_path,))
    db_connection.commit()

# Function to read and store data from store_timezone.csv using LOAD DATA INFILE
def store_store_timezone():
    csv_file_path = os.path.join(current_dir, 'store_timezone.csv')
    query = """
    LOAD DATA INFILE %s
    INTO TABLE store_timezone
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (store_id, timezone_str)
    """
    db_cursor.execute(query, (csv_file_path,))
    db_connection.commit()

# Function to read and store data from updated_business_hours.csv using LOAD DATA INFILE
def store_updated_business_hours():
    csv_file_path = os.path.join(current_dir, 'updated_business_hours.csv')
    query = """
    LOAD DATA INFILE %s
    INTO TABLE updated_business_hours
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (store_id, day, start_time_local, end_time_local, timezone_str, start_time_utc, end_time_utc)
    """
    db_cursor.execute(query, (csv_file_path,))
    db_connection.commit()

# Function to read and store data from updated_store_timezone.csv using LOAD DATA INFILE
def store_updated_store_timezone():
    csv_file_path = os.path.join(current_dir, 'updated_store_timezone.csv')
    query = """
    LOAD DATA INFILE %s
    INTO TABLE updated_store_timezone
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (store_id, timezone_str)
    """
    db_cursor.execute(query, (csv_file_path,))
    db_connection.commit()

# Call the functions to read and store data from the CSV files
store_store_status()
store_business_hours()
store_store_timezone()

# Create a Lock object for thread-safe access to report_status
report_status_lock = threading.Lock()

# Create a dictionary to store status of report generation
report_status = {}

# List to store generated report IDs
stored_report_ids = []  

def generate_report_id():
    # Generate a random report_id
    report_id = str(uuid.uuid4())

    # Store the generated report_id in the list
    stored_report_ids.append(report_id)

    return report_id

# Function to convert start_time_local and end_time_local to UTC times
def convert_to_utc(df):
    timezone_str = pytz.timezone(df['timezone_str'].iloc[0])

    # Convert start_time_local and end_time_local to UTC
    df['start_time_utc'] = pd.to_datetime(df['start_time_local'], format='%H:%M:%S').dt.tz_localize(timezone_str).dt.tz_convert(pytz.UTC).dt.time

    df['end_time_utc'] = pd.to_datetime(df['end_time_local'], format='%H:%M:%S').dt.tz_localize(timezone_str).dt.tz_convert(pytz.UTC).dt.time

    return df

def generate_report(report_id):

    # Fetch data from the 'store_timezone' table directly into a DataFrame
    df_store_timezone = pd.read_sql_query("SELECT store_id, timezone_str FROM store_timezone", db_connection)

    # Filter the data to keep only the store_ids present in store_status
    df_store_status = pd.read_sql_query("SELECT DISTINCT store_id FROM store_status", db_connection)
    store_status_ids = set(df_store_status['store_id'].unique())
    df_updated_store_timezone = df_store_timezone[df_store_timezone['store_id'].isin(store_status_ids)]

    # Get unique store_id values from updated_business_hours
    updated_store_timezone_store_ids = set(df_updated_store_timezone['store_id'].unique())

    # Find the store_ids that are present in store_status but not in updated_store_timezone
    missing_store_ids = list(store_status_ids - updated_store_timezone_store_ids)
    missing_store_data = [{'store_id': store_id, 'timezone_str': 'America/Chicago'} for store_id in missing_store_ids]
    df_missing_store_timezone = pd.DataFrame(missing_store_data)

    # Append missing store data to updated_business_hours
    df_updated_store_timezone = pd.concat([df_updated_store_timezone, df_missing_store_timezone], ignore_index=True)

    # Store updated_store_timezone data in a csv file
    df_updated_store_timezone.to_csv('updated_store_timezone.csv', index=False)

    # Call the functions to read and store data from the CSV files
    store_updated_store_timezone()

    # Fetch data from the 'business_hours' table directly into a DataFrame
    df_business_hours = pd.read_sql_query("SELECT store_id, day, start_time_local, end_time_local FROM business_hours", db_connection)

    # Filter the data to keep only the store_ids present in store_status
    df_store_status = pd.read_sql_query("SELECT DISTINCT store_id FROM store_status", db_connection)
    store_status_ids = set(df_store_status['store_id'].unique())
    df_updated_business_hours = df_business_hours[df_business_hours['store_id'].isin(store_status_ids)]

    # Convert timedelta columns to strings in the desired format
    df_updated_business_hours['start_time_local'] = df_updated_business_hours['start_time_local'].astype(str).str.slice(7, -1)
    df_updated_business_hours['end_time_local'] = df_updated_business_hours['end_time_local'].astype(str).str.slice(7, -1)

    # Convert start_time_local and end_time_local columns back to time data type
    df_updated_business_hours['start_time_local'] = pd.to_datetime(df_updated_business_hours['start_time_local']).dt.strftime('%H:%M:%S')
    df_updated_business_hours['end_time_local'] = pd.to_datetime(df_updated_business_hours['end_time_local']).dt.strftime('%H:%M:%S')

    # Get unique store_id values from updated_business_hours
    updated_business_hours_store_ids = set(df_updated_business_hours['store_id'].unique())

    # Find the store_ids that are present in store_status but not in updated_business_hours
    missing_store_ids = list(store_status_ids - updated_business_hours_store_ids)

    # Prepare data for missing store_ids with open 24*7
    missing_store_data = [{'store_id': store_id, 'day': i, 'start_time_local': '00:00:00', 'end_time_local': '23:59:59'} for store_id in missing_store_ids for i in range(7)]
    df_missing_business_hours = pd.DataFrame(missing_store_data)

    # Append missing store data to updated_business_hours
    df_updated_business_hours = pd.concat([df_updated_business_hours, df_missing_business_hours], ignore_index=True)

    # Merge updated_business_hours_df with updated_store_timezone_df based on 'store_id'
    df_updated_business_hours = df_updated_business_hours.merge(df_updated_store_timezone[['store_id', 'timezone_str']], on='store_id', how='left')

    # Sort the updated_business_hours data first by store_id and then by day
    df_updated_business_hours = df_updated_business_hours.sort_values(by=['store_id', 'day'])

    # Call the convert_to_utc function to convert the columns
    df_updated_business_hours = convert_to_utc(df_updated_business_hours)

     # Store updated_business_hours data in a csv file
    df_updated_business_hours.to_csv('updated_business_hours.csv', index=False)

    # Call the functions to read and store data from the CSV files
    store_updated_business_hours()

    os.remove('updated_business_hours.csv')
    os.remove('updated_store_timezone.csv')

    # Read data from store_status table in mysql database
    df_store_status = pd.read_sql_query("SELECT store_id, status, timestamp_utc FROM store_status", db_connection)

    # Call function to store valid data i.e. removing data not in business hours
    valid_store_data(df_store_status, df_updated_business_hours)

    # Mark the report as completed
    with report_status_lock:
        report_status[report_id] = 'completed'
       
    print("Report Generation Completed🎉🎉")




# Function to store valid data i.e. removing data not in business hours
def valid_store_data(df_store_status, df_updated_business_hours):

    # Filter relevant columns from df_store_status
    df_store_status = df_store_status[['store_id', 'status', 'timestamp_utc']]

    # Filter relevant columns from df_updated_business_hours
    df_updated_business_hours = df_updated_business_hours[['store_id', 'day', 'start_time_utc', 'end_time_utc']]

    # Set index for faster merging and grouping
    df_updated_business_hours.set_index(['store_id', 'day'])
    df_store_status.set_index('store_id')

    # Initialize the report data dictionary
    report_data = {
        'store_id': [],
        'uptime_last_hour (in minutes)': [],
        'uptime_last_day (in hours)': [],
        'uptime_last_week (in hours)': [],
        'downtime_last_hour (in minutes)': [],
        'downtime_last_day (in hours)': [],
        'downtime_last_week (in hours)': []
    }

    # Create a dictionary to store precomputed business hours for each store and day
    business_hours_dict = {}

    # Loop through each group and store precomputed business hours in the dictionary
    for index, row in df_updated_business_hours.iterrows():
        store_id = row['store_id']
        day = row['day']
        start_time_utc = row['start_time_utc']
        end_time_utc = row['end_time_utc']
        # Get the existing business hours for this combination
        existing_hours = business_hours_dict.get((store_id, day), [])
        
        # Add the current business hours to the existing list
        existing_hours.append((start_time_utc, end_time_utc))
        
        # Store the updated list in the dictionary
        business_hours_dict[(store_id, day)] = existing_hours


    # Get the current timestamp (max timestamp from store_status data)
    current_timestamp = df_store_status['timestamp_utc'].max()
    current_timestamp = pd.to_datetime(current_timestamp)

    # Initialize an empty dataframe
    valid_data_df = pd.DataFrame()

    # Loop through each store_id and day using nested groupby
    for (store_id, day), group in df_store_status.groupby([df_store_status['store_id'], df_store_status['timestamp_utc'].dt.dayofweek]):

        # Filter data for that particular store_id
        group = group[group['store_id'] == store_id]

        # Get the corresponding business hours for the current store_id and day
        business_hours_list = business_hours_dict.get((store_id, day))
    
        # If there are no business hours for the current day, skip
        if not business_hours_list:
            continue
 
        # interval = 0 

        # Loop through each set of business hours for the current day
        for start_time_utc, end_time_utc in business_hours_list:

            # Filter the observations within business hours for set of business hours
            if start_time_utc <= end_time_utc:
                filtered_group = group[(group['timestamp_utc'].dt.time >= start_time_utc) & (group['timestamp_utc'].dt.time <= end_time_utc)]
                # Calculate the time interval for the current day
                # interval += abs((datetime.combine(datetime.today(), end_time_utc) - datetime.combine(datetime.today(), start_time_utc)).total_seconds() / 3600)
            else:
                filtered_group = group[((group['timestamp_utc'].dt.time >= start_time_utc) & (group['timestamp_utc'].dt.time <= time(23, 59, 59))) | ((group['timestamp_utc'].dt.time >= time(0, 0, 0)) & (group['timestamp_utc'].dt.time <= end_time_utc))]
                # Calculate the time interval for the current day
                # interval += abs(
                #     (
                #         (datetime.combine(datetime.today(), end_time_utc) - datetime.combine(datetime.today(), time(0, 0, 0)))
                #         + (datetime.combine(datetime.today(), time(23, 59, 59)) - datetime.combine(datetime.today(), start_time_utc))
                #     ).total_seconds() / 3600
                # )
        # Store current day in filtered_group 'day' column
        filtered_group['day'] = day
        # filtered_group['interval'] = interval

        # Append filtered_data to the valid_data DataFrame
        valid_data_df = pd.concat([valid_data_df, filtered_group], ignore_index=True)
 
    # Sort the final DataFrame by store_id and timestamp_utc
    valid_data_df = valid_data_df.sort_values(by=['store_id', 'timestamp_utc'])
  
    # Iterate over each unique store_id in the day_observations_df
    unique_store_ids = valid_data_df['store_id'].unique()
    
    # Convert timestamp_utc to datetime
    valid_data_df["timestamp_utc"] = pd.to_datetime(valid_data_df["timestamp_utc"])

    uptime_downtime_last_hour(valid_data_df, current_timestamp, unique_store_ids, report_data)
    uptime_downtime_last_day(valid_data_df, current_timestamp, unique_store_ids, report_data)
    uptime_downtime_last_week(valid_data_df, current_timestamp, unique_store_ids, report_data)

    # Convert the report data to a DataFrame
    report_df = pd.DataFrame(report_data)
 
    # Save the report DataFrame to a CSV file
    report_df.to_csv('static/report.csv', index=False)



@app.route('/trigger_report', methods=['GET']) 
def trigger_report():
    # Generate a random report ID
    report_id = generate_report_id()

    # Start the report generation process asynchronously in the background
    thread = threading.Thread(target=generate_report, args=(report_id,))
    thread.start()

    # Return the report_id to the user
    return report_id

@app.route('/get_report', methods=['GET', 'POST'])
def get_report():
    if request.method == 'POST':
        # Get the user-provided report_id from the form
        user_report_id = request.form['report_id']

        # Check if the user_report_id matches any of the stored report IDs
        if user_report_id not in stored_report_ids:
            return 'Invalid report_id'
        
        # Check if the report is completed
        if report_status.get(user_report_id) == 'completed':
            # Get the URL for the CSV file
            csv_url = '/static/report.csv'
            status = 'completed'
            # Render the HTML template with the appropriate context
            return render_template('download.html', status=status, csv_url=csv_url)
        else:
            # Report is still running
            return 'Running'
        
    else:
        # Render the index.html template with the form
        return render_template('index.html')
    

if __name__ == '__main__':
    app.run(debug=True)

# Close the database connection
db_cursor.close()
db_connection.close()
