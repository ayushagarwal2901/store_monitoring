import pandas as pd
from datetime import timedelta

# Function to calculate uptime and downtime for last hour
def uptime_downtime_last_hour(valid_data_df, current_timestamp, unique_store_ids, report_data):

    # Initialize an empty DataFrame to store filtered data
    combined_filtered_data = pd.DataFrame()

    # Initialize a list to store filtered data
    filtered_data_list = []

    for store_id in unique_store_ids:

        # Filter valid_data_df for the current store_id
        store_data = valid_data_df[valid_data_df['store_id'] == store_id]

        # Calculate the timestamp for one hour ago
        one_hour_ago = current_timestamp - timedelta(hours=1)
        one_hour_ago = pd.to_datetime(one_hour_ago)

        # Filter data for the specified time interval
        filtered_data = store_data.loc[
            (store_data["timestamp_utc"].between(one_hour_ago, current_timestamp))
        ]

        # Include previous row if filtered_data is empty or first timestamp is greater than one_hour_ago
        if filtered_data.empty or filtered_data.iloc[0]["timestamp_utc"] > one_hour_ago:
            prev_row = store_data[store_data["timestamp_utc"] < one_hour_ago].tail(1)
            filtered_data = pd.concat([prev_row, filtered_data], ignore_index=True)

        # Append filtered_group to the list
        filtered_data_list.append(filtered_data)
    
    # Concatenate all filtered dataframes into a single DataFrame
    combined_filtered_data = pd.concat(filtered_data_list, ignore_index=True)
    

    # Loop through unique store_ids
    unique_store_ids = valid_data_df['store_id'].unique()

    for store_id in unique_store_ids:

        # Filter combined_filtered_data for current store_id
        store_filtered_data = combined_filtered_data[combined_filtered_data['store_id'] == store_id]

        # Initialize variables for uptime and downtime
        uptime = timedelta()
        downtime = timedelta()
 
        # Calculate uptime and downtime based on the states and durations
        for i in range(len(store_filtered_data)):
            if(len(store_filtered_data) == 1):
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime = current_timestamp - one_hour_ago
                else:
                    downtime = current_timestamp - one_hour_ago
            else:
                if(i != len(store_filtered_data) - 1):
                    if(i==0 and store_filtered_data['timestamp_utc'].iloc[i] < one_hour_ago):
                        start_time = one_hour_ago
                        
                    else:
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                    end_time = store_filtered_data['timestamp_utc'].iloc[i+1]
                    duration = end_time - start_time
                    
                else:
                    # Calculate uptime and downtime for the last state
                    if(store_filtered_data['timestamp_utc'].iloc[i] < current_timestamp):
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                        end_time = current_timestamp
                        duration = end_time - start_time
                    else:
                        break
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime += duration
                else:
                    downtime += duration

        # Convert uptime and downtime to minutes
        uptime_last_hour = round(uptime.total_seconds() / 60, 2)
        downtime_last_hour = round(downtime.total_seconds() / 60, 2)

        # Append the data to report_data dictionary
        report_data['store_id'].append(store_id)
        report_data['uptime_last_hour (in minutes)'].append(uptime_last_hour)
        report_data['downtime_last_hour (in minutes)'].append(downtime_last_hour)


# Function to calculate uptime and downtime for last day
def uptime_downtime_last_day(valid_data_df, current_timestamp, unique_store_ids, report_data):

    # Initialize an empty DataFrame to store filtered data
    combined_filtered_data = pd.DataFrame()

    # Initialize a list to store filtered data
    filtered_data_list = []

    for store_id in unique_store_ids:

        # Filter valid_data_df for the current store_id
        store_data = valid_data_df[valid_data_df['store_id'] == store_id]

        # Calculate the timestamp for one day ago
        one_day_ago = current_timestamp - timedelta(days=1)
        one_day_ago = pd.to_datetime(one_day_ago)

        # Filter data for the specified time interval
        filtered_data = store_data.loc[
            (store_data["timestamp_utc"].between(one_day_ago, current_timestamp))

        ]

        # Include previous row if filtered_data is empty or first timestamp is greater than one_day_ago
        if filtered_data.empty or filtered_data.iloc[0]["timestamp_utc"] > one_day_ago:
            prev_row = store_data[store_data["timestamp_utc"] < one_day_ago].tail(1)
            filtered_data = pd.concat([prev_row, filtered_data], ignore_index=True)

        # Append filtered_group to the list
        filtered_data_list.append(filtered_data)
    
    # Concatenate all filtered dataframes into a single DataFrame
    combined_filtered_data = pd.concat(filtered_data_list, ignore_index=True)

    # Loop through unique store_ids
    unique_store_ids = valid_data_df['store_id'].unique()

    for store_id in unique_store_ids:

        # Filter combined_filtered_data for current store_id
        store_filtered_data = combined_filtered_data[combined_filtered_data['store_id'] == store_id]

        # Initialize variables for uptime and downtime
        uptime = timedelta()
        downtime = timedelta()
 
        # Calculate uptime and downtime based on the states and durations
        for i in range(len(store_filtered_data)):
            if(len(store_filtered_data) == 1):
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime = current_timestamp - one_day_ago
                else:
                    downtime = current_timestamp - one_day_ago
            else:
                if(i != len(store_filtered_data) - 1):
                    if(i==0 and store_filtered_data['timestamp_utc'].iloc[i] < one_day_ago):
                        start_time = one_day_ago
                        
                    else:
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                    end_time = store_filtered_data['timestamp_utc'].iloc[i+1]
                    duration = end_time - start_time
                    
                else:
                    # Calculate uptime and downtime for the last state
                    if(store_filtered_data['timestamp_utc'].iloc[i] < current_timestamp):
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                        end_time = current_timestamp
                        duration = end_time - start_time
                    else:
                        break
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime += duration
                else:
                    downtime += duration

        # Convert uptime and downtime to hours
        uptime_last_day = round(uptime.total_seconds() / 3600, 2)
        downtime_last_day = round(downtime.total_seconds() / 3600, 2)
        
        # Append the data to report_data dictionary
        report_data['uptime_last_day (in hours)'].append(uptime_last_day)
        report_data['downtime_last_day (in hours)'].append(downtime_last_day)


# Function to calculate uptime and downtime for last week
def uptime_downtime_last_week(valid_data_df, current_timestamp, unique_store_ids, report_data):

    # Initialize an empty DataFrame to store filtered data
    combined_filtered_data = pd.DataFrame()

    # Initialize a list to store filtered data
    filtered_data_list = []

    for store_id in unique_store_ids:

        # Filter valid_data_df for the current store_id
        store_data = valid_data_df[valid_data_df['store_id'] == store_id]

        # Calculate the timestamp for one week ago
        one_week_ago = current_timestamp - timedelta(weeks=1)
        one_week_ago = pd.to_datetime(one_week_ago)

        # Filter data for the specified time interval
        filtered_data = store_data.loc[
            (store_data["timestamp_utc"].between(one_week_ago, current_timestamp))
        ]

        # Include previous row if filtered_data is empty or first timestamp is greater than one_week_ago
        if filtered_data.empty or filtered_data.iloc[0]["timestamp_utc"] > one_week_ago:
            prev_row = store_data[store_data["timestamp_utc"] < one_week_ago].tail(1)
            if not prev_row.empty:
                filtered_data = pd.concat([prev_row, filtered_data], ignore_index=True)

        # Append filtered_group to the list
        filtered_data_list.append(filtered_data)
    
    # Concatenate all filtered dataframes into a single DataFrame
    combined_filtered_data = pd.concat(filtered_data_list, ignore_index=True)

    # Loop through unique store_ids
    unique_store_ids = valid_data_df['store_id'].unique()
    for store_id in unique_store_ids:

        # Filter combined_filtered_data for current store_id
        store_filtered_data = combined_filtered_data[combined_filtered_data['store_id'] == store_id]

        # Initialize variables for uptime and downtime
        uptime = timedelta()
        downtime = timedelta()
 
        # Calculate uptime and downtime based on the states and durations
        for i in range(len(store_filtered_data)):
            if(len(store_filtered_data) == 1):
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime = current_timestamp - one_week_ago
                else:
                    downtime = current_timestamp - one_week_ago
            else:
                if(i != len(store_filtered_data) - 1):
                    if(i==0 and store_filtered_data['timestamp_utc'].iloc[i] < one_week_ago):
                        start_time = one_week_ago
                        
                    else:
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                    end_time = store_filtered_data['timestamp_utc'].iloc[i+1]
                    duration = end_time - start_time
                    
                else:
                    # Calculate uptime and downtime for the last state
                    if(store_filtered_data['timestamp_utc'].iloc[i] < current_timestamp):
                        start_time = store_filtered_data['timestamp_utc'].iloc[i]
                        end_time = current_timestamp
                        duration = end_time - start_time
                    else:
                        break
                if store_filtered_data['status'].iloc[i] == 'active':
                    uptime += duration
                else:
                    downtime += duration

        # Edge case 
        # Check the status of the first row for this store ID
        if store_filtered_data.iloc[0]['timestamp_utc'] > one_week_ago:
            first_status = store_filtered_data.iloc[0]['status']

            # Take appropriate action based on the first_status
            if first_status == 'active':
                uptime += store_filtered_data.iloc[0]['timestamp_utc'] - one_week_ago
            
            else:
                downtime += store_filtered_data.iloc[0]['timestamp_utc'] - one_week_ago

        # Convert uptime and downtime to hours
        uptime_last_week = round(uptime.total_seconds() / 3600, 2)
        downtime_last_week = round(downtime.total_seconds() / 3600, 2)

        # Append data to report_data dictionary
        report_data['uptime_last_week (in hours)'].append(uptime_last_week)
        report_data['downtime_last_week (in hours)'].append(downtime_last_week)
