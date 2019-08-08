import pandas as pd


def determine_well_type(injector_value):
    value = "injector" if injector_value else "producer"
    return value


def transform(filename_path, date_column_name, start_date, end_date):
    # let the new data frame be called NEW_DATA_FRAME
    new_data_frame = pd.DataFrame({})

    # Select data only within that date range
    data_frame = pd.read_excel(filename_path)
    data_frame[date_column_name] = pd.to_datetime(data_frame[date_column_name])
    mask = (data_frame[date_column_name] >= start_date) & (data_frame[date_column_name] <= end_date)
    data_frame = data_frame.loc[mask]

    # Add all the unique dates to the data frame
    unique_date = data_frame['Date'].unique()

    new_data_frame['Date'] = unique_date
    new_data_frame['days'] = 0
    print(unique_date[0])

    # Iterate through the data frame selected
    for index, row in data_frame.iterrows():
        my_index = new_data_frame.loc[new_data_frame['Date'] == row[date_column_name]].index[0]
        well_type = "%s-%s" % (row['Well Name'], determine_well_type(row['Is Injector Well']))
        if well_type not in new_data_frame.columns:
            new_data_frame[well_type] = 0
        new_data_frame.at[my_index, well_type] = row['Oil Production Rate']
        new_data_frame.at[my_index, 'days'] = (row[date_column_name] - unique_date[0]).days

    # Convert date column which is in type of TIMESTAMP to string
    new_data_frame['Date'] = new_data_frame['Date'].dt.strftime('%Y-%m-%d')
    print(new_data_frame)

    # export the new data frame into an excel
    new_data_frame.to_excel(r'export_dataframe.xlsx', index=None, header=True)
    # export the new data frame into a csv
    new_data_frame.to_csv('results.csv', index=False)


transform('myexcel.xls', 'Date', '01/01/2000', '10/02/2000')
