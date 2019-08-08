import pandas as pd


def calculate_spearman(start_date, end_date, column1, column2, filename="transform_output.xlsx",):
    # Select data only within that date range
    data_frame = pd.read_excel(filename)
    data_frame['Date'] = pd.to_datetime(data_frame['Date'])
    mask = (data_frame['Date'] >= start_date) & (data_frame['Date'] <= end_date)
    data_frame = data_frame.loc[mask]
    print(data_frame)
    corr_val = data_frame[column1].corr(data_frame[column2], method="spearman")
    print(corr_val)

    # print(type(data_frame))
    # print(type(data_frame['Date']))
    #
    # date_row_index = data_frame.loc[data_frame['Date'] == pd.to_datetime(date)].index[0]
    # print(date_row_index)
    # print(data_frame)
    # print(pd.to_datetime(date))
    # #print(data_frame[data_frame['Date'].equals(pd.to_datetime(date))])
    # print(type(data_frame['Date']))




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
    new_data_frame['Date'] = new_data_frame['Date'].dt.strftime('%m/%d/%Y')
    print(new_data_frame)

    # export the new data frame into an excel
    new_data_frame.to_excel(r'transform_output.xlsx', index=None, header=True)
    # export the new data frame into a csv
    new_data_frame.to_csv('transform_output.csv', index=False)


#transform('myexcel.xls', 'Date', '01/01/2000', '10/02/2000')
calculate_spearman('01/02/2000', '10/01/2000', 'C2_Prod2-producer', 'C5_Prod2-producer')
