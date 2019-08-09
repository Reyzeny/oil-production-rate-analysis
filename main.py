import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def is_csv(filename_path):
    ext = filename_path[-3:]
    return ext == 'csv'


def determine_well_type(injector_value):
    value = "injector" if injector_value else "producer"
    return value


def get_data_frame(filename):
    # checking for file type (excel or csv)
    if is_csv(filename):
        data_frame = pd.read_csv(filename, header=0)
    else:
        data_frame = pd.read_excel(filename)
    if 'Date' in data_frame.columns:
        data_frame['Date'] = pd.to_datetime(data_frame['Date'])
        return data_frame
    print("Cannot find date column hence cannot proceed")


def transform(filename_path, start_date, end_date):
    # let the new data to be produced be called NEW_DATA_FRAME
    new_data_frame = pd.DataFrame({})
    # load external file
    data_frame = get_data_frame(filename_path)
    # Select data only within a date range
    mask = (data_frame['Date'] >= start_date) & (data_frame['Date'] <= end_date)
    data_frame = data_frame.loc[mask]
    # Add all the unique dates to the new data file
    unique_date = data_frame['Date'].unique()
    new_data_frame['Date'] = unique_date
    new_data_frame['days'] = 0

    # Iterate through the data selected. For each iteration
    #   get the index of the current date in the new data frame so we can perform insertion with respect to that date,
    #   if the well type of the current record is not present in the new data frame as a column then create it,
    #   locate the cell corresponding to the current date and well type column in the new data frame and insert the oil production rate,
    #   locate the cell corresponding to the current date and day column in the new data frame and insert the days difference from starting date,
    for index, row in data_frame.iterrows():
        my_index = new_data_frame.loc[new_data_frame['Date'] == row['Date']].index[0]
        well_type = "%s-%s" % (row['Well Name'], determine_well_type(row['Is Injector Well']))
        if well_type not in new_data_frame.columns:
            new_data_frame[well_type] = 0
        new_data_frame.at[my_index, well_type] = row['Oil Production Rate']
        new_data_frame.at[my_index, 'days'] = (pd.to_datetime(row['Date']) - pd.to_datetime(unique_date[0])).days

    # Convert date column which is in type of TIMESTAMP to string for better file output at the end
    if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(new_data_frame['Date']):
        new_data_frame['Date'] = new_data_frame['Date'].dt.strftime('%m/%d/%Y')
    print(new_data_frame)

    # export the new data frame into an excel
    new_data_frame.to_excel(r'transform_output.xlsx', index=None, header=True)
    # export the new data frame into a csv
    new_data_frame.to_csv('transform_output.csv', index=False)


def calculate_spearman(start_date, end_date, column1, column2, filename="transform_output.csv"):
    # retrieve external output file
    data_frame = get_data_frame(filename)
    # Select the data only within a date range
    mask = (data_frame['Date'] >= start_date) & (data_frame['Date'] <= end_date)
    data_frame = data_frame.loc[mask]
    # verify the columns to carry out correlation on is present else don't proceed
    if column1 not in data_frame.columns or column2 not in data_frame.columns:
        print("one of the columns you entered is not present in the file")
        return
    # perform correlation
    corr_val = data_frame[column1].corr(data_frame[column2], method="spearman")
    print("correlation between %s and %s within %s and %s is %f" % (column1, column2, start_date, end_date, corr_val))
    # draw heat map
    required_data_frame = data_frame[['Date', column1, column2]]
    plt.figure(figsize=(10, 5))
    sns.heatmap(required_data_frame.corr(), annot=True, linewidth=0.5, cmap='coolwarm')
    # You may remove the next line depending on where you are running this code. If some editors have inbuilt graph-
    # dialogs, if so this will not be necessary.
    plt.show()


'''
To only transform the data, call the function transform(name_of_file, start_date, end_date). This will produce 2 files
called 'transform_output.csv' and 'transform_output.xlsx' in the current directory of the project.

To calculate the correlation, cal the function calculate_spearman(start_date, end_date, first_column, second_column),
the value will be printed and the graph will be shown.   
'''
# transform('input.xls', '01/01/2000', '10/02/2003')
# calculate_spearman('01/02/2000', '10/01/2000', 'C2_Prod2-producer', 'C5_Prod2-producer')
transform('input.csv', '7/1/2016', '4/1/2018')
calculate_spearman('07/1/2016', '04/1/2018', 'B10-producer', 'B13-producer')

