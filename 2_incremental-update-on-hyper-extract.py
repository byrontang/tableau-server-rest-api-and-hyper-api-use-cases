import os
import zipfile
from datetime import date
import tableauserverclient as TSC
from tableauhyperapi import *
from tableau_tools import *
from tableau_tools.tableau_documents import *

""" 
Set up Variables
"""
main_hyper_extract = '.hyper'  # The file path of main hyper extract with historical data
main_tdsx_file = '.tdsx'  # The file path of main .tdsx workbook with historical data
new_extract_directory = '' + str(date.today()) # The directory to save new extracts

account = ''
password = ''
server = ''
site = ''

""" 
Read Data from Main Extract 

Get the latest data in current extract.
In Hyper api, a hyper file is considered as a database. 
Therefore, the hyper file path will be passed into the database argument.
"""
# Start Hyper
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    #  Connect to an existing .hyper file (CreateMode.NONE)
    with Connection(endpoint=hyper.endpoint, database=main_hyper_extract) as connection:
        table_names = connection.catalog.get_table_names(schema="Extract")

        for table in table_names:
            # # Run the following codes to get columns if necessary
            # table_definition = connection.catalog.get_table_definition(name=table)
            # print(f"Table {table.name} has qualified name: {table}")
            # for column in table_definition.columns:
            #     print(f"Column {column.name} has type={column.type} and nullability={column.nullability}")
            #     print("")

            # Get latest dates from extract
            table_name = TableName("Extract", "Extract")
            query = f"SELECT MAX(created_at) FROM {table_name}"
            # In the following query, tableauhyperapi.timestamp.Timestamp is returned.
            # This can be used when running query in new extract.
            latest_time = connection.execute_scalar_query(query=query)
            print("Latest time in main extract:", latest_time)  # tableauhyperapi.timestamp.Timestamp

            print("Current row count:",
                  connection.execute_scalar_query(f"SELECT COUNT(*) FROM {table_name}"))

"""
Download Data Source from Server (REST API)

The downloaded data source will be in .tdsx format
"""
file_path = new_extract_directory + '\\PackagedDataSource'

# Create directory to download
try:
    os.mkdir(new_extract_directory)
    print('Directory {} is created.'.format(new_extract_directory))
except FileExistsError:
    print('Directory of {} already exists.'.format(str(date.today())))

# Sign-in
tableau_auth = TSC.TableauAuth(account, password, site)

# add user_server_version=True to use the latest version
server = TSC.Server(server, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    # # Run the following code to find the data source if necessary
    # all_datasources, pagination_item = server.datasources.get()
    # for datasource in all_datasources:
    #     print(datasource.name, datasource.id)

    server.datasources.download('445ccaa3-7b62-445f-ae43-ca35e55259a6',
                                file_path)
print('Extract is downloaded to {}.'.format(new_extract_directory))

"""
Unzip Packaged Data Source (.tdsx file)

After unzipping .tdsx file, we can work with the .hyper file.
"""
with zipfile.ZipFile(file_path + '.tdsx', 'r') as zip_ref:
    zip_ref.extractall(file_path)

new_hyper_extract = file_path + '\\Data\\Extracts\\' + os.listdir(file_path + '\\Data\\Extracts')[0]
print("Unzipped workbook. Saved hyper file saved at:", new_hyper_extract)

""" 
Read Data from New Hyper Files 
"""
# Start Hyper
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    #  Connect to an existing .hyper file (CreateMode.NONE)
    with Connection(endpoint=hyper.endpoint, database=new_hyper_extract) as connection:
        table_names = connection.catalog.get_table_names(schema="Extract")

        for table in table_names:
            # # Run the folloing codes to get columns if neccessary
            # table_definition = connection.catalog.get_table_definition(name=table)
            # print(f"Table {table.name} has qualified name: {table}")
            # for column in table_definition.columns:
            #     print(f"Column {column.name} has type={column.type} and nullability={column.nullability}")
            #     print("")

            # Get new rows
            table_name = TableName("Extract", "Extract")

            query = f"SELECT * \
                        FROM {table_name} \
                       WHERE created_at > '{latest_time}'"
            new_rows = connection.execute_list_query(query=query)
            print(len(new_rows), 'new rows detected.')

            # Get latest time for checking
            new_time = connection.execute_scalar_query(f"SELECT MAX(created_at) FROM {table_name}")

""" 
Insert Data in Hyper File 
"""
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=main_hyper_extract) as connection:

        table_name = TableName("Extract", "Extract")
        latest_time = connection.execute_scalar_query(f"SELECT MAX(created_at) FROM {table_name}")

        # Check whether the latest times between two files
        if latest_time >= new_time:
            print("No update - main extract already has the updated data.")

        # Update the file only if the new data hasn't been inserted
        else:
            with Inserter(connection, table_name) as inserter:
                inserter.add_rows(rows=new_rows)
                inserter.execute()
            print(f"Main extract updated - new rows inserted to {table_name}.")

        # Number of rows after update
        print("Row count after update:",
              connection.execute_scalar_query(f"SELECT COUNT(*) FROM {table_name}"))

""" Swap Hyper File
If the hyper file is published directly, the column names will be different . (ex: name3 instead of Id (Hist Sites)). 
The inconsistent column names will break the report built upon the original extract.

Code reference: https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/publish-multi-table-hyper 
"""
# Uses tableau_tools to replace the hyper file in the TDSX.
try:
    local_tds = TableauFileManager.open(filename=main_tdsx_file)
except TableauException as e:
    sys.exit(e)

filenames = local_tds.get_filenames_in_package()
for filename in filenames:
    #     print(filename)
    if filename.find('.hyper') != -1:
        local_tds.set_file_for_replacement(filename_in_package=filename,
                                           replacement_filname_on_disk=main_hyper_extract)
        break

# Overwrites the original TDSX file locally.
tdsx_name_before_extension, tdsx_name_extension = os.path.splitext(main_tdsx_file)
# print(tdsx_name_before_extension, '\n', tdsx_name_extension)
tdsx_updated_name = tdsx_name_before_extension + '_updated' + tdsx_name_extension
local_tds.save_new_file(new_filename_no_extension=tdsx_updated_name)
os.remove(main_tdsx_file)
os.rename(tdsx_updated_name, main_tdsx_file)

""" 
Publish Packaged Data Source to Server (REST API) 
"""
with server.auth.sign_in(tableau_auth):
    # # Run the fllowing code to get project ID if neccessary
    # all_project_items, pagination_item = server.projects.get()
    # for proj in all_project_items:
    #     print(proj.name, proj.id)

    project_id = ''  # type in id

    # Use the project id to create new datsource_item
    new_datasource = TSC.DatasourceItem(project_id)

    # publish data source (specified in file_path)
    new_datasource = server.datasources.publish(new_datasource, main_tdsx_file, 'Overwrite')

    print('File published to {} site: {}'.format(site, main_tdsx_file))