import paramiko
import datetime
#from datetime import datetime, timedelta
from stat import S_ISDIR, S_ISREG
import pandas as pd
import snowflake.connector
import xml.etree.ElementTree as ET



def conn_sftp(host, port, username, password):
    # connect to sftp
    transport = paramiko.Transport((host, port))
    print("connecting to SFTP...")
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("connection established.")
    return sftp


def get_files_from_sftp(sftp, sftp_directory, days_to_retrieve):
    
    start_date = datetime.datetime.now() - datetime.timedelta(days_to_retrieve) 

    # Retrieve files list matching criterias
    xml_files = [] 
    files = sftp.listdir(sftp_directory)
    for file in files:
        # check file name to match 'Analysis....xml'
        if file.endswith('.xml') and file.startswith('Analysis'):

            # Check if files were modified in the last n days
            file_attributes = sftp.stat(f"{sftp_directory}/{file}")
            last_modified_at = datetime.datetime.fromtimestamp(file_attributes.st_mtime)

            if last_modified_at > start_date:
                xml_files.append((file, None, last_modified_at))
    

    # Read XML files content and decode them
    for i, (filename, _, updated_at) in enumerate(xml_files):
        with sftp.open(f"{sftp_directory}/{filename}", 'r') as xml_file:
          chunk_size = 4096  # Max chunk size in bytes  
          xml_content = b''  # Initialize xml_content 
          print(filename)
          while True:
            content_bloc = xml_file.read(chunk_size)
            if not content_bloc:
                break
            xml_content += content_bloc
        xml_content_formated = xml_content.decode('utf8').replace("'", "")
        xml_files[i] = (filename, xml_content_formated, updated_at)
    
    # Close SFTP connection
    sftp.close()
    
    return xml_files


#Find node and convert content to text
def getElement(Node,ToFind):
    elm = Node.find(ToFind)
    if elm is not None:
        elm = elm.text
    else:
        elm = ''
    return elm

#Flattend xml in table
def parseXml(xml_to_process):  

    data = []
    if xml_to_process is not None:
        for row in xml_to_process:
            file_name = row[0]
            xml_content = row[1]
            file_updated_at = row[2]
            print(file_name)

            # Load XML content from string
            root = ET.fromstring(xml_content)

            # File elements
            sample_code = getElement(root,'SampleCode')
            sample_description = getElement(root,'SampleDescription')
            recipient_lab_code = getElement(root,'RecipientLabCode') 
            reception_date = getElement(root,'ReceptionDate')

            # Navigate in XML file and extract 
            for fraction in root.iter('Fraction'):

                for test in fraction.iter('Test'):
                    test_ref = getElement(test,'TestReference')
                    test_code = getElement(test,'TestCode')

                    for parameter in test.iter('Parameter'):
                        parameter_code = getElement(parameter,'ParameterCode')

                        for result in parameter.iter('Result'):
                            result_value = getElement(result,'ResultValue')
                            result_unit = getElement(result,'ResultUnit')

                            # Append data to a list
                            data.append([
                                file_name,

                                sample_code,
                                sample_description,
                                recipient_lab_code,
                                reception_date,
               
                                test_ref,
                                test_code,

                                parameter_code,

                                result_value,
                                result_unit,
                                file_updated_at
                            ])

                            # Display extracted values
                            print("FileName:", file_name)
                            print("FileUpdatedAt:", file_updated_at)
                            print("SampleCode:", sample_code)
                            print("SampleDescription:", sample_description)
                            print("RecipientLabCode", recipient_lab_code)
                            print("ReceptionDate:", reception_date)
                            print("TestReference:", test_ref)
                            print("TestCode:", test_code)
                            print("ParameterCode:", parameter_code)
                            print("ResultValue:", result_value)
                            print("ResultUnit:", result_unit)
                            print("---")

    # Create dataframe
    result_table = pd.DataFrame(data)
    result_table.columns = [
        "FileName",
        "SampleCode",
        "SampleDescription",
        "RecipientLabCode",
        "ReceptionDate",
        "TestReference",
        "TestCode",
        "ParameterCode",
        "ResultValue",
        "ResultUnit",
        "FileUpdatedAt"
    ]

    return result_table


#Define max period to retrieve 
days_to_retrieve = 3

# Connection parameters 
sftp_host = '00.000.00.000'  # IP adress or host
sftp_port = 22  # Port SFTP 
sftp_username = 'user' 
sftp_password = 'pwd' 
sftp_directory = '/home/Exchange'


sftp = conn_sftp(sftp_host,sftp_port,sftp_username,sftp_password)

xml_files = get_files_from_sftp(sftp, sftp_directory, days_to_retrieve)

result_table = parseXml(xml_files)


###### Write results in Snowflake table ######


# Configure Snowflake connection
conn = snowflake.connector.connect(
    user='test',
    password='pwd',
    account='host',
    warehouse='default',
    database='prod',
    schema='analytics'
)

# Write DataFrame in a Snowflake table fct_results (must have the same fields as dataframe)
result_table.to_sql('fct_results', conn, if_exists='append', index=False) 

# Close Snowflake connection
conn.close()


###### Move files processed ######

# Source path
source_folder = '/home/Analysis/'
# Destination path
destination_folder = '/home/Analysis/Import_completed/'

# Get list of file stored in database
query = 'SELECT DISTINCT filename FROM fct_results'
stored_db = pd.read_sql(query, conn)

# SFTP connection
sftp = conn_sftp(sftp_host,sftp_port,sftp_username,sftp_password)

# Get list of file processed
to_move = result_table['FileName'].unique()

# Move files
if to_move:
  for filename in to_move:
    if filename in stored_db: # Check if the file is in the database
      source_path = source_folder + filename
      destination_path = destination_folder + filename

      try:
          sftp.rename(source_path, destination_path)
          print(f'File {source_path} has been moved to {destination_path}')

      except Exception as e:
          print(f"An error occured : {str(e)}")


# Close SFTP connection
sftp.close()

