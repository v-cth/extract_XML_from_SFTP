import datetime
import paramiko
from stat import S_ISDIR, S_ISREG

#Define max period to retrieve 
days_to_retrieve = 3

# Connection parameters 
sftp_host = '00.000.00.000'  # IP adress or host
sftp_port = 22  # Port SFTP 
sftp_username = 'user' 
sftp_password = 'pwd' 
sftp_directory = '/home/Exchange'


def conn_sftp(host, port, username, password):
    # connect to sftp
    transport = paramiko.Transport((host, port))
    print("connecting to SFTP...")
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("connection established.")
    return sftp


def get_files_from_sftp(sftp, sftp_directory, days_to_retrieve):
    
    start_date = datetime.datetime.now() - datetime.timedelta(days_to_retrive) 

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
        xml_content_formated = xml_content.decode('utf8').replace("'", "") # get rid of single quotes and decode utf 8
        xml_files[i] = (filename, xml_content_formated, updated_at)
         
    
    # Close SFTP connection
    sftp.close()
    
    return xml_files
