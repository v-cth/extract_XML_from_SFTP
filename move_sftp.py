
import paramiko
import pandas


# Source path
source_folder = '/home/Analysis/'
# Destination path
destination_folder = '/home/Analysis/Import_completed/'

# Get list of file stored in database
query = 'SELECT DISTINCT filename FROM fct_results'
stored_db = pd.read_sql(query, conn)

# SFTP connection
sftp = conn_sftp(host, port, username, password)

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


