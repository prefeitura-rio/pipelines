from prefect import task
from ftplib import FTP

@task
def download_from_ftp(ftp_url, remote_filepath, local_filepath):
    try:
        # Create an FTP connection
        ftp = FTP(ftp_url)
        
        # Change to the directory containing the file
        ftp.cwd(remote_filepath)
        
        # Open a local file for writing
        with open(local_filepath, 'wb') as local_file:
            # Download the remote file to the local file
            ftp.retrbinary('RETR ' + remote_filepath, local_file.write)
        
        print(f"File downloaded successfully to {local_filepath}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the FTP connection
        ftp.quit()
