
import urllib.request
import shutil
import zipfile
import os


def download_from_ftp(file_url: str, destination_path: str):
    # Open the FTP connection
    with urllib.request.urlopen(file_url) as response:
        # Open the destination file in write binary mode
        with open(destination_path, 'wb') as file:
            # Copy the contents of the FTP response to the destination file
            shutil.copyfileobj(response, file)

def get_home_path():
    return os.path.expanduser('~')


def unzip_file(zip_file_path, extract_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
        print("File successfully unzipped!")