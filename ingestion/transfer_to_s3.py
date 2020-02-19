#!/usr/bin/env python

import subprocess
import boto3
import wget
import os
from bs4 import BeautifulSoup
import requests



''' This function parses the urls of data dump files and saves urls and file names in arrays ''' 
def extract_url(data_url):
    
    page = requests.get(data_url)
    soup = BeautifulSoup(page.text, features = 'lxml')
    file_names = []
    
    for link in soup.find_all('a'):
        if link.get('href')[-3:] == '.7z':
            file_names.append(link.get('href'))
    return file_names



''' This script downloads the 7z files of Stack Overflow contents and extracts into xml files and sends then to S3 ''' 
if __name__ == '__main__':

    # Create an S3 client
    s3 = boto3.client('s3')

    # Create an S3 bucket
    bucket_name = 'stack-overflow-file-dump-xml'


    source_url = 'https://archive.org/download/stackexchange/'
    file_names = extract_url(source_url)


    for filename in file_names:
        if "stackoverflow.com-" in filename:

            dir_name = filename[:-3]
            url = source_url + filename
            print('Downloading ' + filename + '...')
            wget.download(url)
            process_7z_shell = "process_7z.sh"
            subprocess.call([process_7z_shell], shell=True)
            # use AWS CLI to bulk upload instead of boto3
            print('Uploading ' + dir_name + ' to S3...')
            new_dir = dir_name + '/*.xml'
            s3.upload_file(filename, bucket_name, new_dir)
            os.remove(filename)

        else: pass 