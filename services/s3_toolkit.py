# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY and FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



import os
import boto3
import logging
from urllib.parse import urlparse, quote

logger = logging.getLogger(__name__)

# FIX: Add destination_path to the function signature
def upload_to_s3(file_path, s3_url, access_key, secret_key, bucket_name, region, destination_path):
    
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    client = session.client('s3', endpoint_url=s3_url)

    # Use the provided destination_path as the key name in the bucket (e.g., 'merged_audio/job_id.mp3')
    target_key = destination_path 

    try:
        logger.info(f"Uploading file to S3: {file_path} to s3://{bucket_name}/{target_key}")
        
        # Upload the file to the specified S3 bucket using the target_key
        with open(file_path, 'rb') as data:
            client.upload_fileobj(
                data, 
                bucket_name, 
                target_key, # FIX: Use target_key instead of os.path.basename(file_path)
                ExtraArgs={'ACL': 'public-read'}
            )

        # FIX: The file URL should use the entire target_key (which includes the 'folder')
        # URL encode the entire target_key
        encoded_key = quote(target_key, safe=':/') # Use safe characters for S3 path structure
        
        # Construct the URL, assuming the S3_ENDPOINT_URL is base-compatible
        # Example: https://sgp-labs.nyc3.digitaloceanspaces.com/bucket_name/merged_audio/job_id.mp3
        file_url = f"{s3_url}/{bucket_name}/{encoded_key}"
        
        logger.info(f"File uploaded successfully to S3: {file_url}")
        return file_url
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        raise
