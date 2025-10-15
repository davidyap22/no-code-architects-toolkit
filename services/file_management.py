# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



import os
import requests
import logging
from typing import List
import uuid  # <-- 缺失的导入

logger = logging.getLogger(__name__)

def download_file(url: str, storage_path: str) -> str:
    """
    Downloads a file from a URL to the specified local storage path.

    Args:
        url (str): The public URL of the file to download.
        storage_path (str): The local directory path (e.g., /tmp/) to save the file.

    Returns:
        str: The full local path of the downloaded file.
    
    Raises:
        Exception: If the download fails.
    """
    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    # Note: local_filename logic will be fully determined by the UUID logic below for safety.
    local_filename = os.path.join(storage_path, os.path.basename(url).split('?')[0])
    
    logger.info(f"Attempting to download {url} to {local_filename}")

    try:
        # Stream download to handle large files efficiently
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            
            # Use UUID for robust, unique local filenames
            unique_id = str(uuid.uuid4()).split('-')[0]
            extension = os.path.splitext(os.path.basename(url).split('?')[0])[1] or '.mp4' 
            # Ensure the extension is valid (e.g., handles /file.mp3?query=)
            if not extension or len(extension) > 5:
                extension = '.' + os.path.basename(url).split('.')[-1].split('?')[0]
                if len(extension) > 5: # Fallback safety check
                    extension = '.mp4'
            
            local_filename = os.path.join(storage_path, f"{unique_id}{extension}")

            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Download complete: {local_filename}")
            return local_filename
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download file from {url}: {e}")
        raise Exception(f"File download failed: {e}")


def cleanup_files(file_paths: List[str]):
    """
    Safely removes a list of local files. Logs errors but does not raise exceptions.
    """
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Successfully cleaned up: {file_path}")
        except OSError as e:
            logger.error(f"Error removing temporary file {file_path}: {e}")
            pass 
