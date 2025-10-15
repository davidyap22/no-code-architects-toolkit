# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import subprocess
import logging
import uuid
from typing import List, Tuple
from services.file_management import download_file, cleanup_files
from services.cloud_storage import upload_file
from config import LOCAL_STORAGE_PATH

# Set up logging
logger = logging.getLogger(__name__)

def process_audio_concatenation(audio_urls: List[str], job_id: str) -> str:
    """
    Downloads multiple audio files, combines them using FFmpeg, uploads the result 
    to cloud storage, and cleans up local temporary files.

    Args:
        audio_urls (List[str]): List of public URLs for the audio files.
        job_id (str): A unique ID for the processing job (used for file naming).

    Returns:
        str: The public URL of the final merged audio file in cloud storage.
    
    Raises:
        Exception: If downloading, FFmpeg processing, or uploading fails.
    """
    downloaded_files: List[str] = []
    output_path: str = ""
    
    try:
        logger.info(f"Starting audio concatenation for job: {job_id}. Total files: {len(audio_urls)}")
        
        # 1. Download all input audio files
        for url in audio_urls:
            local_path = download_file(url, LOCAL_STORAGE_PATH)
            downloaded_files.append(local_path)
            logger.info(f"Downloaded file: {url} to {local_path}")

        # 2. Create the FFmpeg concat list file
        concat_list_path = os.path.join(LOCAL_STORAGE_PATH, f"concat_list_{job_id}.txt")
        with open(concat_list_path, 'w') as f:
            for file_path in downloaded_files:
                # Use 'file' protocol to reference files in the same directory
                f.write(f"file '{os.path.basename(file_path)}'\n")
        
        # Add concat list to cleanup list
        downloaded_files.append(concat_list_path)

        # 3. Perform FFmpeg concatenation
        output_filename = f"{job_id}_merged.mp3"
        output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)
        
        # FFmpeg command for concatenation:
        # -f concat: Reads the list file
        # -safe 0: Allows relative paths in the list file
        # -i {list}: Specifies the input list file
        # -c copy: Merges without re-encoding (FASTEST and generally recommended for same format)
        cmd = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', concat_list_path,
            '-c', 'copy',
            output_path
        ]
        
        logger.info(f"Running FFmpeg command for merge: {' '.join(cmd)}")
        
        # Run the FFmpeg command
        # Note: We execute from LOCAL_STORAGE_PATH to make file paths in the list relative
        process = subprocess.run(
            cmd, 
            cwd=LOCAL_STORAGE_PATH, # Run command from the working directory
            capture_output=True, 
            text=True
        )
        
        if process.returncode != 0:
            logger.error(f"FFmpeg Merge Error: {process.stderr}")
            raise Exception(f"FFmpeg concatenation failed: {process.stderr}")
        
        logger.info(f"Successfully created merged audio: {output_path}")

        # 4. Upload the result to cloud storage
        # Cloud Storage destination path (e.g., "merged_audio/job_id_merged.mp3")
        destination_path = f"merged_audio/{output_filename}"

        # FIX: Now correctly calls upload_file with 2 required arguments
        cloud_url = upload_file(output_path, destination_path)
        
        logger.info(f"Final merged audio uploaded to: {cloud_url}")
        
        return cloud_url
        
    except Exception as e:
        logger.error(f"Audio concatenation failed for job {job_id}: {str(e)}")
        raise
        
    finally:
        # 5. Clean up all temporary files (inputs, list file, and final output)
        if output_path and os.path.exists(output_path):
            downloaded_files.append(output_path)
            
        # Use cleanup_files function to safely remove everything
        cleanup_files(downloaded_files)
        logger.info(f"Cleaned up {len(downloaded_files)} temporary files for job {job_id}.")
