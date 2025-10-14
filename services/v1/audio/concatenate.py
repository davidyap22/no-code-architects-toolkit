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
import ffmpeg
import logging 
import subprocess 

# NOTE: 
# The following imports are assumed to be available in your service structure:
# from services.file_management import download_file 
# from services.cloud_storage import upload_file 
# from config import LOCAL_STORAGE_PATH

# 为了让这段代码在没有实际服务依赖的情况下可以运行，我暂时注释掉上方行，
# 并添加占位符，但在您的 Cloud Run 环境中，您需要取消注释它们。

# 假设占位符 (在您的实际部署中应替换为真实的服务代码)
LOCAL_STORAGE_PATH = "/tmp" 
def download_file(url, target_path): 
    # This is a placeholder for your actual download logic
    logging.warning(f"PLACEHOLDER: Downloading {url} to {target_path}")
    return target_path

def upload_file(local_path, destination_path):
    # This is a placeholder for your actual cloud upload logic
    logging.warning(f"PLACEHOLDER: Uploading {local_path} to {destination_path}")
    return f"https://your-storage-bucket/public-url/{destination_path}"


# Configure logging to use standard output for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_audio_concatenate(media_urls, job_id, webhook_url=None):
    """
    Combine multiple audio files into one using FFmpeg's concat demuxer.
    
    This function:
    1. Downloads all input audio files.
    2. Creates a concat list file with POSIX-style absolute paths.
    3. Uses FFmpeg's concat demuxer to merge the files quickly (using c='copy').
    4. Uploads the final merged audio file to Cloud Storage.
    5. Returns the public URL of the merged file.
    6. Ensures proper cleanup of all local temporary files.
    """
    input_files = []
    output_filename = f"{job_id}.mp3"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    try:
        # 1. Download all media files
        logger.info("Starting download of input audio files...") 
        for i, media_item in enumerate(media_urls):
            url = media_item['audio_url']
            # Use a simple filename for the downloaded file
            temp_filename_local = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}.mp3")
            
            # Assuming download_file is robust and returns the final local path
            input_filename = download_file(url, temp_filename_local)
            input_files.append(input_filename)
            logger.info(f"Downloaded: {url} to {input_filename}")

        # 2. Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        logger.info(f"Creating concat list file: {concat_file_path}") 

        with open(concat_file_path, 'w', encoding='utf-8') as concat_file:
            for input_file in input_files:
                # CRITICAL: Convert to POSIX-style absolute path for FFmpeg compatibility
                abs_posix_path = os.path.abspath(input_file).replace(os.path.sep, '/')
                concat_file.write(f"file '{abs_posix_path}'\n")

        # 3. Use the concat demuxer to concatenate the audio files
        logger.info("Starting FFmpeg concatenation with c='copy'...")
        
        # IMPORTANT FIX: Do not capture stdout/stderr to prevent Response size too large error
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(
                output_path, 
                c='copy',  # Fastest option, but requires uniform input format
            )
            .run(overwrite_output=True) 
        )
        
        logger.info(f"Audio combination successful: {output_path}")

        # 4. Upload file and return URL (CRITICAL FIX for POST 500 error)
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        logger.info(f"Uploading merged audio file to Cloud Storage: {output_path}")
        
        # Use a distinct path in cloud storage, e.g., 'merged_audio/'
        cloud_url = upload_file(output_path, f"merged_audio/{output_filename}")
        
        # 5. Clean up temporary files (local merged file and all inputs)
        os.remove(output_path)
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        logger.info(f"Cleanup complete. Returning URL: {cloud_url}")
        
        # Return the public URL only (short string response)
        return cloud_url
        
    except ffmpeg.Error as e:
        # Logs the failure details without being included in the HTTP response body
        logger.error("="*50)
        logger.error("FFmpeg Concatenation Failed! Check Cloud Run logs for raw FFmpeg output.")
        logger.error("="*50)
        raise Exception(f"FFmpeg error during concatenation. Check Cloud Run logs for details.") from e
        
    except Exception as e:
        logger.error(f"Audio combination failed: {str(e)}")
        # Ensures all temporary files are cleaned up before raising
        if 'input_files' in locals():
            for f in input_files:
                if os.path.exists(f):
                    os.remove(f)
        if 'concat_file_path' in locals() and os.path.exists(concat_file_path):
            os.remove(concat_file_path)
        if 'output_path' in locals() and os.path.exists(output_path):
            os.remove(output_path)
        
        raise
