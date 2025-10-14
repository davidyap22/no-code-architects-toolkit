# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import ffmpeg
import logging 
import subprocess 

# CRITICAL: These imports must point to your actual service files
from services.file_management import download_file 
from services.cloud_storage import upload_file 
from config import LOCAL_STORAGE_PATH

# Configure logging to use standard output for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_audio_concatenate(media_urls, job_id, webhook_url=None):
    """
    Combine multiple audio files into one using FFmpeg's concat demuxer.
    
    This function applies critical fixes for path compatibility (POSIX style), 
    response size limits (returns URL only), and robust error logging.
    """
    input_files = []
    output_filename = f"{job_id}.mp3"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    try:
        # 1. Download all media files
        logger.info("Starting download of input audio files...") 
        for i, media_item in enumerate(media_urls):
            url = media_item['audio_url']
            temp_filename_local = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}.mp3")
            
            input_filename = download_file(url, temp_filename_local)
            input_files.append(input_filename)
            logger.info(f"Downloaded: {url} to {input_filename}")

        # 2. Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        logger.info(f"Creating concat list file: {concat_file_path}") 

        with open(concat_file_path, 'w', encoding='utf-8') as concat_file:
            for input_file in input_files:
                # CRITICAL FIX: Convert to POSIX-style absolute path for FFmpeg compatibility
                abs_posix_path = os.path.abspath(input_file).replace(os.path.sep, '/')
                concat_file.write(f"file '{abs_posix_path}'\n")

        # 3. Use the concat demuxer to concatenate the audio files
        logger.info("Starting FFmpeg concatenation with c='copy'...")
        
        # CRITICAL FIX: Do not capture stdout/stderr to prevent Response size too large error
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(
                output_path, 
                c='copy',  # Fastest option
            )
            .run(overwrite_output=True) 
        )
        
        logger.info(f"Audio combination successful: {output_path}")

        # 4. Upload file and return URL (CRITICAL FIX for POST 500/Response Size error)
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        logger.info(f"Uploading merged audio file to Cloud Storage: {output_path}")
        
        # Assume upload_file handles the destination bucket logic.
        cloud_url = upload_file(output_path, f"merged_audio/{output_filename}")
        
        # 5. Clean up temporary files (local merged file and all inputs)
        os.remove(output_path)
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        logger.info(f"Cleanup complete. Returning URL: {cloud_url}")
        
        # Return the public URL only
        return {"audio_url": cloud_url}
        
    except ffmpeg.Error as e:
        logger.error("="*50)
        logger.error("FFmpeg Concatenation Failed! Check Cloud Run logs for raw FFmpeg output.")
        logger.error("="*50)
        raise Exception(f"FFmpeg error during concatenation. Check Cloud Run logs for details.") from e
        
    except Exception as e:
        logger.error(f"Audio combination failed: {str(e)}")
        # Ensures cleanup on failure
        if 'input_files' in locals():
            for f in input_files:
                if os.path.exists(f): os.remove(f)
        if 'concat_file_path' in locals() and os.path.exists(concat_file_path): os.remove(concat_file_path)
        if 'output_path' in locals() and os.path.exists(output_path): os.remove(output_path)
        
        raise
