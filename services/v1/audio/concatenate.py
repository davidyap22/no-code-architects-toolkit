# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import ffmpeg
import logging # <-- ADDED: Import logging module

# Ensure you have necessary imports
# from services.file_management import download_file 
# from config import LOCAL_STORAGE_PATH

# Configure logging to use standard output for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) # <-- ADDED: Define logger

def process_audio_concatenate(media_urls, job_id, webhook_url=None):
    """
    Combine multiple audio files into one, using a robust FFmpeg concat demuxer 
    with absolute and POSIX-style paths to handle complex filenames (e.g., Chinese characters).
    """
    input_files = []
    output_filename = f"{job_id}.mp3"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    try:
        # Download all media files
        logger.info("Starting download of input audio files...") # <-- CHANGED from print
        for i, media_item in enumerate(media_urls):
            # Assuming media_item['audio_url'] is the correct key based on your usage
            url = media_item['audio_url']
            # Download file using a temporary, simple name to reduce path complexity during operation
            temp_filename_local = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}.mp3")
            
            # Assuming download_file returns the path to the downloaded file
            # If download_file handles the full path, this line is fine:
            # NOTE: We assume 'download_file' is imported and working.
            input_filename = download_file(url, temp_filename_local)
            input_files.append(input_filename)
            logger.info(f"Downloaded: {url} to {input_filename}") # <-- CHANGED from print

        # 1. Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        logger.info(f"Creating concat list file: {concat_file_path}") # <-- CHANGED from print

        with open(concat_file_path, 'w', encoding='utf-8') as concat_file:
            for input_file in input_files:
                # IMPORTANT: Convert to absolute path, normalize it, and replace backslashes 
                # with forward slashes (POSIX style) for FFmpeg compatibility.
                abs_posix_path = os.path.abspath(input_file).replace(os.path.sep, '/')
                
                # IMPORTANT: Use 'file ' format in the concat list
                concat_file.write(f"file '{abs_posix_path}'\n")

        # 2. Use the concat demuxer to concatenate the audio files
        # We switch to re-encoding (libmp3lame) if 'copy' fails due to inconsistent input streams.
        # Use c='copy' for maximum speed ONLY if you are certain all 9 files have identical formats.
        logger.info("Starting FFmpeg concatenation...") # <-- CHANGED from print
        
        # NOTE: We are capturing stdout/stderr to help with debugging, but these large strings 
        # might be contributing to the response size if the platform wraps them.
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(
                output_path, 
                c='copy',  # Try 'copy' first for speed
                # If 'copy' fails, uncomment the lines below and comment out 'c='copy''
                # c:a='libmp3lame',  # Re-encode to a consistent MP3 format
                # b:a='192k',       # Set a consistent bitrate
                # ar='44100'        # Set a consistent sample rate
            )
            .run(overwrite_output=True, capture_stdout=True, capture_stderr=True) # Capture output for debugging
        )
        logger.info(f"Audio combination successful: {output_path}") # <-- CHANGED from print

        # 3. Clean up input files
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        # Final check
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        return output_path
        
    except ffmpeg.Error as e:
        logger.error("="*50)
        logger.error("FFmpeg Concatenation Failed!")
        # These outputs are for debugging logs, not the response body.
        logger.error(f"Stdout:\n{e.stdout.decode('utf8')}") 
        logger.error(f"Stderr:\n{e.stderr.decode('utf8')}")
        logger.error("="*50)
        raise Exception(f"FFmpeg error during concatenation. See logs for details.") from e
        
    except Exception as e:
        logger.error(f"Audio combination failed: {str(e)}") # <-- CHANGED from print
        raise
