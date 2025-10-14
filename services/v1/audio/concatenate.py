# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import ffmpeg
import logging 
import subprocess # <-- Ensure subprocess is imported if you use it elsewhere.

# Ensure you have necessary imports
from services.file_management import download_file 
from config import LOCAL_STORAGE_PATH

# Configure logging to use standard output for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.info("Starting download of input audio files...") 
        for i, media_item in enumerate(media_urls):
            url = media_item['audio_url']
            temp_filename_local = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}.mp3")
            
            # NOTE: We assume 'download_file' is imported and working.
            input_filename = download_file(url, temp_filename_local)
            input_files.append(input_filename)
            logger.info(f"Downloaded: {url} to {input_filename}")

        # 1. Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        logger.info(f"Creating concat list file: {concat_file_path}") 

        with open(concat_file_path, 'w', encoding='utf-8') as concat_file:
            for input_file in input_files:
                abs_posix_path = os.path.abspath(input_file).replace(os.path.sep, '/')
                concat_file.write(f"file '{abs_posix_path}'\n")

        # 2. Use the concat demuxer to concatenate the audio files
        logger.info("Starting FFmpeg concatenation...")
        
        # *** 关键修复: 移除 capture_stdout 和 capture_stderr ***
        # 这将避免捕获长达7分钟的FFmpeg详细输出，防止响应体超限。
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(
                output_path, 
                c='copy',  # 沿用 'copy' 以保持速度，假设格式已一致
            )
            .run(overwrite_output=True) # 仅运行，不捕获输出
        )
        
        logger.info(f"Audio combination successful: {output_path}")

        # 3. Clean up input files
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        # Final check
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        return output_path
        
    except ffmpeg.Error as e:
        # 由于我们不再捕获 stdout/stderr，所以我们不能在这里打印它们。
        logger.error("="*50)
        logger.error("FFmpeg Concatenation Failed! Check Cloud Run logs for raw FFmpeg output.")
        logger.error("="*50)
        # 抛出异常以中断工作流
        raise Exception(f"FFmpeg error during concatenation. Check Cloud Run logs for details.") from e
        
    except Exception as e:
        logger.error(f"Audio combination failed: {str(e)}")
        raise

