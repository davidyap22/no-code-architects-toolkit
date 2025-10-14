# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import ffmpeg
import logging 
import subprocess 

# Ensure you have necessary imports
from services.file_management import download_file 
from services.cloud_storage import upload_file # <-- 确保这个 import 存在
from config import LOCAL_STORAGE_PATH

# Configure logging to use standard output for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_audio_concatenate(media_urls, job_id, webhook_url=None):
    """
    Combine multiple audio files into one.
    """
    input_files = []
    output_filename = f"{job_id}.mp3"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    # --- (省略了前面下载和拼接的代码) ---

    try:
        # ... (前面是文件下载和 concat list 创建逻辑)

        # 2. Use the concat demuxer to concatenate the audio files
        logger.info("Starting FFmpeg concatenation...")
        
        # 仅运行，不捕获输出，避免响应体过大
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(
                output_path, 
                c='copy',  # 沿用 'copy'
            )
            .run(overwrite_output=True) 
        )
        
        logger.info(f"Audio combination successful: {output_path}")

        # 3. Clean up input files
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        # Final check
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        # =======================================================
        # *** CRITICAL FIX: UPLOAD FILE AND RETURN ONLY THE URL ***
        # =======================================================
        logger.info(f"Uploading merged audio file to Cloud Storage: {output_path}")
        
        # 假设 upload_file(local_path, destination_bucket) 返回公共 URL
        # 如果您的 upload_file 需要更多参数，请相应调整。
        # 假设您的 Cloud Storage 目标路径是 'merged_audio/'
        cloud_url = upload_file(output_path, f"merged_audio/{output_filename}")
        
        # 删除本地的最终文件
        os.remove(output_path)
        
        logger.info(f"Successfully uploaded and cleaned up. Returning URL: {cloud_url}")
        
        # 返回公共 URL，而不是本地文件路径
        return cloud_url
        
    except ffmpeg.Error as e:
        logger.error("="*50)
        logger.error("FFmpeg Concatenation Failed! Check Cloud Run logs for raw FFmpeg output.")
        logger.error("="*50)
        raise Exception(f"FFmpeg error during concatenation. Check Cloud Run logs for details.") from e
        
    except Exception as e:
        logger.error(f"Audio combination failed: {str(e)}")
        raise

