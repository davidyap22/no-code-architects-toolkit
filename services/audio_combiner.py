# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import subprocess
import logging
import uuid
from typing import List, Tuple
# FIX: 导入 services/file_management.py 中的下载和清理函数
from services.file_management import download_file, cleanup_files 
from services.cloud_storage import upload_file
# 假设 config.py 中定义了 LOCAL_STORAGE_PATH
from config import LOCAL_STORAGE_PATH 

# Set up logging
logger = logging.getLogger(__name__)

def process_audio_concatenation(audio_urls: List[str], job_id: str) -> str:
    """
    Downloads multiple audio files, combines them using FFmpeg, uploads the result 
    to cloud storage, and cleans up local temporary files.

    Args:
        audio_urls (List[str]): List of public URLs for the audio files.
        job_id (str): A unique ID for the processing job (用于文件命名).

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
            # download_file 现在使用 file_management 中的 UUID 逻辑来确保本地路径唯一性
            local_path = download_file(url, LOCAL_STORAGE_PATH)
            downloaded_files.append(local_path)
            logger.info(f"Downloaded file: {url} to {local_path}")

        # 2. Create the FFmpeg concat list file
        concat_list_path = os.path.join(LOCAL_STORAGE_PATH, f"concat_list_{job_id}.txt")
        # NOTE: 写入文件名时，我们只使用 basename，因为 FFmpeg 将在 LOCAL_STORAGE_PATH 目录下执行 (见步骤 3)
        with open(concat_list_path, 'w') as f:
            for file_path in downloaded_files:
                f.write(f"file '{os.path.basename(file_path)}'\n")
        
        # 将 concat list 文件添加到清理列表
        downloaded_files.append(concat_list_path)

        # 3. Perform FFmpeg concatenation
        output_filename = f"{job_id}_merged.mp3"
        output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)
        
        cmd = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', os.path.basename(concat_list_path), # 输入文件使用 basename
            '-c', 'copy',
            os.path.basename(output_path) # 输出文件也使用 basename
        ]
        
        logger.info(f"Running FFmpeg command for merge: {' '.join(cmd)}")
        
        # 在 LOCAL_STORAGE_PATH 目录下执行 FFmpeg 命令
        process = subprocess.run(
            cmd, 
            cwd=LOCAL_STORAGE_PATH, 
            capture_output=True, 
            text=True
        )
        
        if process.returncode != 0:
            logger.error(f"FFmpeg Merge Error: {process.stderr}")
            raise Exception(f"FFmpeg concatenation failed: {process.stderr}")
        
        logger.info(f"Successfully created merged audio: {output_path}")

        # 4. Upload the result to cloud storage
        # 目标云存储路径 (e.g., "merged_audio/job_id_merged.mp3")
        destination_path = f"merged_audio/{output_filename}"

        # FIX: 现在正确地使用 2 个必需参数调用 upload_file
        cloud_url = upload_file(output_path, destination_path)
        
        logger.info(f"Final merged audio uploaded to: {cloud_url}")
        
        return cloud_url
        
    except Exception as e:
        logger.error(f"Audio concatenation failed for job {job_id}: {str(e)}")
        raise
        
    finally:
        # 5. Clean up all temporary files (输入文件、列表文件和最终输出文件)
        # 确保 output_path 被添加到清理列表（如果在 try 块内创建成功）
        if output_path and os.path.exists(output_path):
            downloaded_files.append(output_path)
            
        # 调用 cleanup_files 函数进行安全清理
        cleanup_files(downloaded_files)
        logger.info(f"Cleaned up {len(downloaded_files)} temporary files for job {job_id}.")
```eof

请保存这个文件，然后告诉我下一步你想看哪个文件的代码！
