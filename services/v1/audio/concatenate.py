# routes/v1/audio/concatenate.py
# 实现了 GCS 下载、pydub 合并和 GCS 上传功能。
# 必须安装: pydub, requests, google-cloud-storage

from flask import Blueprint
from app import app # 引入主应用实例以访问 queue_task
import time
import os
import io
import requests
from pydub import AudioSegment
from google.cloud import storage

# --- 配置项 ---
# 请设置您的 GCS 存储桶名称，用于存放最终合并的音频文件。
RESULT_GCS_BUCKET_NAME = os.environ.get("RESULT_GCS_BUCKET_NAME", "your-default-merged-audio-bucket")
# ---

# 初始化 GCS 客户端
# 客户端初始化放在外面，避免在每次请求时重复创建
try:
    gcs_client = storage.Client()
except Exception as e:
    # 打印警告，但允许 Worker 继续运行 (假设在具有默认凭证的环境中)
    print(f"Warning: GCS Client initialization failed. Check credentials: {e}")
    gcs_client = None

# 创建蓝图 (Blueprint)
audio_bp = Blueprint('audio', __name__, url_prefix='/v1/audio')


def _download_gcs_file_to_memory(gcs_url):
    """从 GCS URL (gs:// 或 https://) 下载文件内容到内存流。"""
    if gcs_client is None:
        raise Exception("GCS Client is not initialized. Check authentication.")

    # 尝试从 URL 中解析出桶和对象名称
    if gcs_url.startswith("gs://"):
        parts = gcs_url[5:].split('/', 1)
        bucket_name = parts[0]
        blob_name = parts[1]
        
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        file_bytes = blob.download_as_bytes()
        return io.BytesIO(file_bytes)
        
    elif "storage.googleapis.com" in gcs_url:
        # 针对公开的 HTTP 链接，使用 requests
        response = requests.get(gcs_url, stream=True)
        response.raise_for_status()
        return io.BytesIO(response.content)
    else:
        raise ValueError(f"Unsupported URL format: {gcs_url}. Must be gs:// or storage.googleapis.com link.")


def _upload_file_from_memory(data_stream, destination_blob_name, content_type="audio/mp3"):
    """将内存流中的内容上传到目标 GCS 桶。"""
    if gcs_client is None:
        raise Exception("GCS Client is not initialized. Check authentication.")
        
    bucket = gcs_client.bucket(RESULT_GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    
    # 将流的指针重置到开头
    data_stream.seek(0)
    
    # 上传文件
    blob.upload_from_file(data_stream, content_type=content_type)
    
    # 返回文件的公共访问 URL (如果桶设置允许)
    return blob.public_url


@audio_bp.route('/concatenate', methods=['POST'])
@app.queue_task() # 使用 app.queue_task 装饰器，实现异步执行和 Webhook 回调
def concatenate_audio_route(job_id=None, data=None):
    """
    处理异步任务：合并 'audio_urls' 列表中的音频文件。
    """
    endpoint = "/v1/audio/concatenate"

    # 1. 输入校验
    if not data or 'audio_urls' not in data:
        return {"message": "Missing 'audio_urls' in request body."}, endpoint, 400
    
    audio_urls = data.get('audio_urls', [])
    if not isinstance(audio_urls, list) or not audio_urls:
        return {"message": "'audio_urls' must be a non-empty list of GCS links."}, endpoint, 400
    if gcs_client is None:
        return {"message": "Server Error: GCS credentials or client failed to initialize."}, endpoint, 500

    print(f"--- Job ID: {job_id}. Starting merge of {len(audio_urls)} clips. ---")
    
    # 初始化一个空的 AudioSegment
    final_combined_audio = AudioSegment.empty()
    
    # 2. 核心合并逻辑
    try:
        for i, url in enumerate(audio_urls):
            print(f"Downloading clip {i+1}/{len(audio_urls)}.")
            
            # 下载 GCS 文件内容到内存流
            audio_stream = _download_gcs_file_to_memory(url)
            
            # 使用 pydub 加载 (pydub 会自动检测音频格式)
            clip = AudioSegment.from_file(audio_stream)
            
            # 顺序追加音频段
            final_combined_audio += clip
            
            print(f"Clip {i+1} merged. Current duration: {len(final_combined_audio) / 1000.0:.2f} seconds.")

        # 3. 导出最终合并的音频到内存
        output_buffer = io.BytesIO()
        # 导出为 MP3 格式，参数 "-q:a 0" 表示最高质量 VBR MP3 (需要 FFmpeg)
        final_combined_audio.export(output_buffer, format="mp3", parameters=["-q:a", "0"])
        
        # 4. 上传最终结果到 GCS
        unique_id = data.get("id", job_id)
        destination_blob_name = f"merged/result_{unique_id}_{int(time.time())}.mp3"
        final_url = _upload_file_from_memory(output_buffer, destination_blob_name)
        
        print(f"--- Audio concatenation successful. Final URL: {final_url} ---")
        
        # 5. 成功响应 (通过 Webhook 发送)
        return {
            "final_url": final_url,
            "clips_merged": len(audio_urls),
            "total_duration_seconds": len(final_combined_audio) / 1000.0,
            "message": "Audio concatenation complete."
        }, endpoint, 200

    except Exception as e:
        error_msg = f"Audio processing failed: {str(e)}"
        print(f"ERROR: {error_msg}")
        return {
            "message": "Audio processing failed.",
            "error_details": error_msg,
            "job_id": job_id
        }, endpoint, 500

