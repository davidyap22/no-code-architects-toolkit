# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import json
import logging
import os
import uuid
from flask import Blueprint, request, jsonify

# FIX: 从 audio_combiner 导入核心处理函数
from services.audio_combiner import process_audio_concatenation 
# 假设 gcp_toolkit.py 提供了触发 Cloud Run Job 的功能
from services.gcp_toolkit import trigger_cloud_run_job 

logger = logging.getLogger(__name__)

# 使用 job_name 作为 Blueprint name
concatenate_bp = Blueprint('concatenate', __name__)

# --- Cloud Run Job 环境检测 ---
# 用于判断当前代码是在主 Web 服务中运行 (需要触发 Job) 
# 还是在 Cloud Run Job (Batch) 环境中运行 (直接执行处理)
IS_CLOUD_RUN_JOB_ENV = os.getenv('K_SERVICE') and os.getenv('CLOUD_RUN_EXECUTION')


@concatenate_bp.route('/', methods=['POST'])
def concatenate():
    """
    Handles POST requests for concatenating multiple audio files.
    """
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"error": "Invalid JSON body provided."}), 400

        audio_urls = data.get('audio_urls')
        if not audio_urls or not isinstance(audio_urls, list) or len(audio_urls) < 2:
            return jsonify({"error": "The request must contain a list of at least two 'audio_urls'."}), 400

        job_id = str(uuid.uuid4())
        logger.info(f"Received concatenation request for job ID: {job_id}")

        if IS_CLOUD_RUN_JOB_ENV:
            # --- 场景 1: 运行在 Cloud Run Job (Batch) 环境中 ---
            # 直接执行繁重的工作
            logger.info(f"Executing audio combination within job environment: {job_id}")
            result_url = process_audio_concatenation(audio_urls, job_id)
            return jsonify({
                "job_id": job_id,
                "status": "success",
                "output_url": result_url,
                "environment": "job_execution"
            }), 200
        else:
            # --- 场景 2: 运行在 Cloud Run Service (Web) 环境中 ---
            # 触发一个新的 Cloud Run Job 进行后台处理（推荐用于长任务）
            
            # 确保 CLOUD_RUN_JOB_NAME 环境变量已设置，且名称匹配您的 Job
            CLOUD_RUN_JOB_NAME = os.environ.get('CLOUD_RUN_JOB_NAME', 'your-ffmpeg-job-name')

            # 将输入数据作为环境变量传递给 Job
            overrides = {
                "container_overrides": [
                    {
                        "env": [
                            {"name": "JOB_INPUT_DATA", "value": json.dumps(data)},
                            {"name": "JOB_TYPE", "value": "concatenate"}
                        ]
                    }
                ]
            }
            
            # 触发 Cloud Run Job
            trigger_result = trigger_cloud_run_job(CLOUD_RUN_JOB_NAME, overrides=overrides)

            if trigger_result.get("job_submitted"):
                return jsonify({
                    "job_id": job_id,
                    "status": "processing_started",
                    "message": f"Cloud Run Job '{trigger_result['execution_name']}' initiated for background processing.",
                    "environment": "service_trigger"
                }), 202
            else:
                logger.error(f"Failed to trigger Cloud Run Job: {trigger_result.get('error')}")
                return jsonify({"error": "Failed to initiate background processing.", "details": trigger_result.get('error')}), 500

    except Exception as e:
        logger.error(f"Error during audio concatenation request: {e}", exc_info=True)
        return jsonify({"error": "Internal server error during processing request."}), 500
