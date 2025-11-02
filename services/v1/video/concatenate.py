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
import math
from services.file_management import download_file
from config import LOCAL_STORAGE_PATH

def _probe(path):
    return ffmpeg.probe(path)

def _get_video_params(path):
    """返回 (width, height, fps)"""
    info = _probe(path)
    v = next(s for s in info['streams'] if s['codec_type'] == 'video')
    w, h = int(v['width']), int(v['height'])
    # 解析帧率
    fr = v.get('r_frame_rate', '25/1')
    num, den = fr.split('/')
    fps = float(num) / float(den) if float(den) != 0 else 25.0
    return w, h, fps

def _has_audio(path):
    info = _probe(path)
    return any(s for s in info['streams'] if s['codec_type'] == 'audio')

def _duration(path):
    info = _probe(path)
    return float(info['format']['duration'])

def _round_fps(fps):
    # 将 fps 归一到常见的整数（25/30）以避免时基不一致
    if abs(fps - 30.0) < 1.0: return 30
    if abs(fps - 25.0) < 1.0: return 25
    return int(round(fps))

def _normalize_one(
    in_path: str,
    out_path: str,
    target_w: int,
    target_h: int,
    target_fps: int
):
    """
    规范化单个输入：
    - 视频：libx264, yuv420p, scale 到 (target_w,target_h), fps=target_fps (CFR)
    - 音频：如果存在 → 转 AAC 48k 2ch；如果不存在 → anullsrc 生成静音 AAC
    - 统一时间戳，避免负 ts
    """
    has_aud = _has_audio(in_path)

    v_in = ffmpeg.input(in_path)
    v = (
        v_in.video
        .filter('scale', target_w, target_h)
        .filter('fps', fps=target_fps, round='up')
        .filter('settb', '1/90000')
        .filter('setpts', 'PTS-STARTPTS')
    )

    if has_aud:
        a = (
            v_in.audio
            .filter('aresample', 48000)
            .filter('asetpts', 'PTS-STARTPTS')
        )
        out = ffmpeg.output(
            v, a, out_path,
            vcodec='libx264', pix_fmt='yuv420p',
            r=target_fps,
            acodec='aac', ar='48000', ac=2, audio_bitrate='192k',
            movflags='+faststart',
            **{'fflags': '+genpts', 'avoid_negative_ts': 'make_zero'}
        )
    else:
        # 生成完全静音但“有波形”的 AAC：anullsrc
        silent = ffmpeg.input('anullsrc=r=48000:cl=stereo', f='lavfi')
        out = ffmpeg.output(
            v, silent, out_path,
            vcodec='libx264', pix_fmt='yuv420p',
            r=target_fps,
            acodec='aac', ar='48000', ac=2, audio_bitrate='192k',
            shortest=None,  # 以更短者结束；这里保证音频不会比视频更长
            movflags='+faststart',
            **{'fflags': '+genpts', 'avoid_negative_ts': 'make_zero'}
        ).global_args('-map', '0:v:0', '-map', '1:a:0')

    # 执行
    out.overwrite_output().run(quiet=False)

def process_video_concatenate(media_urls, job_id, webhook_url=None):
    """
    将多个视频合并：
    1) 全部下载
    2) 用首个视频的分辨率 & fps 作为目标参数，逐个做规范化（保证每段都有 AAC 音轨）
    3) 用 concat demuxer + copy 拼接
    """
    input_files = []
    norm_files = []
    output_filename = f"{job_id}.mp4"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    try:
        # 1) 下载
        for i, media_item in enumerate(media_urls):
            url = media_item['video_url']
            tmp = download_file(url, os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}"))
            input_files.append(tmp)

        if not input_files:
            raise RuntimeError("No input files downloaded.")

        # 2) 以首个视频作为基准，确定目标分辨率 & fps
        base_w, base_h, base_fps = _get_video_params(input_files[0])
        target_w, target_h = base_w, base_h
        target_fps = _round_fps(base_fps)

        # 3) 逐个规范化，确保都有音轨、编码一致
        for idx, src in enumerate(input_files):
            norm = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_norm_{idx}.mp4")
            _normalize_one(src, norm, target_w, target_h, target_fps)
            norm_files.append(norm)

        # 4) 写 concat list（绝对路径）
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        with open(concat_file_path, 'w') as f:
            for p in norm_files:
                f.write(f"file '{os.path.abspath(p)}'\n")

        # 5) concat demuxer + copy（高速、无损）
        (
            ffmpeg
            .input(concat_file_path, format='concat', safe=0)
            .output(output_path, c='copy', movflags='+faststart')
            .overwrite_output()
            .run(quiet=False)
        )

        # 6) 清理
        for p in input_files:
            if os.path.exists(p): os.remove(p)
        for p in norm_files:
            if os.path.exists(p): os.remove(p)
        if os.path.exists(concat_file_path): os.remove(concat_file_path)

        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        print(f"✅ Video combination successful: {output_path}")
        return output_path

    except Exception as e:
        print(f"❌ Video combination failed: {str(e)}")
        raise
