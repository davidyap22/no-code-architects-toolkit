import os
import ffmpeg
from services.file_management import download_file
from config import LOCAL_STORAGE_PATH

def _has_audio(path: str) -> bool:
    """Check if file has at least one audio stream."""
    probe = ffmpeg.probe(path)
    for s in probe.get("streams", []):
        if s.get("codec_type") == "audio":
            return True
    return False

def _duration_seconds(path: str) -> float:
    """Get duration in seconds from format or streams."""
    probe = ffmpeg.probe(path)
    # prefer stream duration if present
    for s in probe.get("streams", []):
        if "duration" in s:
            try:
                return float(s["duration"])
            except Exception:
                pass
    try:
        return float(probe.get("format", {}).get("duration", 0.0))
    except Exception:
        return 0.0

def process_video_concatenate(media_urls, job_id, webhook_url=None):
    """
    稳定拼接方案：
    1) 下载所有输入
    2) 对无音轨的视频补静音音轨
    3) 统一音频参数（48kHz/stereo/对齐PTS）
    4) 使用 concat filter (v=1,a=1) 拼接，避免 demuxer + -c copy 的错位问题
    """
    input_files = []
    tmp_with_audio = []   # 补齐音轨后的中间件
    tmp_normalized = []   # 统一音频参数后的中间件

    output_filename = f"{job_id}.mp4"
    output_path = os.path.join(LOCAL_STORAGE_PATH, output_filename)

    try:
        # 1) 下载所有媒体
        for i, media_item in enumerate(media_urls):
            url = media_item["video_url"]
            dl_path = download_file(url, os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}"))
            input_files.append(dl_path)

        # 2) 确保每段都有音轨（没有则补静音）
        for i, ipath in enumerate(input_files):
            out_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_withaudio_{i}.mp4")
            if _has_audio(ipath):
                # 直接复制（容器重封装），避免复用原路径
                (
                    ffmpeg
                    .input(ipath)
                    .output(out_path, c="copy")
                    .overwrite_output()
                    .run(quiet=True)
                )
            else:
                # 生成与视频等长的静音音轨并与视频合并
                dur = _duration_seconds(ipath) or 0.001
                # anullsrc -> aac 48k stereo；映射到视频上；音频做基础对齐
                (
                    ffmpeg
                    .input(ipath)  # 0: 有画面无声
                    .input("anullsrc=r=48000:cl=stereo", f="lavfi", t=dur)  # 1: 静音音轨
                    .output(
                        out_path,
                        map="0:v:0",
                        **{
                            "map:1:a:0": None,  # 显式映射音轨
                        },
                        c_v="copy",
                        af="aresample=async=1:min_hard_comp=0.100:first_pts=0,"
                           "aformat=sample_fmts=s16:sample_rates=48000:channel_layouts=stereo",
                        c_a="aac", b_a="192k"
                    )
                    .overwrite_output()
                    .run(quiet=True)
                )
            tmp_with_audio.append(out_path)

        # 3) 统一音频参数（48kHz/stereo，对齐PTS；画面尽量不转码）
        for i, ipath in enumerate(tmp_with_audio):
            norm_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_norm_{i}.mp4")
            (
                ffmpeg
                .input(ipath)
                .output(
                    norm_path,
                    c_v="copy",
                    af="aresample=async=1:min_hard_comp=0.100:first_pts=0,"
                       "aformat=sample_fmts=s16:sample_rates=48000:channel_layouts=stereo",
                    c_a="aac", b_a="192k"
                )
                .overwrite_output()
                .run(quiet=True)
            )
            tmp_normalized.append(norm_path)

        # 4) concat filter（严格 v=1,a=1）
        inputs = [ffmpeg.input(p) for p in tmp_normalized]
        # 展开成 v1,a1,v2,a2,... 的顺序供 concat
        streams = []
        for inp in inputs:
            streams.extend([inp.video, inp.audio])

        concat = ffmpeg.concat(*streams, v=1, a=1)  # 返回 (v, a)
        vout = concat[0]
        aout = concat[1]

        (
            ffmpeg
            .output(
                vout, aout, output_path,
                c_v="libx264", preset="veryfast", crf=18,
                c_a="aac", b_a="192k"
            )
            .overwrite_output()
            .run(quiet=True)
        )

        # 5) 清理中间文件
        for f in input_files + tmp_with_audio + tmp_normalized:
            try:
                os.remove(f)
            except OSError:
                pass

        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        print(f"Video combination successful: {output_path}")
        return output_path

    except Exception as e:
        print(f"Video combination failed: {str(e)}")
        # 失败也尽量清理
        for f in input_files + tmp_with_audio + tmp_normalized:
            try:
                os.remove(f)
            except OSError:
                pass
        raise


    except Exception as e:
        print(f"❌ Video combination failed: {str(e)}")
        raise
