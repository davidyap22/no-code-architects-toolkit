# Copyright (c) 2025 Stephen G. Pope
# (License header omitted for brevity)

import os
import ffmpeg
# Ensure you have necessary imports
# from services.file_management import download_file 
# from config import LOCAL_STORAGE_PATH

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
        print("Starting download of input audio files...")
        for i, media_item in enumerate(media_urls):
            # Assuming media_item['audio_url'] is the correct key based on your usage
            url = media_item['audio_url']
            # Download file using a temporary, simple name to reduce path complexity during operation
            temp_filename_local = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_input_{i}.mp3")
            
            # Assuming download_file returns the path to the downloaded file
            # If download_file handles the full path, this line is fine:
            input_filename = download_file(url, temp_filename_local)
            input_files.append(input_filename)
            print(f"Downloaded: {url} to {input_filename}")

        # 1. Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(LOCAL_STORAGE_PATH, f"{job_id}_concat_list.txt")
        print(f"Creating concat list file: {concat_file_path}")

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
        print("Starting FFmpeg concatenation...")
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
        print(f"Audio combination successful: {output_path}")

        # 3. Clean up input files
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)

        # Final check
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        return output_path
        
    except ffmpeg.Error as e:
        print("="*50)
        print("FFmpeg Concatenation Failed!")
        print(f"Stdout:\n{e.stdout.decode('utf8')}")
        print(f"Stderr:\n{e.stderr.decode('utf8')}")
        print("="*50)
        raise Exception(f"FFmpeg error during concatenation. See logs for details.") from e
        
    except Exception as e:
        print(f"Audio combination failed: {str(e)}")
        raise
