import os
import hashlib

import time

import json
from typing import Tuple

from pytube import YouTube

import sys
base_dir = './'
sys.path.insert(0, base_dir)

from utility.logger import logger


def download_video(url, 
                   output_dir="output/ingress-video", 
                   res="720p", 
                   fps=16):
    """
    Downloads a video with a specific local filename and saves its metadata to a JSON file.
    """
    yt = YouTube(url)

    # Get video information

    start_time = time.time()  # Record start time

    # Get stream under resolution
    video_length = yt.length
    video_stream = yt.streams.filter(res=res, fps=fps).first()
    if video_stream is None:
        logger.warning(f'Video download failed due to constraints \
                       in {res} resolution and {fps}fps, network error, \
                       or invalid URL({url}).')
        return None
    
    # Get metadata
    video_format = video_stream.subtype
    video_size = video_stream.filesize_mb
    video_resolution = video_stream.resolution
    video_fps = video_stream.fps

    # set local video file name
    video_short_hash = url.split('?v=')[1]
    local_filename = '{}_{}_{}'.format(video_short_hash, 
                                       video_resolution, 
                                       video_fps, 
                                       video_format)

    # get file hash
    try:
        file = video_stream.download(output_path=output_dir, 
                                     filename=f'{local_filename}.{video_format}')
        video_hash = hashlib.md5(file.encode()).hexdigest()
    except Exception as e:
        logger.error("Downloading video failed(url = {url}) -> " + str(e))
        return None
    
    end_time = time.time()  # Record end time

    download_time = (end_time - start_time)  # Calculate download time
    # Save video_download_info to a JSON file
    video_download_info = {
        "video_hash": video_hash,
        "video_url": url,
        "video_filename": local_filename,
        "video_filepath": output_dir,
        "video_length(s)": video_length,
        "video_format": video_format,
        "video_size(mb)": round(video_size, 2),
        "video_resolution": video_resolution,
        "video_fps": video_fps,
        "m/s": round(video_size / download_time, 2),
        "stats-download-time": round(download_time, 2),
    }

    video_download_info_path = os.path.join(output_dir, 
                                            os.path.splitext(local_filename)[0] + ".json")
    with open(video_download_info_path, "w") as f:
        json.dump(video_download_info, f, indent=4)

    return video_download_info

def download_videos(video_urls, output_dir = "output/ingress-video"):
    """
    Download videos with video_urls

    Args:
        video_urls (list): list of video urls
        output_dir (str): output directory, default dir is output/ingress-video
    Returns:
        video_meta_data_list (list): list of video metadata
    """    
    video_meta_data_list = []
    failed_video_urls = []

    for url in video_urls:

        video_meta_data = download_video(url, output_dir)
        if video_meta_data is None:
            failed_video_urls.append(url)
        else:
            video_meta_data_list.append(video_meta_data)
    
    with open(f'{output_dir}/failed_urls.json', 'w') as f:
        json.dump(failed_video_urls, f, indent=1)

    return video_meta_data_list

def get_resolution_dimensions(resolution: str) -> Tuple[int, int]:
    """
    Extracts the width and height from a video resolution string.
    
    Args:
        resolution (str): The video resolution string (e.g., "1920x1080", "760p", "480p").
    
    Returns:
        Tuple[int, int]: The width and height of the video resolution.
    """
    if "x" in resolution:
        # Resolution is in the format "width x height"
        width, height = resolution.split("x")
        return int(width), int(height)
    elif resolution.endswith("p"):
        # Resolution is in the format "number"p
        height = int(resolution[:-1])
        if height == 2160:
            return 3840, 2160  # 4K resolution
        elif height == 1440:
            return 2560, 1440  # 2K resolution
        elif height == 1080:
            return 1920, 1080  # 1080p resolution
        elif height == 720:
            return 1280, 720   # 720p resolution
        elif height == 480:
            return 640, 480    # 480p resolution
        elif height == 360:
            return 640, 360    # 360p resolution
        elif height == 240:
            return 426, 240    # 240p resolution
        elif height == 144:
            return 256, 144    # 144p resolution
        else:
            return None, None  # Unknown resolution
    else:
        return None, None      # Unknown format
    
