import os
import time
import hashlib
from pytube import YouTube
import json

import sys
base_dir = './'

from utility.logger import logger


def download_video(url, output_dir):
    """
    Downloads a video with a specific local filename and saves its metadata to a JSON file.
    """
    yt = YouTube(url)

    # Get video information

    start_time = time.time()  # Record start time

    # Get stream under resolution
    video_length = yt.length
    video_stream = yt.streams.filter(res="720p").first()
    if video_stream is None:
        logger.warning(f'Failed to load video({url})')
        return None
    
    # Get metadata
    video_format = video_stream.subtype
    video_size = video_stream.filesize_mb
    video_resolution = video_stream.resolution
    video_fps = video_stream.fps

    # set local video file name
    video_short_hash = url.split('?v=')[1]
    local_filename = '{}_{}_{}'.format(video_short_hash, video_resolution, video_fps, video_format)

    # get file hash
    file = video_stream.download(output_path=output_dir, filename=f'{local_filename}.{video_format}')
    video_hash = hashlib.md5(file.encode()).hexdigest()
    
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

    
    video_download_info_path = os.path.join(output_dir, os.path.splitext(local_filename)[0] + ".json")
    with open(video_download_info_path, "w") as f:
        json.dump(video_download_info, f, indent=4)

    return video_download_info

def download_videos(video_urls, output_dir = "output/ingress-video"):
    """
    Download videos with video_urls

    Args:
        video_urls (list): list of video urls
        output_dir (str): output directory
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