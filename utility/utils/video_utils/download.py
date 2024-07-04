# utils/video_utils/download.py

import time
import json
import os
import hashlib
import yt_dlp
from pytube import YouTube

import sys
sys.path.insert(0, './')

from .utils import extract_video_metadata

def download_video(url, 
                   output_dir="output/ingress-video"):
    
    return download_video_yt_dlp(url=url,
                                output_dir=output_dir)

def download_video_yt_dlp(url, 
                   output_dir="output/ingress-video", 
                   format = 'bv[height=720][fps=60]/bv[height=720][fps=30]',
                   download = True):
    
    video_download_info = extract_video_metadata(url=url,
                                         format=format,
                                         output_dir=output_dir,
                                         download=download)
    if video_download_info is None:
        raise Exception("Failed to download video, {}".format(url))
    
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