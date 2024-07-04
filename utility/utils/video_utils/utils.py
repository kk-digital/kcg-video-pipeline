# utils/video_utils/video_info.py

from typing import Tuple
import yt_dlp
import time
import os

import sys
base_dir = './'
sys.path.insert(0, base_dir)

from utility.utils.url_utils import get_video_short_hash_from_url
from utility.utils.file_utils import get_file_hash

def extract_video_metadata(url: str, format: str, output_dir='output/temp', download = True):
    
    video_short_hash = get_video_short_hash_from_url(url=url)
    url = f"https://www.youtube.com/watch?v={video_short_hash}"
    
    ydl_opts = {
        'outtmpl': f'{output_dir}/{video_short_hash}.%(ext)s',
        'format': format
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
            
            local_filename = "{}".format(video_short_hash)
            file_path = "{}/{}.{}".format(output_dir, local_filename, info["ext"])
            
            # Download video and get file hashif download is True
            download_time = 0
            
            if download and not os.path.isfile(file_path):
            # if download:
                start_time = time.time()
                file_hash = ""
                ydl.download(url_list=[url])
                file_hash = get_file_hash(file_path)
                download_time = time.time() - start_time
                
            video_info = {
                "video_url": url,
                "video_hash": file_hash,
                "video_short_hash": video_short_hash,
                "video_filepath": output_dir,
                "video_filename": local_filename,
                "video_filesize": info.get("filesize"),
                "video_format": info.get("ext"),
                "video_length(s)": info.get("duration"),
                "video_resolution": info.get("resolution"),
                "video_fps": info.get("fps"),
                "m/s": round(info.get("filesize") / download_time, 2) if download_time > 0 else 0,
                "stats-download-time": download_time
            }
            return video_info
        
        except yt_dlp.utils.DownloadError:
            return None