from importlib import metadata
import json
import argparse
from pprint import pprint
import requests
from datetime import datetime
import logging
from functools import wraps
from typing import Callable
from tenacity import after_log, retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from requests.exceptions import ConnectionError, ConnectTimeout
from urllib3.exceptions import NewConnectionError
from tqdm import tqdm

logger = logging.getLogger(__name__)
BATCH_SIZE = 20
RETRY_TIMES = 4
SERVER_ADDRESS = 'http://103.20.60.90:8764'
# SERVER_ADDRESS = 'http://127.0.0.1:8000'

def default_retry(func: Callable) -> Callable:
    @wraps(func)
    @retry(
        retry=retry_if_exception_type((ConnectTimeout, ConnectionError, NewConnectionError)),
        stop=stop_after_attempt(RETRY_TIMES),
        wait=wait_fixed(2),
        after=after_log(logger, logging.WARNING),
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper

@default_retry
def http_add_ingress_video(ingress_video):
    endpoint = SERVER_ADDRESS + "/ingress-videos/add-ingress-video"
    response = None
    
    try:
        response = requests.post(endpoint, json=ingress_video)
        if response.status_code == 200:
            return response.json()["response"]
        else:
            return response.json()
    except Exception as e:
        print("Error in saving ingress-video", ingress_video, e)
    return response

@default_retry
def http_add_ingress_videos(ingress_videos):
    endpoint = SERVER_ADDRESS + "/ingress-videos/add-ingress-video-list"
    response = None
    try:
        response = requests.post(endpoint, json=ingress_videos)
        if response.status_code == 200:
            return response.json()["response"]
        else:
            # print(response.json())
            logger.error("Error in adding ingress-videos, {}".format(response.json()))
    except Exception as e:
        # print("Error", e)
        logger.error(f"Error in saving ingress-video: {str(e)}")
    return response

def save_ingress_video_from_manifest_to_db(manifest_fpath):
    
    with open(manifest_fpath, 'r') as f:
        ingress_videos = json.load(f)
    
    len_ingress_videos = len(ingress_videos)
    for i in tqdm(range(0, len_ingress_videos, BATCH_SIZE), total=len_ingress_videos // BATCH_SIZE + 1):
        batch_ingress_videos = ingress_videos[i:min(i+BATCH_SIZE, len_ingress_videos)]
        
        metadata_list = []
        for ingress_video in batch_ingress_videos:
            # Save video metadata to database here
            ingress_video['file_path'] = f'ingress-video/S_{ingress_video["steam_id"]}/{ingress_video["video_id"]}.{ingress_video["video_extension"]}'

            metadata = {
                "file_hash": ingress_video["file_hash"],
                "file_path": ingress_video["file_path"],
                "video_id": ingress_video["video_id"],
                "video_url": ingress_video["video_url"],
                "video_title": ingress_video["video_title"],
                "video_description": ingress_video["video_description"],
                "video_resolution": "720p",
                "video_extension": ingress_video["video_extension"],
                "video_length": ingress_video["video_length"],
                "video_filesize": ingress_video["video_filesize"] if ingress_video["video_filesize"] else -1,
                "video_frame_rate": ingress_video["video_frame_rate"],
                "video_language": ingress_video["video_language"] if ingress_video["video_language"] else '',
                "processed": False,
                "game_id": ingress_video["steam_id"],
                "upload_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            }
            # http_add_ingress_video(metadata)
            metadata_list.append(metadata)

        http_add_ingress_videos(metadata_list)
        
def main():
    parser = argparse.ArgumentParser(description="Save video metadata")
    parser.add_argument("--path", type=str, default="./manifest.json", help="Manifest file path of ingress video metadata file")
    args = parser.parse_args()
    
    save_ingress_video_from_manifest_to_db(manifest_fpath=args.path)

if __name__ == "__main__":
    main()