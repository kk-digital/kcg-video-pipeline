import json
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

import torch

import sys

base_dir = './'
sys.path.insert(0, base_dir)

from config import MINIO_ACCESS_KEY, MINIO_ADDRESS, MINIO_SECRET_KEY

from schema import VideoMetaData
from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder
from pipelines import VideoProcessingPipeline
from utility.minio import cmd
from utility.http.request import http_get_unprocessed_videos
from utility import logger

def run_pipeline(minio_client, image_encoder, video: VideoMetaData) -> bool:
    pipeline = VideoProcessingPipeline(minio_client=minio_client, image_encoder=image_encoder, video=video)
    return pipeline.run(), pipeline.get_uploaded_image_count()

def run_video_processing(minio_client, 
                        image_encoder,
                        videos: List[VideoMetaData], 
                        batch_size: int = 1,
                        max_workers:int = 8) -> List[str]:
    futures = []
    failed_video_info_list = []
    len_videos = len(videos)
    logger.info("video processing...")
    total_uploaded_image_count = 0
    for index in range(0, len_videos, batch_size):
        batch_videos = videos[index:min(index+batch_size, len_videos)]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for video in batch_videos:
                futures.append(executor.submit(run_pipeline, minio_client, image_encoder, video))

            for future in as_completed(futures):
                is_success, url, uploaded_image_count = future.result()
                total_uploaded_image_count += uploaded_image_count
                logger.critical(f"Uploaded images count: {total_uploaded_image_count}")
                if not is_success:
                    failed_video_info_list.append(url)
        logger.info(f"{index + 1}/{len_videos} videos processed")

    return failed_video_info_list


if __name__ == '__main__':

    # Load minio client using access and secret key, minio ip address
    minio_client = cmd.get_minio_client(minio_access_key=MINIO_ACCESS_KEY,
                                        minio_secret_key=MINIO_SECRET_KEY,
                                        minio_ip_addr=MINIO_ADDRESS)
    
    print('Loading image-process-encoder model')
    encoder = KandinskyCLIPImageEncoder(device= 'cuda' if torch.cuda.is_available() else 'cpu')
    encoder.load_submodels()
    print('Successfully loaded the model')
    
    # Get the hash list of unprocessed ingress video
    videos = http_get_unprocessed_videos()
    failed_video_list = run_video_processing(minio_client=minio_client, 
                            image_encoder=encoder,
                            videos=videos)

    # Save list of failed URLs in pipeline in json format
    with open(file='failed_list.json', mode='w') as f:
        json.dump(obj=failed_video_list, fp=f, indent=4)