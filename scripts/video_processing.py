import json
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

import torch

import sys
base_dir = './'
sys.path.insert(0, base_dir)

from config import MINIO_ACCESS_KEY, MINIO_ADDRESS, MINIO_SECRET_KEY

from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder
from pipelines import VideoProcessingPipeline
from utility.minio import cmd

# define class for video information
class VideoInfo:
    def __init__(self, key: str, value: str, dataset: str) -> None:
        self.key = key # video_url or video_hash
        self.value = value
        self.dataset = dataset

def run_pipeline_with_url(minio_client, image_encoder, video_info: VideoInfo) -> bool:
    pipeline = VideoProcessingPipeline(minio_client=minio_client, image_encoder=image_encoder, dataset_name=video_info.dataset)
    return pipeline.run_pipeline_with_url(video_info.value)

def run_pipeline_with_hash(minio_client, image_encoder, video_info: VideoInfo) -> bool:
    pipeline = VideoProcessingPipeline(minio_client=minio_client, image_encoder=image_encoder, dataset_name=video_info.dataset)
    return pipeline.run_pipeline_with_video_hash(video_info.value)

def run_video_processing(minio_client, 
                         image_encoder,
                         video_info_list:List[VideoInfo], 
                         max_workers:int = 8) -> List[str]:
    futures = []
    failed_video_info_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for video_info in video_info_list:
            if video_info.key == "video_url":
                futures.append(executor.submit(run_pipeline_with_url, 
                                                        minio_client, 
                                                        image_encoder,
                                                        video_info))
            elif video_info.key == "video_hash":
                futures.append(executor.submit(run_pipeline_with_hash, 
                                               image_encoder,
                                               minio_client, 
                                               video_info))
                
        for future in as_completed(futures):
            is_success, url = future.result()
            if not is_success:
                failed_video_info_list.append(url)
    
    return failed_video_info_list

def get_video_info_list(csv_fname: str) -> List[VideoInfo]:
    import pandas as pd

    df = pd.read_csv(csv_fname)
    video_info_list = []
    
    for _, row in df.iterrows():
        video_info_list.append(VideoInfo(key="video_url", 
                                        value=row['Video Url'],
                                        dataset="test0002"))
                                        # dataset=row['Dataset']))
    
    return video_info_list

if __name__ == '__main__':
    # TODO: add preprocessing to get video_info
    video_info_list = []
    video_info_list.append(VideoInfo(key="video_url", 
                                     value="https://www.youtube.com/watch?v=kk7XWcIH2BA",
                                     dataset="test0001"))
    # Get video info list from csv file
    # video_info_list = get_video_info_list("video-data-list.csv")
    # Load minio client using access and secret key, minio ip address
    minio_client = cmd.get_minio_client(minio_access_key=MINIO_ACCESS_KEY,
                                        minio_secret_key=MINIO_SECRET_KEY,
                                        minio_ip_addr=MINIO_ADDRESS)
    
    print('Loading image-process-encoder model')
    encoder = KandinskyCLIPImageEncoder(device= 'cuda' if torch.cuda.is_available() else 'cpu')
    encoder.load_submodels()
    print('Successfully loaded the model')
    
    failed_video_list = run_video_processing(minio_client=minio_client, 
                             image_encoder=encoder,
                             video_info_list=video_info_list)
    
    # Save list of failed URLs in pipeline in json format
    with open(file='failed_list.json', mode='w') as f:
        json.dump(obj=failed_video_list, fp=f, indent=4)