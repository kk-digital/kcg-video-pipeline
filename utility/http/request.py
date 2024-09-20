import requests

from typing import Optional, List, Dict
import json

import time

import sys

base_dir = './'
sys.path.insert(0, base_dir)

# Import schema
from schema import VideoMetaData
# Import logger
from utility import logger

# from config import ORCHESTRATION_ADDRESS as SERVER_ADDRESS
MAX_RETRY = 6
DELAY_TIME = 2

from config import ORCHESTRATION_ADDRESS as SERVER_ADDRESS
# SERVER_ADDRESS = "http://localhost:8000"

def http_wrapper(http_request, retry = 0, *args, **wargs):
    try:
        return http_request(*args, **wargs)
    except ConnectionRefusedError as e:
        if retry < MAX_RETRY:
            retry_delay_time = DELAY_TIME ** retry
            time.sleep(retry_delay_time)

            logger.warning(msg=f"{retry + 1}th retrying( http_{http_request.__name__} )... ")
            http_wrapper(http_request=http_request, retry = retry + 1, *args, **wargs)
        else:
            logger.error(msg=f"http_{http_request.__name__} {str(e)}")
            return None
    except Exception as e:
        logger.error(msg=f"http_{http_request.__name__} {str(e)}")
        return None
    
def http_add_video(video_metadata) -> Optional[VideoMetaData]:

    def add_video(video_metadata) -> Optional[VideoMetaData]:
        url = f'{SERVER_ADDRESS}/ingress-videos/add-ingress-video'
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url=url, json=video_metadata, headers=headers)

        video_metadata = None
        if response.status_code == 200:
            video_metadata_json = response.json()['response']
            video_metadata = VideoMetaData.deserialize(data=video_metadata_json)
        else:
            logger.error(
                msg=f"Uploading video metadata failed -> {json.dumps(obj=response.json())}"
            )
        if response:
            response.close()
            
        return video_metadata
    
    return http_wrapper(http_request=add_video, video_metadata=video_metadata)

def http_add_image_list(image_data_list) -> Optional[List[Dict]]:

    def add_image_list(image_data_list) -> Optional[List[Dict]]:
        url = f'{SERVER_ADDRESS}/external-images/add-external-image-list'
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url, json=image_data_list, headers=headers)
        result = None
        if response.status_code == 200:
            result = response.json()['response']['data']
        else:
            logger.error(msg="Adding image metadata list into failed! -> {}".format(response.text))
        if response:
            response.close()
            
        return result
    
    return http_wrapper(http_request=add_image_list, image_data_list=image_data_list)

def http_add_image(image_data) -> Optional[Dict]:
    def add_image(image_data):
        url = f'{SERVER_ADDRESS}/external-images/add-external-image'
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url=url, json=image_data, headers=headers)
        result = None
        if response.status_code == 200:
            result = response.json()['response']
        elif response.status_code == 422:
            logger.debug(msg="Error: http_add_image - {}".format(response.text))
        else:
            logger.error(msg="Adding image metadata into failed! -> {}".format(response.text))
        if response:
            response.close()
            
        return result
    
    return http_wrapper(http_request=add_image, image_data=image_data)

def http_get_video_metadata(video_hash) -> Optional[VideoMetaData]:

    def get_video_metadata(video_hash) -> Optional[VideoMetaData]:
        url = f'{SERVER_ADDRESS}/ingress-videos/get-ingress-video-by-video-id?video_hash={video_hash}'
        response = requests.get(url=url)

        video_metadata = None
        if response.status_code == 200:
            video_metadata_json = response.json()['response']['data']
            video_metadata = VideoMetaData.deserialize(data=video_metadata_json)
            return video_metadata
        else:
            logger.error(
                msg=f"Getting video metadata from mongodb failed -> \
                    {json.dumps(obj=response.json())}"
            )
        if response:
            response.close()
                
        return video_metadata
    
    return http_wrapper(http_request=get_video_metadata, video_hash=video_hash)

def http_get_unprocessed_videos() -> Optional[List[VideoMetaData]]:
    def get_unprocessed_videos():
        url = f'{SERVER_ADDRESS}/ingress-videos/list-unprocessed-list'
        response = requests.get(url=url)
        
        video_metadata = None
        if response.status_code == 200:
            metadata_list = response.json()['response']['data']
            # Deserialize each video metadata item into a VideoMetaData object
            return [VideoMetaData.deserialize(item) for item in metadata_list]
        else:
            logger.error(
                msg=f"Getting all video metadata from mongodb failed -> \
                    {json.dumps(obj=response.json())}"
            )
        if response:
            response.close()
                
        return video_metadata
    
    return http_wrapper(http_request=get_unprocessed_videos)


def http_get_video_game(game_id):
    url = f'{SERVER_ADDRESS}/video-games/get-video-game-by-game-id?game_id={game_id}'
    response = requests.get(url=url)

    video_game = None
    if response.status_code == 200:
        video_game = response.json()['response']
    else:
        logger.error(
            msg=f"Getting video game data from mongodb failed -> \
                {json.dumps(obj=response.json())}"
        )
    if response:
        response.close()

    return video_game

def http_update_video_status_to_processed(video: VideoMetaData):
    url = f'{SERVER_ADDRESS}/ingress-videos/update-ingress-video'
    response = requests.put(url=url, json=video.serialize())
    headers = {'Content-Type': 'application/json'}
    
    video_game = None
    if response.status_code == 200:
        video_game = response.json()['response']
    else:
        logger.error(
            msg=f"Getting video game data from mongodb failed -> \
                {json.dumps(obj=response.json())}"
        )
    if response:
        response.close()

    return video_game

def http_ingress_video_by_video_hash(video_id) -> Optional[VideoMetaData]:
    url = f'{SERVER_ADDRESS}/ingress-videos/get-ingress-video-by-video-id?video_id={video_id}'
    response = requests.get(url=url)

    video_game = None
    if response.status_code == 200:
        video_game = VideoMetaData.deserialize(response.json()['response'])
    else:
        logger.error(
            msg=f"Getting ingress video data from mongodb failed -> \
                {json.dumps(obj=response.json())}"
        )
    if response:
        response.close()

    return video_game

def http_add_new_dataset(dataset_name, bucket_id):
    server_url = f"{SERVER_ADDRESS}/datasets/add-new-dataset?dataset_name={dataset_name}&bucket_id={bucket_id}"
    response = None
    try:
        response = requests.post(server_url)
        if response.status_code == 200:
            return response.json()['response']
        else:
            print(response.json())
    except Exception as e:
        print('request exception ', e)
    return None