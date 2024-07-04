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
from utility.logger import logger

from config import ORCHESTRATION_ADDRESS as SERVER_ADDRESS
MAX_RETRY = 6
DELAY_TIME = 2


def http_wrapper(http_request, retry = 0, *args, **wargs):
    try:
        result = http_request(*args, **wargs)
        return result
    except ConnectionRefusedError as e:
        if retry < MAX_RETRY:
            retry_delay_time = DELAY_TIME ** retry
            time.sleep(retry_delay_time)

            logger.warning(msg=f"{retry + 1}th retrying( http_{http_request.__name__} )... ")
            http_wrapper(http_request=http_request, retry = retry + 1, *args, **wargs)
        else:
            logger.error(msg=f"http_{http_request.__name__} " + str(e))
            return None
    except Exception as e:
        logger.error(msg=f"http_{http_request.__name__} " + str(e))
        return None
    
def http_add_video(video_metadata) -> Optional[VideoMetaData]:

    def add_video(video_metadata) -> Optional[VideoMetaData]:
        url = SERVER_ADDRESS + '/ingress-video/add-video'
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url=url, json=video_metadata, headers=headers)

        video_metadata = None
        if response.status_code == 200:
            video_metadata_json = response.json()['response']
            video_metadata = VideoMetaData.deserialize(data=video_metadata_json)
        else:
            logger.error(msg="Uploading video metadata failed -> " + json.dumps(obj=response.json()))
        if response:
            response.close()
            
        return video_metadata
    
    return http_wrapper(http_request=add_video, video_metadata=video_metadata)

def http_add_image_list(image_data_list) -> Optional[List[Dict]]:

    def add_image_list(image_data_list) -> Optional[List[Dict]]:
        url = SERVER_ADDRESS + '/external-images/add-external-image-list'
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
        url = SERVER_ADDRESS + '/external-images/add-external-image'
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url=url, json=image_data, headers=headers)
        result = None
        if response.status_code == 200:
            result = response.json()['response']
        elif response.status_code == 422:
            logger.debug(msg="Image metadata already existed! {}".format(image_data["image_hash"]))
        else:
            logger.error(msg="Adding image metadata into failed! -> {}".format(response.text))
        if response:
            response.close()
            
        return result
    
    return http_wrapper(http_request=add_image, image_data=image_data)

def http_get_video_metadata(video_hash) -> Optional[VideoMetaData]:

    def get_video_metadata(video_hash) -> Optional[VideoMetaData]:
        url = SERVER_ADDRESS + f'/ingress-video/get-video?video_hash={video_hash}'
        response = requests.get(url=url)

        video_metadata = None
        if response.status_code == 200:
            video_metadata_json = response.json()['response']['data']
            video_metadata = VideoMetaData.deserialize(data=video_metadata_json)
            return video_metadata
        else:
            logger.error(msg="Getting video metadata from mongodb failed -> " + json.dumps(obj=response.json()))
        if response:
            response.close()
                
        return video_metadata
    
    return http_wrapper(http_request=get_video_metadata, video_hash=video_hash)

