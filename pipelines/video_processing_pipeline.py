
import sys
import os
from io import BytesIO
from typing import Dict, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from minio import Minio
from pydantic import FilePath

import msgpack

import torch
from PIL import Image

base_dir = './'
sys.path.insert(0, base_dir)

from utility.http import request
from utility.minio import cmd
from utility.utils.video_utils import download_video
from utility.utils.url_utils import get_video_short_hash_from_url
from utility.path import separate_bucket_and_file_path
from utility.video_processing import extract_frames_from_video
from utility.minio.progress import Progress
from utility.utils.file_utils import delete_all_files
from utility import logger

from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder

from schema import VideoMetaData
class VideoProcessingPipeline():
    
    def __init__(self, 
                 minio_client: Minio, 
                 image_encoder: KandinskyCLIPImageEncoder,
                 dataset_name: str, 
                 video_bucket_name = 'ingress-video',
                 frame_bucket_name = 'external') -> None:
        self.dataset = dataset_name
        self.video_bucket_name = video_bucket_name
        self.frame_bucket_name = frame_bucket_name
        self.minio_client = minio_client
        self._temp_dir = 'output/temp'
        self._temp_video_path = ''
        self._extracted_frames = []
        
        self.encoder = image_encoder
        
        if os.path.isfile(self._temp_dir):
            os.mkdir(self._temp_dir)

    def _download_video_from_url(self, url: str) -> Dict:
        logger.debug(msg="Download video...")
        video_download_info = download_video(url=url, output_dir=self._temp_dir, type="yt_dlp")
        if video_download_info is None:
            raise Exception("Downloading video failed")
        self._temp_video_path = \
            '{}/{}.{}'.format(
                video_download_info['video_filepath'],
                video_download_info['video_filename'],
                video_download_info['video_format'])
        logger.debug("Successfully downloaded!")
        return video_download_info
        
    def _upload_video_to_minio(self) -> None:
        logger.debug("Uploading the video....")
        _, file_path = separate_bucket_and_file_path(path_str=self.video_metadata.file_path)
        cmd.upload_from_file(client=self.minio_client,
                             bucket_name=self.video_bucket_name, 
                             object_name=file_path,
                             file_path=self._temp_video_path,
                             progress=Progress())
        
        buf = BytesIO()
        buf.write(json.dumps(self.video_metadata.serialize(), indent=4).encode())
        buf.seek(0)
        
        cmd.upload_data(client=self.minio_client,
                        bucket_name=self.video_bucket_name,
                        object_name="{}.json".format(os.path.splitext(file_path)[0]),
                        data=buf)
        
        logger.debug("Successfully upload video!")
        # os.remove(self._temp_video_path)

    def _save_video_metadata_in_db(self, video_download_info: Dict) -> None:
        video_metadata = VideoMetaData(
            file_hash=video_download_info['video_hash'],
            filename=video_download_info['video_filename'],
            file_path='',
            file_type=video_download_info['video_format'],
            source_url=video_download_info['video_url'],
            video_length=video_download_info['video_length(s)'],
            video_resolution=video_download_info['video_resolution'],
            video_frame_rate=video_download_info['video_fps'],
            video_description='',
            dataset=self.dataset,
            upload_date=''
        )

        self.video_metadata =\
              request.http_add_video(video_metadata=video_metadata.serialize())
        if self.video_metadata is None:
            raise Exception("Uploading _save_video_metadata_in_db failed")

    def _get_video_metadata(self, video_hash: str) -> None:
        self.video_metadata = request.http_get_video_metadata(video_hash=video_hash)
        if self.video_metadata is None:
            raise Exception("Getting video metadata failed!")

    def _download_video_from_minio(self) -> None:
        self._temp_video_path = \
            f'{self._temp_dir}/{self.video_metadata.filename}.{self.video_metadata.file_type}'

        cmd.download_from_minio(self.minio_client, 
                                self.frame_bucket_name, 
                                self.video_metadata.file_path, 
                                self._temp_video_path)

    def _extract_frame(self) -> None:
        logger.debug(msg="Extracting frames from vido....")
        # # define dir path to extract frames from video
        frames_dir = os.path.join(self._temp_dir, os.path.splitext(self.video_metadata.filename)[0])
        if os.path.isfile(path=frames_dir):
            os.makedirs(name=frames_dir)

        # TODO: Temporary part for test
        self._extracted_frames = \
            extract_frames_from_video.process_video(video_path=self._temp_video_path, 
                                                output_dir=frames_dir,
                                                fps=self.video_metadata.video_frame_rate)
        with open("extract_frame.json", mode='w') as f:
            json.dump(self._extracted_frames, f, indent=4)

        # with open(file="extract_frame.json", mode='r') as f:
        #     self._extracted_frames = json.load(fp=f)
        # END TEST
            
        logger.debug(msg="Successfully extracted frames!")
    
    def _get_clip_vector(self, fpath: str):
        img = Image.open(fpath)
        img = img.convert("RGB")
        
        clip_vector = self.encoder.get_image_features(img)
        clip_vector = clip_vector.cpu().numpy().tolist()
        
        clip_feature_dict = {"clip-feature-vector": clip_vector}
        clip_feature_msgpack = msgpack.packb(clip_feature_dict)

        clip_feature_msgpack_buffer = BytesIO()
        clip_feature_msgpack_buffer.write(clip_feature_msgpack)
        clip_feature_msgpack_buffer.seek(0)

        return clip_feature_msgpack_buffer
    
    def _upload_clip_vector(self, minio_path: str, fpath: str):
        clip_vector = self._get_clip_vector(fpath)
        
        output_path = os.path.splitext(minio_path)[0]
        output_path = output_path + "_clip_kandinsky.msgpack"
        print(output_path, "------------------------->")
        cmd.upload_data(client=self.minio_client,
                        bucket_name=self.frame_bucket_name,
                        object_name=output_path,
                        data=clip_vector)
    
    def _upload_frame_into_minio(self) -> None:
        logger.debug(msg="Uploading frames into minio....")
        
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = []
            for frame_info in self._extracted_frames:
                _, file_path = \
                        separate_bucket_and_file_path(path_str=frame_info['file_path'])
                
                futures.append(executor.submit(self._upload_clip_vector, 
                                               file_path,
                                               self.hash_to_local_path_map[frame_info["image_hash"]]))

        with ThreadPoolExecutor(max_workers=32) as executor:
            for frame_info in self._extracted_frames:
                _, file_path = \
                        separate_bucket_and_file_path(path_str=frame_info['file_path'])
                futures.append(executor.submit(cmd.upload_from_file, 
                                            self.minio_client,
                                            self.frame_bucket_name,
                                            file_path,
                                            self.hash_to_local_path_map[frame_info["image_hash"]]))

        logger.debug(msg="Successfully uploaded frames!")
            
    def _save_metadata_in_db(self) -> None:
        
        logger.debug(msg="Uploading frame metadata into mongodb...")

        upload_frames_list = []
        for frame in self._extracted_frames:
            frame_info = {
                'file_path': '',
                'dataset': self.dataset,
                'image_hash': frame['image_hash'],
                'image_resolution': frame['image_resolution'],
                'image_format': frame['image_format'],
                'source_image_dict': {
                    'frame_num': frame['frame_num'],
                    'source_video': self.video_metadata.file_hash
                },
                'task_attributes_dict': {},
                'upload_date': '',
                'uuid': ''
            }
            upload_frames_list.append(frame_info)
        
        updated_frames_list = []
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = []
            for frame in upload_frames_list:
                futures.append(executor.submit(request.http_add_image, frame))
                continue
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    updated_frames_list.append(future.result())
                
        self.hash_to_local_path_map = {}
        for frame_info in self._extracted_frames:
            self.hash_to_local_path_map[frame_info["image_hash"]] = frame_info["file_path"]

        self._extracted_frames = updated_frames_list
        logger.debug(msg="Successfully uploaded frame metadata!")
        
    def delete_temp_files(self) -> None:
        # delete_all_files(self._temp_dir)
        pass

    def run_pipeline_with_url(self, url: str) -> Tuple[bool, str]:
        is_success = False
        try:
            self._temp_dir = \
                os.path.join(self._temp_dir, get_video_short_hash_from_url(url=url))
            os.makedirs(name=self._temp_dir, exist_ok=True)
            
            video_download_info = self._download_video_from_url(url=url)
            
            self._save_video_metadata_in_db(video_download_info=video_download_info)
            self._upload_video_to_minio()
            
            self._extract_frame()

            self._save_metadata_in_db()
            self._upload_frame_into_minio()

            is_success = True

        except Exception as e:
            logger.error(msg="Error on running pipeline -> " + str(e))
            return False, url
        finally:
            self.delete_temp_files()

        return is_success, url

    def run_pipeline_with_video_hash(self, video_hash) -> Tuple[bool, str]:
        is_success = False
        try:
            self._get_video_metadata(video_hash=video_hash)
            self._download_video_from_minio()

            self._extract_frame()

            self._save_metadata_in_db()
            self._upload_frame_into_minio()

            is_success = True

        except Exception as e:
            logger.exception(msg="Error on running pipeline:" + str(e))
            return False, video_hash
        finally:
            self.delete_temp_files()

        return is_success, video_hash