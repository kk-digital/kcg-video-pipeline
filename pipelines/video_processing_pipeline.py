
from genericpath import isfile
import sys
import os
from io import BytesIO
from typing import Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from minio import Minio
import msgpack
from PIL import Image

base_dir = './'
sys.path.insert(0, base_dir)

from utility.http import request
from utility.minio import cmd
from utility.path import separate_bucket_and_file_path
from utility.video.video_processing import extract_frames_from_video
from utility.minio.progress import Progress
from utility.utils.file_utils import delete_all_files
from utility import logger

from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder

from schema import VideoMetaData
class VideoProcessingPipeline():
    
    def __init__(self, 
                minio_client: Minio,
                image_encoder: KandinskyCLIPImageEncoder,
                video: VideoMetaData, 
                video_bucket_name = 'ingress-video',
                frame_bucket_name = 'external') -> None:
        self.video_metadata = video
        self.video_bucket_name = video_bucket_name
        self.frame_bucket_name = frame_bucket_name
        self.minio_client = minio_client
        self._temp_dir = \
            os.path.join('output',
                        video_bucket_name,
                        self.video_metadata.video_id)
        self._temp_video_path = ''
        self._extracted_frames = []
        
        self.encoder = image_encoder
        
        if os.path.isfile(self._temp_dir):
            os.mkdir(self._temp_dir)

    def _download_video_from_minio(self) -> None:
        self._temp_video_path = \
            os.path.join(self._temp_dir,
                        f"{self.video_metadata.video_id}.{self.video_metadata.video_extension}")
        
        if os.path.isfile(self._temp_video_path):
            logger.debug(msg=f'Video file already exists: {self._temp_video_path}')
            return
        
        bucket, video_minio_path = separate_bucket_and_file_path(
            self.video_metadata.file_path
        )
        cmd.download_from_minio(self.minio_client, 
                                self.video_bucket_name, 
                                video_minio_path, 
                                self._temp_video_path)

    def _extract_frame(self) -> None:
        logger.debug(msg="Extracting frames from video....")
        # define dir path to extract frames from video
        frames_dir = os.path.join(self._temp_dir)
        if os.path.isfile(path=frames_dir):
            os.makedirs(name=frames_dir)

        self._extracted_frames = \
            extract_frames_from_video.process_video(video_path=self._temp_video_path, 
                                                output_dir=frames_dir,
                                                fps=self.video_metadata.video_frame_rate)

        with open("extract_frame.json", mode='w') as f:
            json.dump(self._extracted_frames, f, indent=4)
            
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
        output_path = f"{output_path}_clip_kandinsky.msgpack"
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

        dataset = request.http_get_video_game(game_id=self.video_metadata.game_id)['title']
        request.http_add_new_dataset(dataset, 2)
        print(dataset)
        upload_frames_list = []
        for frame in self._extracted_frames:
            frame_info = {
                'uuid': '',
                'file_path': '',
                'dataset': dataset,
                'image_hash': frame['image_hash'],
                'image_resolution': frame['image_resolution'],
                'image_format': frame['image_format'],
                'source_image_dict': {
                    'frame_num': frame['frame_num'],
                    'source_video': self.video_metadata.video_id
                },
                'task_attributes_dict': {},
                'upload_date': '',
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
    
    def _update_video_status_to_processed(self):
        self.video_metadata.processed = True
        request.http_update_video_status_to_processed(self.video_metadata)

    def delete_temp_files(self) -> None:
        delete_all_files(self._temp_dir)
    
    def _update_video_metadata(self):
        video_file_stats = os.stat(self._temp_video_path)
        self.video_metadata.video_filesize = video_file_stats.st_size
    
    def run(self) -> Tuple[bool, str]:
        is_success = False
        
        try:
            self._download_video_from_minio()
            # temporary part: To update file size of video metadata 
            # because file size of some video metadata is missing now.
            self._update_video_metadata()
            self._extract_frame()
            self._save_metadata_in_db()
            self._upload_frame_into_minio()
            self._update_video_status_to_processed()
            self.delete_temp_files()
            is_success = True
        except Exception as e:
            logger.error(msg="Error on running pipeline:" + str(e))
    
        return is_success, self.video_metadata.video_id
    
    def get_uploaded_image_count(self):
        return len(self._extracted_frames)