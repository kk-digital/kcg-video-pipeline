# kcg-dataset-video-dataset-processing

This pipeline aims to extract frames from a video, get clip vectors from the extracted frames, and save the video, the extracted frames and clip vectors into Minio.

# environment setup

install ffmpeg and other libraries
```bash
pip install -r requirements.txt
apt-get install libopengl0 libegl1
apt-get install ffmpeg
```

requirements.txt
```
mamba
ffmpeg-python
Pillow
numpy
pandas
matplotlib
ipython
ipykernel
ipywidgets
nbformat
tqdm
pytube
pydantic
minio
requests
python-dotenv
opencv-python
```

# Pipeline workflow

### 1. The pipeline downloads a video and extracts the video metadata by the URL from YouTube.
 - video metadata
    ```
        file_hash: str
        filename: str
        file_path: str
        file_type: str
        source_url: str
        video_length: int
        video_resolution: str
        video_frame_rate: int
        video_description: str
        dataset: str
        upload_date: str

    ```
### 2. The pipeline saves the video metadata into MongoDB and uploads the video to Minio with a unique identifier.specific minio path
 - Save video meta data into ingress-video-collection
 - Upload video to minio
    ```
        {video_game_name}/{short_hash}_720p30fps.mp4
        {video_game_name}/{short_hash}_720p30fps.json
    ```
### 3. Extract frames and metadata from video
 - frame metadata
 ```
    {
    'file_path': str,
    'dataset': str,
    'image_hash': str,
    'image_resolution': {
        'width': int,
        'height': int
    },
    'image_format': str,
    'source_image_dict': {
        'frame_num': int,
        'source_video': str
    },
    'task_attributes_dict': dict,
    'upload_date': str,
    'uuid': str
}
 ```
### 4. Save frame metadata into mongodb and upload extracted frame to minio
- frame minio path
    ```
        {video_game_name}/0001/000001.jpg
    ```
### 5. Get clip vector from extracted frame and upload to minio
```
    {video_game_name}/{short_hash}_720p30fps_clip_kandinsky.msgpack
```
### 6. Delete all temporary files
