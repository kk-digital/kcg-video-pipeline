from typing import List

class VideoMetaData():

    def __init__(self, 
                 file_hash: str, 
                 filename: str, 
                 file_path: str, 
                 file_type: str, 
                 source_url: str, 
                 video_length: int,
                 video_resolution: str, 
                 video_frame_rate: int, 
                 video_description: str, 
                 dataset: str,
                 upload_date: str):
        """
        Initializes a Video object with the given attributes.

        Args:
            file_hash (str): The hash of the video file.
            filename (str): The name of the video file.
            file_path (str): The path to the video file.
            file_type (str): The type of the video file (e.g., mp4, avi).
            source_url (str): The URL of the video source.
            video_length (int): The length of the video in seconds.
            video_resolution (str): The resolution of the video (e.g., 1080p, 720p).
            video_frame_rate (int): The frame rate of the video.
            video_description (str): A description of the video.
            upload_date (str): The date the video was uploaded.
        """

        self.file_hash = file_hash
        self.filename = filename
        self.file_path = file_path
        self.file_type = file_type
        self.source_url = source_url
        self.video_length = video_length
        self.video_resolution = video_resolution
        self.video_frame_rate = video_frame_rate
        self.video_description = video_description
        self.dataset = dataset
        self.upload_date = upload_date
        
    def serialize(self):
        return {
            "file_hash": self.file_hash,
            "filename": self.filename,
            "file_path": self.file_path,
            "file_type": self.file_type,
            "source_url": self.source_url,
            "video_length": self.video_length,
            "video_resolution": self.video_resolution,
            "video_frame_rate": self.video_frame_rate,
            "video_description": self.video_description,
            "dataset": self.dataset,
            "upload_date": self.upload_date
        }
    
    @classmethod
    def deserialize(cls, data):
        """
        Deserializes a Video Meta object from a dictionary.

        Args:
            data (dict): A dictionary containing the video meta data.

        Returns:
            Video: A Video Meta object.
        """

        return cls(
            file_hash=data.get("file_hash"),
            filename=data.get("filename"),
            file_path=data.get("file_path"),
            file_type=data.get("file_type"),
            source_url=data.get("source_url"),
            video_length=data.get("video_length"),
            video_resolution=data.get("video_resolution"),
            video_frame_rate=data.get("video_frame_rate"),
            video_description=data.get("video_description"),
            dataset=data.get("dataset"),
            upload_date=data.get("upload_date"),
        )
    

class VideoMetaDataList():

    def __init__(self, video_metadata_list: List[VideoMetaData]):
        """
        Initializes a Video List object with the given attributes.
        Args:
            videos (list[VideoMetaData]): A list of Video Meta objects.
        """
        self.video_metadata_list = video_metadata_list

    def serialize(self):
        return [video.serialize() for video in self.video_metadata_list]
    
    def deserialize(cls, data):
        """
        Deserializes a Video List object from a dictionary.
        
        Args:
            data(list(dict)): A list of dictionaries containing the video meta data.
        Returns:
            VideoList: A Video Meta List object.
        """

        return cls([VideoMetaData.deserialize(video_data) for video_data in data])