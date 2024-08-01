from typing import List

class VideoMetaData():

    def __init__(self, 
                file_hash: str,
                file_path: str,
                video_id: str,
                video_url: str,
                video_title: str,
                video_description: str,
                video_resolution: str,
                video_extension: int,
                video_length: str,
                video_filesize: int,
                video_frame_rate: str,
                video_language: str,
                processed: bool,
                game_id: str,
                upload_date: str):

        self.file_hash = file_hash
        self.file_path = file_path
        self.video_id = video_id
        self.video_url = video_url
        self.video_title = video_title
        self.video_description = video_description
        self.video_resolution = video_resolution
        self.video_extension = video_extension
        self.video_length = video_length
        self.video_filesize = video_filesize
        self.video_frame_rate = video_frame_rate
        self.video_language = video_language
        self.processed = processed
        self.game_id = game_id
        self.upload_date = upload_date
        
    def serialize(self):
        return {
            "file_hash": self.file_hash,
            "file_path": self.file_path,
            "video_id": self.video_id,
            "video_url": self.video_url,
            "video_title": self.video_title,
            "video_description": self.video_description,
            "video_resolution": self.video_resolution,
            "video_extension": self.video_extension,
            "video_length": self.video_length,
            "video_filesize": self.video_filesize,
            "video_frame_rate": self.video_frame_rate,
            "video_language": self.video_language,
            "processed": self.processed,
            "game_id": self.game_id,
            "upload_date": self.upload_date
        }
    
    @classmethod
    def deserialize(cls, data):

        return cls(
            file_hash=data.get("file_hash"),
            file_path=data.get("file_path"),
            video_id=data.get("video_id"),
            video_url=data.get("video_url"),
            video_title=data.get("video_title"),
            video_description=data.get("video_description"),
            video_resolution=data.get("video_resolution"),
            video_extension=data.get("video_extension"),
            video_length=data.get("video_length"),
            video_filesize=data.get("video_filesize"),
            video_frame_rate=data.get("video_frame_rate"),
            video_language=data.get("video_language"),
            processed=data.get("processed"),
            game_id=data.get("game_id"),
            upload_date=data.get("upload_date"),
        )