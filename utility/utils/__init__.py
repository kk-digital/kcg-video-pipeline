
def minio_path_with_seq_id(dataset_name, middle_path, seq_id):
    folder_id = (seq_id // 1000) + 1
    file_id = (seq_id % 1000) + 1
    
    folder_name = f"{folder_id:04d}"
    file_name = f"{file_id:06d}"
    
    if middle_path != '' and middle_path is not None:
        path = f'{dataset_name}/{middle_path}/{folder_name}/{file_name}'
    else:
        path = f'{dataset_name}/{folder_name}/{file_name}'
    
    return path

def get_minio_video_path(seq_id, dataset_name, format, middle_path = ''):
    path = minio_path_with_seq_id(dataset_name, middle_path, seq_id)

    return f'{path}.{format}'

# TODO: Consider to Remove
def minio_video_metadata_path(seq_id, dataset_name):
    path = minio_path_with_seq_id(dataset_name, seq_id)

    return f'{path}.json'