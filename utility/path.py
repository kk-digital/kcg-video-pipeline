import pathlib
def file_exists(file_path):
    return pathlib.Path(file_path).is_file()

def separate_bucket_and_file_path(path_str):
    p = pathlib.Path(path_str)

    file_path = '/'.join([*p.parts[1:]])
    file_path = "{}".format(file_path)
    bucket = str(p.parts[0])

    return bucket, file_path