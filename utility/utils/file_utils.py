import os
import shutil
import hashlib

def delete_all_files(dir: str, ignore_missing=True) -> None:
    """
    This function deletes all files and directories in a directory.

    Args:
        dir (str): Path to the directory to delete files from.
        ignore_missing (bool, optional): If True, the function will not raise an error if the directory does not exist. Defaults to True.

    Raises:
        FileNotFoundError: When the directory is not found and `ignore_missing` is False.
    """
    try:
        # Delete all files and directories in the temp directory
        for item in os.listdir(path=dir):
            item_path = os.path.join(dir, item)
            if os.path.isdir(s=item_path):
                # Delete directory and its contents
                shutil.rmtree(item_path)
            else:
                # Delete file
                os.remove(path=item_path)
        os.rmdir(path=dir)
    except FileNotFoundError:
        if not ignore_missing:
            raise FileNotFoundError(f"Directory {dir} does not exist.")

def get_file_hash(fpath: str, chunk_size: int = 1024) -> str:
    """
    Calculate the SHA-256 hash of a file

    Args:
        fpath (str): The path of the file
        chunk_size (int, optional): The size of file chunk to read from the file. Defaults to 1024 bytes.

    Returns:
        str: The hash of file.
    """
    sha256 = hashlib.sha256()

    with open(fpath, 'rb') as f:
        # Read the file in chunks and update the hash
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()