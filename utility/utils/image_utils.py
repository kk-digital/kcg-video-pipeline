from PIL import Image
import hashlib
from utility.utils.blake256_hash import Blake256

def get_image_hash(img) -> str:
    # Convert the image to bytes
    img_bytes = img.tobytes()
    # Calculate the SHA-256 hash
    image_hash = hashlib.sha256(string=img_bytes).hexdigest()
    return image_hash

def get_blake256_hash(img) -> str:
    # Convert the image to bytes
    img_bytes = img.tobytes()
    # Compute BLAKE256 hash using Blake256 class with data
    hash_instance = Blake256(data=img_bytes)
    return hash_instance.hexdigest()

def get_image_info(img_path):
    img = Image.open(fp=img_path)
    
    image_hash = get_image_hash(img=img)
    image_blake256_hash = get_blake256_hash(img=img)
    image_resolution = img.size
    image_format = img.format
    
    return {
        "image_hash": image_hash,
        "blake256_hash": image_blake256_hash,
        "image_resolution": {
            "width": image_resolution[0],
            "height": image_resolution[1]
        },
        "image_format": image_format
    }

