from PIL import Image
import hashlib

def get_image_hash(img) -> str:
    # Convert the image to bytes
    img_bytes = img.tobytes()
    # Calculate the SHA-256 hash
    image_hash = hashlib.sha256(string=img_bytes).hexdigest()
    return image_hash

def get_image_info(img_path):
    img = Image.open(fp=img_path)
    
    image_hash = get_image_hash(img=img)
    image_resolution = img.size
    image_format = img.format
    
    return {
        "image_hash": image_hash,
        "image_resolution": {
            "width": image_resolution[0],
            "height": image_resolution[1]
        },
        "image_format": image_format
    }

