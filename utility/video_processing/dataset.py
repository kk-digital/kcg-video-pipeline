import torch
from PIL import Image, ImageOps
import numpy as np
from torchvision.transforms.v2 import functional as VF
from torchvision.transforms.v2 import RandomResizedCrop

def pad_to_square(pil_img: Image.Image, fill_color=0):
    width, height = pil_img.size

    if width == height:
        return pil_img
    elif width > height:
        padding = (0, (width - height) // 2)
    else:
        padding = ((height - width) // 2, 0)

    result = ImageOps.expand(pil_img, border=padding, fill=fill_color)
    return result

def crop_to_square(pil_img: Image.Image):
    width, height = pil_img.size
    
    if width == height:
        return pil_img
    elif width > height:
        left = (width - height) // 2
        right = left + height
        top = 0
        bottom = height
    else:
        top = (height - width) // 2
        bottom = top + width
        left = 0
        right = width

    result = pil_img.crop((left, top, right, bottom))
    return result

class Dataset(torch.utils.data.Dataset):

    def __init__(self, file_paths: list, target_size: int):
        # Constructor for the Dataset object
        self.file_paths = file_paths  # List of file paths for the images
        self.target_size = target_size  # Target size to which images will be resized
    
    def __len__(self):
        # Return the total number of items in the dataset
        return len(self.file_paths)
    
    def __getitem__(self, index: int):
        # Retrieve an item by its index
        file_path = self.file_paths[index]  # Get the file path at the specified index

        # Open the image, convert it to RGB to ensure it has three channels
        image = Image.open(file_path).convert('RGB')

        # Calculate a scale factor for resizing that ensures no distortion occurs, but limits scaling to a factor of 0.5 at most
        scale = min((self.target_size / min(image.size)) ** 2, .5)

        # Get the parameters for cropping and resizing using the scale factor and a fixed aspect ratio of 1:1
        params = RandomResizedCrop.get_params(image, scale=(scale, 1), ratio=(1., 1.))
        # Apply the cropping and resizing transformation to the image with bicubic interpolation and antialiasing
        image = VF.resized_crop(image, *params, size=self.target_size, interpolation=VF.InterpolationMode.BICUBIC, antialias=True)

        # Convert the image to a NumPy array
        image = np.array(image)
        
        # Return the processed image
        return image
