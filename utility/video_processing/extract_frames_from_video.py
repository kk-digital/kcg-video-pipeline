import os

import ffmpeg
import numpy as np
from PIL import Image
import cv2
import argparse

from tqdm.auto import tqdm

import sys
base_dir = './'
sys.path.insert(0, base_dir)

from utility.utils.image_utils import get_image_info

def frame_generator(video_path, start=0., end=None, fps=1, width=1920, height=1080):

    # .input(video_path, hwaccel='cuda', hwaccel_device='0', hwaccel_output_format='cuda')
    if end is None:
        stream = ffmpeg.input(video_path, ss=start)
    else:
        stream = ffmpeg.input(video_path, ss=start, to=end)
    
    process = (
        stream
        .filter('fps', fps=fps)
        .output('pipe:', format='rawvideo', pix_fmt='rgb24')
        .global_args('-loglevel', 'error')
        .run_async(pipe_stdout=True)
    )
    while True:
        in_bytes = process.stdout.read(width * height * 3)
        if not in_bytes:
            break
        in_frame = np.frombuffer(in_bytes, np.uint8).reshape([height, width, 3])
        yield in_frame
    process.wait()


def key_frame_generator(video_path, start=0., end=None, width=1920, height=1080):

    # .input(video_path, hwaccel='cuda', hwaccel_device='0', hwaccel_output_format='cuda')
    if end is None:
        stream = ffmpeg.input(video_path, ss=start, skip_frame='nokey')
    else:
        stream = ffmpeg.input(video_path, ss=start, to=end, skip_frame='nokey')
        
    process = (
        stream
        .filter('select', 'eq(pict_type,I)')
        .output('pipe:', format='rawvideo', pix_fmt='rgb24', vsync='vfr')
        .global_args('-loglevel', 'error')
        .run_async(pipe_stdout=True)
    )
    
    frame_num = 0
    while True:
        in_bytes = process.stdout.read(width * height * 3)
        if not in_bytes:
            break
        in_frame = np.frombuffer(in_bytes, np.uint8).reshape([height, width, 3])
        yield in_frame, frame_num
        frame_num += 1

    process.wait()


def get_video_info(video_path):
    try:
        probe = ffmpeg.probe(video_path)
        video_info = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
        width = video_info['width']
        height = video_info['height']
        codec = video_info['codec_name']
        # duration = video_info['duration']
        fps = eval(video_info['avg_frame_rate'])  # Converts the fraction fps to a float
        # frames = video_info['nb_frames']
        return {
            'width': width,
            'height': height,
            'codec': codec,
            'fps': fps,
            # 'frames': frames,
            # 'duration': duration
        }
    except ffmpeg.Error as e:
        print("FFmpeg error:", e.stderr)
        raise e


def process_video(video_path, output_dir, fps):

    info = get_video_info(video_path)
    print(info)

    os.makedirs(output_dir, exist_ok=True)

    distance_threshold = 40
    match_ratio_threshold = 0.75
    thumb_delta_threshold = 0.1

    num_old_thumbs = 64

    orb = cv2.ORB_create()

    count = 0

    old_kp, old_des = None, None
    old_thumbs = list()

    frames = []
    for frame, frame_num in tqdm(key_frame_generator(video_path, width=info['width'], height=info['height'])):
    
        kp, des = orb.detectAndCompute(frame, None)

        thumb = np.array(Image.fromarray(frame).resize((64, 64))).astype(float) / 255.

        if old_kp is not None and old_des is not None:
            
            matcher = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
            matches = matcher.match(des, old_des)
            matches = sorted(matches, key=lambda x: x.distance)

            good_matches = [m for m in matches if m.distance < distance_threshold]
            good_ratio = len(good_matches) / len(matches) if matches else 0

            if good_ratio > match_ratio_threshold:
                continue

            delta = np.abs(thumb - np.stack(old_thumbs, axis=0)).mean(-1).mean(-1).mean(-1).min()

            if delta < thumb_delta_threshold:
                continue
        
        frame_path = os.path.join(output_dir, f'{count:05d}.jpg')
        Image.fromarray(frame).save(frame_path)

        frame_info = get_image_info(frame_path)
        frame_info['frame_num'] = frame_num
        frame_info['file_path'] = frame_path

        frames.append(frame_info)

        count += 1
        
        old_kp, old_des = kp, des
        if len(old_thumbs) >= num_old_thumbs:
            old_thumbs = old_thumbs[1:]
        old_thumbs.append(thumb)
    
    return frames

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a video.")
    parser.add_argument("--video_path", type=str, required=True, help="Path to the input video file")
    parser.add_argument("--output_dir", type=str, required=True, help="Path where the output video should be saved")
    parser.add_argument("--fps", type=int, required=True, help="Frames per second for the output video")

    args = parser.parse_args()

    print(process_video(args.video_path, args.output_dir, args.fps))
