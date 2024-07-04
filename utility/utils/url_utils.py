from urllib.parse import urlparse, parse_qs

def get_video_short_hash_from_url(url: str) -> str:    
    # Parse the URL using urlparse
    parsed_url = urlparse(url=url)

    # Extract the query parameters using parse_qs
    query_params = parse_qs(qs=parsed_url.query)
    # Get the value of the 'v' parameter
    video_short_hash = query_params.get('v', [""])[0]

    if not video_short_hash:
        raise ValueError("The video short hash is empty.")
    
    return video_short_hash