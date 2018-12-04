import random

import requests

from setting import USER_AGENT


def download_urls(file_url):
    user_agent = random.choice(USER_AGENT)
    headers = {
        "User-Agent": user_agent
    }
    response = requests.get(url=file_url, headers=headers,stream=True, timeout=120)
    if response.status_code == 200:
        file_name = file_url.split('/')[-1]
        with open(file_name,'wb') as f:
            for chunk in response.iter_content(chunk_size=1000*1024):
                if chunk:
                    f.write(chunk)
    else:









 r = requests.get(url, stream=True, timeout=1)
            with open(fullname, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024000):
                    if chunk:
                        f.write(chunk)