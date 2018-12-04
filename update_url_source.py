import random
import re

import requests

from setting import USER_AGENT, NO_TRANS_FILE_URL, TRANS_FILE_URL


def download_urls(file_url,file_name):
    user_agent = random.choice(USER_AGENT)
    headers = {
        "User-Agent": user_agent
    }
    response = requests.get(url=file_url, headers=headers,stream=True, timeout=180)
    if response.status_code == 200:
        with open(file_name,'wb') as f:
            for chunk in response.iter_content(chunk_size=1000*1024):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception("初始url文件下载失败！！！")



"""
有问题，下载的不完全， 还是手动更新吧
"""
if __name__ == '__main__':
    no_trans_file_name = NO_TRANS_FILE_URL.split("/")[-1]
    download_urls(NO_TRANS_FILE_URL,no_trans_file_name)
    trans_file_name = TRANS_FILE_URL.split("/")[-1]
    download_urls(TRANS_FILE_URL,trans_file_name)

