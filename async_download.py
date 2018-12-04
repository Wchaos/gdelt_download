import asyncio
import os
import random
import re
import time
import traceback
import zipfile

import aiohttp

from user_agent import agents

dir = 'E:/gdelt3/trans'
failed_file_name = 'fail_download.txt'
re_fliter = re.compile('2018\d+(\.translation)\.(gkg|export).+')


async def download_file(url, semaphore):
    if url.rindex('/') >= 0:
        fname = url[url.rindex('/') + 1:]
        if re_fliter.match(fname):
            partdir = '%s/%s' % (dir, fname[0:8])
            fullname = '%s/%s' % (partdir, fname)
            if not os.path.exists(partdir):
                os.makedirs(partdir)
            # 未下载过，开始下载'
            print("开始下载: %s" % fullname)
            user_agent = random.choice(agents)
            headers = {
                'User-Agent': user_agent
            }
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(url, headers=headers, timeout=120) as resp:
                            if resp.status == 200:
                                print("%s响应成功" % fullname)
                                with open(fullname, 'wb') as f:
                                    while 1:
                                        chunk = await resp.content.read(1024000)
                                        if not chunk:
                                            break
                                        f.write(chunk)
                                try:
                                    # 校验看能不能作为zip文件打开
                                    with zipfile.ZipFile(fullname, 'r') as f:
                                        pass
                                    print("%s下载成功" % fullname)
                                except Exception as e:
                                    print("%s下载不完整" % fullname)
                                    write_failed_url(url)
                            else:
                                write_failed_url(url)
                    except Exception as e:
                        print("%s下载失败" % fullname)
                        traceback.print_exc()
                        write_failed_url(url)



def write_failed_url(url):
    fail_path = "%s/fail_download_new.txt" % dir
    with open(fail_path, 'a') as f:
        f.write("%s\r\n" % url)
        f.flush()


def start_download(urls):
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(100)  # 限制并发量为50
    tasks = [download_file(url, semaphore) for url in urls]
    start = time.time()
    loop.run_until_complete(asyncio.wait(tasks))
    end = time.time()
    loop.close()
    print("下载完成，耗时：%s" % (end - start))

def get_urls(fileName):
    urls = []
    with open(fileName, mode='r') as f:
        for line in f.readlines():
            line = line.split(' ')
            if len(line) == 3:
                url = line[2].strip()
                urls.append(url)
    return urls



def get_failed_urls(failed_file):
    urls = []
    with open(failed_file, mode='r') as f:
        for line in f.readlines():
            url = line.strip()
            if url:
                urls.append(url)
    return urls
if __name__ == "__main__":
    failed_file = "%s/fail_download.txt" % dir
    urls = get_failed_urls(failed_file)
    # file = 'async_month10-11.txt'
    # urls = get_urls(file)
    start_download(urls)


