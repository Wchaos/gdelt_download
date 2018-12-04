import asyncio
import random
import time
import zipfile


import aiohttp
import os
import traceback
import re

from setting import NO_TRANS_FILE_DIR, TRANS_FILE_DIR, TRANS_RE, NO_TRANS_RE
from toolkit.common_tool import get_cur_time
from toolkit.orm import get_session, GdeltTask
from user_agent import agents




async def check_pre_download(url):
    file_name = url.split("/")[-1]
    if re.match(TRANS_RE, file_name):
        part_dir = '%s/%s' % (TRANS_FILE_DIR, file_name[0:8])
    elif re.match(NO_TRANS_RE, file_name):
        part_dir = '%s/%s' % (NO_TRANS_FILE_DIR, file_name[0:8])
    else:
        failed_reason = "url不符合需要下载的正则"
        await update_url_status(url, complete_staus=0, failed_reason=failed_reason)
        return None
    full_name = '%s/%s' % (part_dir, file_name)
    if not os.path.exists(part_dir):
        os.makedirs(part_dir)
    return full_name

async def check_file_integrity(full_name,url):
    try:
        # 校验看能不能作为zip文件打开
        with zipfile.ZipFile(full_name, 'r') as f:
            pass
    except Exception as e:
        print("%s下载失败" % url)
        traceback.print_exc()
        await update_url_status(url, complete_status=4, http_status=200, failed_reason=str(e))
    else:
        print("%s下载成功" % url)
        await update_url_status(url, complete_status=1, http_status=200)

async def download_file(url, semaphore):
    full_name = await check_pre_download(url)
    print("开始下载: %s" % url)
    user_agent = random.choice(agents)
    headers = {
        'User-Agent': user_agent
    }
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, timeout=120) as resp:
                    if resp.status == 200:
                        with open(full_name, 'wb') as f:
                            while 1:
                                chunk = await resp.content.read(1000 * 1024)
                                if not chunk:
                                    break
                                f.write(chunk)
                        await check_file_integrity(full_name,url)
                    else:
                        print("%s请求返回失败" % url)
                        await update_url_status(url, complete_status=2, http_status=resp.status)
            except Exception as e:
                print("%s请求出现异常" % url)
                traceback.print_exc()
                update_url_status(url, complete_status=3, failed_reason=str(e))


async def update_url_status(url, complete_status, http_status=None, failed_reason=None):
    session = get_session()
    try:
        task = session.query(GdeltTask).filter(GdeltTask.url == url).first()
        task.complete_status = complete_status
        if complete_status == 1:
            task.failed_reason = None
        else:
            task.failed_count += 1
            if failed_reason:
                task.failed_reason = failed_reason
        if http_status:
            task.http_status = http_status
        task.update_time = get_cur_time()
        session.commit()
    except Exception as e:
        print("[%s]更新url状态失败,url=%s\r\n%s\r\n" % (get_cur_time(), url, str(e)))
    finally:
        session.close()


def start_async_download(urls, loop, semaphore):
    tasks = [download_file(url, semaphore) for url in urls]
    start = time.time()
    loop.run_until_complete(asyncio.wait(tasks))
    end = time.time()
    print("[%s]下载完成，耗时：%s\r\n" % (get_cur_time(), (end - start)))









def start_download(urls=None):
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(50)  # 限制并发量为50
    if urls:
        start_async_download(urls, loop, semaphore)
    # 之后从数据库查询下载：
    old_failed_num = -1
    while True:
        failed_urls = []
        session = get_session()
        failed_tasks = session.query(GdeltTask).filter(GdeltTask.complete_status > 1).all()
        for task in failed_tasks:
            failed_urls.append(task.url)
        failed_num = len(failed_urls)
        if failed_num == 0:
            print("[%s]所有任务全部完成!!!" % get_cur_time())
            break
        elif old_failed_num == failed_num:
            print("[%s]下载终止，不能再减少失败次数了!!!" % get_cur_time())
            break
        else:
            old_failed_num = failed_num
            print("[%s]start from mysql\r\n任务数%s" % (get_cur_time(), failed_num))
            start_async_download(failed_urls, loop, semaphore)
    loop.close()
