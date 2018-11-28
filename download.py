import asyncio
import random
import time
import zipfile
from datetime import datetime

import aiohttp
import requests
import queue
import os
import traceback
import re

from orm import get_session, GdeltTask
from user_agent import agents

notrans_result_dir = 'D:/gdelt2/notrans'
trans_result_dir = 'D:/gdelt2/trans'

notrans_url_re = re.compile('2018\d+\.(gkg|export).+')
trans_url_re = re.compile('2018\d+(\.translation)\.(gkg|export).+')
double_url_re = re.compile('2018\d+(|\.translation)\.(gkg|export).+')

log_f = open("log.txt", 'a', encoding="utf-8")

async def download_file(url, semaphore):
    if url.rindex('/') >= 0:
        fname = url[url.rindex('/') + 1:]
        if trans_url_re.match(fname):
            partdir = '%s/%s' % (trans_result_dir, fname[0:8])
        elif notrans_url_re.match(fname):
            partdir = '%s/%s' % (notrans_result_dir, fname[0:8])
        else:
            failed_reason = "url不符合需要下载的正则"
            update_url_status(url, complete_staus=0, failed_reason=failed_reason)
        fullname = '%s/%s' % (partdir, fname)
        if not os.path.exists(partdir):
            os.makedirs(partdir)
        print("开始下载: %s" % fullname)
        user_agent = random.choice(agents)
        headers = {
            'User-Agent': user_agent
        }
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, headers=headers, timeout=30) as resp:
                        if resp.status == 200:
                            with open(fullname, 'wb') as f:
                                while 1:
                                    chunk = await resp.content.read(100 * 1024)
                                    if not chunk:
                                        break
                                    f.write(chunk)
                            try:
                                # 校验看能不能作为zip文件打开
                                with zipfile.ZipFile(fullname, 'r') as f:
                                    pass
                                print("%s下载成功" % fullname)
                            except Exception as e:
                                print("%s下载失败" % fullname)
                                update_url_status(url, complete_status=4, http_status=200, failed_reason=str(e))
                            else:
                                print("%s下载成功" % fullname)
                                update_url_status(url, complete_status=1, http_status=200)
                        else:
                            print("%s请求返回失败" % fullname)
                            update_url_status(url, complete_stattus=2, http_status=resp.status)
                except Exception as ex:
                    print("%s请求出现异常" % fullname)
                    traceback.print_exc()
                    update_url_status(url, complete_status=3, failed_reason=str(ex))


def update_url_status(url, complete_status, http_status=None, failed_reason=None):
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
        log_f.write("[%s]更新url状态失败,url=%s\r\n%s\r\n" % (get_cur_time(), url, str(e)))
    finally:
        session.close()

def start_async_download(urls, loop, semaphore):

    tasks = [download_file(url, semaphore) for url in urls]
    start = time.time()
    loop.run_until_complete(asyncio.wait(tasks))
    end = time.time()
    log_f.write("[%s]下载完成，耗时：%s\r\n" % (get_cur_time(), (end - start)))
    print("下载完成，耗时：%s" % (end - start))


def write_urls_to_db(urls):
     session = get_session()
     try:
         for url in urls:
             file_name = ''
             if url.rindex('/') >= 0:
                 file_name = url[url.rindex('/') + 1:]
             task = GdeltTask(url=url, file_name=file_name, file_date=file_name[0:8])
             session.add(task)
             session.commit()
     except Exception as e:
         log_f.write("[%s]urls写入数据库时，发生异常\r\n%s\r\n" % (get_cur_time(), str(e)))
     finally:
         session.close()



def get_urls_from_file(fileName,pattern):
    urls = []
    with open(fileName, mode='r') as f:
        for line in f.readlines():
            line = line.split(' ')
            if len(line) == 3:
                url = line[2].strip()
                if re.search(pattern, url):
                    urls.append(url)
    return urls

def get_cur_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def start_download(file):

    #文件中包含的两种url都取出来
    # urls = get_urls_from_file(file, double_url_re)
    # write_urls_to_db(urls)

    # log_f.write("[%s]start from file \r\n共从文件中取出%s个url,放入数据库\r\n" % (get_cur_time(), len(urls)))
    # 第一遍下载
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(50)  # 限制并发量为50
    # start_async_download(urls, loop, semaphore)
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
            log_f.write("[%s]所有任务全部完成!!!" % get_cur_time())
            print("所有任务全部完成!!!")
            break
        elif old_failed_num == failed_num:
            log_f.write("[%s]下载终止，不能再减少失败次数了!!!" % get_cur_time())
            print("下载终止，不能再减少失败次数了!!!")
            break
        else:
            old_failed_num = failed_num
            log_f.write("[%s]start from mysql\r\n任务数%s" % (get_cur_time(), failed_num))
            start_async_download(failed_urls, loop, semaphore)
    loop.close()

def write_failed_url():
    session = get_session()
    failed_tasks = session.query(GdeltTask).filter(GdeltTask.complete_status > 1).all()
    fail_path = "fail_download.txt"
    with open(fail_path, 'a') as f:
        for task in failed_tasks:
            f.write("%s\r\n" % task.url)
        f.flush()
    session.close()


if __name__ == "__main__":
    write_failed_url()
    # file = "month1-9.txt"
    # # file = "test.txt"
    # start_download(file)
    # log_f.close()



