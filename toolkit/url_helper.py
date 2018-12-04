import asyncio
import re

from toolkit.common_tool import get_cur_time
from toolkit.orm import get_session, GdeltTask


def get_urls_from_file(file_name,pattern,start_date,end_date):
    urls = []
    with open(file_name, mode='r') as f:
        for line in f.readlines():
            line = line.split(' ')
            if len(line) == 3:
                url = line[2].strip()
                if re.search(pattern, url):
                    url_date = url.split("/")[-1][0:8]
                    if start_date <= url_date <= end_date:
                        urls.append(url)
    return urls

def write_urls_to_db(urls):
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(20)
    tasks = [asyncio.ensure_future(write_url_to_db(url,semaphore)) for url in urls]
    loop.run_until_complete(asyncio.wait(tasks))


async def write_url_to_db(url,semaphore):
    async with semaphore:
        session = get_session()
        try:
            file_name = ''
            if url.rindex('/') >= 0:
                file_name = url[url.rindex('/') + 1:]
            task = GdeltTask(url=url, file_name=file_name, file_date=file_name[0:8])
            session.add(task)
            session.commit()
        except Exception as e:
            print("[%s]urls写入数据库时，发生异常\r\n%s\r\n" % (get_cur_time(), str(e)))
        finally:
            session.close()
