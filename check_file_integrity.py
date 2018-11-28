import os
import traceback
import zipfile

from async_download import write_failed_url

host = "http://data.gdeltproject.org/gdeltv2/"

def check_file(root_dir):
    urls = []
    dir_list = os.listdir(root_dir)
    for item in dir_list:
        path = os.path.join(root_dir, item)
        if os.path.isdir(path):
            file_list = os.listdir(path)
            for fileName in file_list:
                file = os.path.join(path,fileName)
                if os.path.exists(file) and os.path.isfile(file):
                    try:
                        #校验看能不能作为zip文件打开
                        with zipfile.ZipFile(file, 'r') as f:
                            pass
                    except Exception as e:
                        url = host+fileName
                        print(url)
                        urls.append(url)
    return urls



dir = 'D:/async_gdlet/no_trans'
if __name__ == "__main__":
    urls = check_file(dir)
    print("总共%d个文件下载不完整" % len(urls))
    for url in urls:
        write_failed_url(url)







