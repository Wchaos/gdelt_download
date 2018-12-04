import re

from download.download import start_download
from setting import NO_TRANS_FILE_URL, NO_TRANS_RE, TRANS_FILE_URL, TRANS_RE
from toolkit.url_helper import get_urls_from_file

if __name__ == '__main__':
    mode = input("请选择下载模式：\r\n"
                 "'1'表示:从文件中找到目标url下载;'2'表示:从mysql开始下载，请确保mysql中有未完成的url")
    if str(mode) == '1':
        start_date = input("(日期格式为6位数字，如：2018年1月1日为20180101)\r\n"
                           "下载区间为，开始日期到结束日期的闭区间\r\n请输入开始日期：")
        end_date = input("请输入结束日期：")
        ack = input("即将下载[%s, %s]区间的文件，请确认Y/N：" % (start_date, end_date))
        if ack == 'Y' or ack == 'y':
            no_trans_file_name = NO_TRANS_FILE_URL.split("/")[-1]
            no_trans_urls = get_urls_from_file(no_trans_file_name, NO_TRANS_RE, start_date, end_date)
            trans_file_name = TRANS_FILE_URL.split("/")[-1]
            trans_urls = get_urls_from_file(trans_file_name, TRANS_RE, start_date, end_date)
            urls = no_trans_urls + trans_urls
            start_download(urls)
    elif str(mode) == '2':
        start_download()
    else:
        print("输入模式代码不对")
