# -*- coding: utf-8 -*-
"""
百度百科爬虫。
主要用于爬取百科中所包含的词条以及相应的basic-info，并保存html文件
运行命令格式：python *.py in_file_name out_file_name relation_file_name indication_file_name sleep_time start_line
"""

__author__ = "liushen"

import requests
import sys
import logging
# import lxml.html
import time

from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename="baidu_baike_crawler_all.log",
                    filemode="w+"
                    )

sleep_time = 3
start_line = 1
end_line = 999999999
in_file_name = "input/baidu_baike_id_title_0.txt"
out_file_name = "output/baidu_baike_item_basic-info.txt"
relation_file_name = "output/relation_triple.txt"
indication_file_name = "output/relation_indication.txt"

if len(sys.argv) > 1:
    in_file_name = sys.argv[1]

if len(sys.argv) > 2:
    out_file_name = sys.argv[2]

if len(sys.argv) > 3:
    relation_file_name = sys.argv[3]

if len(sys.argv) > 4:
    indication_file_name = sys.argv[4]

if len(sys.argv) > 5:
    sleep_time = float(sys.argv[5])

if len(sys.argv) > 6:
    start_line = int(sys.argv[6])

relation_file = open(relation_file_name, "a")
indication_file = open(indication_file_name, "a")

session = requests.session()
session.headers['User-Agent'] = \
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36"
session.headers['Accept'] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
session.headers['Accept-Language'] = "zh-CN,zh;q=0.8"
session.headers['Host'] = "wapbaike.baidu.com"
session.headers['Referer'] = "http://wapbaike.baidu.com/"

cookies = session.get('http://wapbaike.baidu.com/').cookies


def get_page(baike_id, retries=3):
    """
    爬取百度百科wap页面。
    :param baike_id:    百科词条的id
    :param retries:     最多重复爬取次数（失败时）
    :return:
    """
    global session, sleep_time, cookies

    url = "http://wapbaike.baidu.com/view/%d.htm" % baike_id

    retry_count = 0
    src = ""
    while retry_count < retries:  # 如果爬取失败，在重试次数内重新尝试爬取
        try:
            res = requests.get(url, timeout=30, cookies=cookies, headers=session.headers)
            cookies = res.cookies

            if res.ok:
                # 成功获取
                src = res.content
                break
            elif res.status_code == 404:
                logging.error("Failed(404): %d" % baike_id)
                break
        except:
            logging.error("Sth wrong: %d" % baike_id)
            pass
        finally:
            retry_count += 1

    return src


def parse_src(src):
    """
    解析百度百科网页，获得title并返回。
    :param src: 网页源代码
    :return:    网页的title，即对应的词条名
    """
    if src is None or len(src) == 0:
        return "", ""

    # page_lxml = lxml.html.fromstring(src)
    soup = BeautifulSoup(src)
    # print soup.prettify()
    item = ""
    basic_info = dict()
    
    # print soup.select('div .card-info > p')
    raw_item = soup.select('#main > h1')
    if len(raw_item) == 0:
        return "", ""

    item = raw_item[0].contents[0].strip()
    card_info = soup.select('div .card-info > p')
    for p in card_info:
        # key = p.strong.string.strip()
        key = ""
        for content in p.strong.contents:
            try:
                key += content.string.strip()
            except:
                pass
        key = key.strip()
        key = key.strip(u"：:")
        value = ""
        for content in p.contents[1:]:
            try:
                tail = content.string.strip()
                value += tail
                if content.name	== 'a' and item != tail:
                    # print "%s,%s,%s" % (item, key, content.string)
                    key = key.replace(u' ', '')
                    key = key.replace(u'    ', '')
                    key = key.replace(u'　　', '')
                    relation_file.write("%s--->%s--->%s\n" % (item.encode("utf-8"), key.encode("utf-8"), tail.encode("utf-8")))
                    indication_file.write("%s\n" % (key.encode("utf-8")))
                    relation_file.flush()
                    indication_file.flush()
            except:
                pass
	value = value.strip()
        basic_info[key] = value	
	# print key + "\t" + value
    
    return item, basic_info


if __name__ == "__main__":
    id_list = []
    in_file = open(in_file_name)
    
    count = 1
    for line in in_file:
        if count < start_line:
            count += 1
            continue
        line = int(line.split("\t")[0].strip())
        id_list.append(line)
        
    out_file = open(out_file_name, "a")
    for id in id_list:
        time.sleep(sleep_time)
        item, basic_info = parse_src(get_page(id))
        if len(item) == 0:
            logging.error("%d: None" % id)
            continue
        else:
            logging.info("%d: %s" % (id, item))
        out_file.write("[%s]======%d" % (item.encode("utf-8"), len(basic_info)))
        for (key, value) in basic_info.items():
            key = key.replace(u' ', '')
            key = key.replace(u'    ', '')
            key = key.replace(u'　　', '')
            out_file.write("####%s==>>%s" % (key.encode("utf-8"), value.encode("utf-8")))
        out_file.write("\n")
        out_file.flush()	
        print "%d\t%s" % (id, item.encode("utf-8"))

    logging.info("done")
    in_file.close()
    out_file.close()
    relation_file.close()
    indication_file.close()
