import sys, getopt, os, requests, random, userAgent, time, re, queue, logging
from threading import Thread
from lxml import etree
import csv

status = True

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def save(_file, keyword, url, i):
    """
    将结果进行保存
    :param _file: 已经打开的文件
    :param keyword: 关键词
    :param url: url
    :param i: 位置
    """
    # csv_file = _file.write(codecs.BOM_UTF8)
    spamwriter = csv.writer(_file, dialect='excel')
    spamwriter.writerow([keyword, url, i])


class Timing(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.start()

    def run(self):
        global status
        time.sleep(0.3)
        while status:
            logging.info('待爬去数为 {} 条'.format(self.tasks.qsize()))
            time.sleep(5)
            if self.tasks.qsize() == 0:
                logging.info('任务处理完毕 程序即将结束 请等候!')
                status = False


class Scrapy(Thread):
    def __init__(self, tasks, urls, putout_file):
        Thread.__init__(self)
        self.tasks = tasks
        self.urls = urls
        self.putout_file = putout_file
        self.start()

    def run(self):
        time.sleep(0.3)
        while status:
            try:
                task = self.tasks.get(timeout=3)
            except queue.Empty as empty:
                continue
            finally:
                try:
                    self.tasks.task_done()
                except ValueError as taskError:
                    pass

            agent = random.choice(userAgent.agents)
            new_url = 'https://www.baidu.com/s?wd=' + task
            try:
                response = requests.get(url=new_url, headers={'User-Agent': agent}, timeout=20)
            except requests.exceptions.SSLError as sslError:
                # TODO SSLERROR 错误暂不进行任何处理
                logging.error('SSL Error url->{}'.format(new_url))
                time.sleep(1)  # TODO 冻住不许走
                continue

            except requests.exceptions.Timeout as timeoutError:
                # TODO Timeout 错误暂不进行任何处理
                logging.error('Timeout Error url->{}'.format(new_url))
                time.sleep(1)  # TODO 冻住不许走
                continue

            # TODO 百度验证码 暂时未遇见过  先这样处理了
            if re.search('验证码', response.text):
                logging.error('写入原始文件中->{}.html'.format(new_url))
                with open('{}.html'.format(new_url), 'w') as html_file:
                    html_file.write(response.content)
                continue

            if response.status_code == 200:
                self.parser(response, task)
            else:
                logging.warning('状态码不正确 status_code->{}'.format(response.status_code))

    # 分析百度查询页面
    def parser(self, response, keyword):
        if not response:
            logging.error('Error keyword->{}'.format(keyword))
            return set()
        html = etree.HTML(response.content)

        i = 0
        for div in html.xpath('//div[@id="content_left"]/div'):
            # 过滤广告
            if div.xpath('div/font/a/span|a/span'):
                continue
            i += 1
            try:
                url = div.xpath('div[2]/a[1]/text()|div/div[2]/div[2]/a[1]/text()|/div[3]/a[1]/text()')[0]
                url = url.replace('http://', '')
                url = url.replace('https://', '')

                url = url.split('/')[0]
                url = url.replace('...', '')

                if re.search('com\.cn', url):
                    url = '.'.join(url.split('.')[-3:])
                else:
                    url = '.'.join(url.split('.')[-2:])
            except IndexError as e:
                url = None

            # print('INFO[DEBUG]: keyword->{} url->{}'.format(keyword, url))
            if url in self.urls:
                self.putout_file.flush()
                save(self.putout_file, keyword, url, i)
                logging.info('关键词: {}, 多少位->{}'.format(keyword, i))


class ThreadPool:
    def __init__(self, num_threads, urls, putout_file):
        self.tasks = queue.Queue()
        for _ in range(num_threads):
            Scrapy(self.tasks, urls=urls, putout_file=putout_file)
        Timing(self.tasks)

    def wait_completion(self):
        self.tasks.join()

    def loop_task(self, keyword):
        # print(keyword)
        self.tasks.put(keyword)


def usage():
    print('''
请输入参数:
    -h --help 查看帮助文档
    -u --url  指定url文件位置,  若不指定则使用当前文件夹下文件
    -k --key  指定关键词文件位置, 若不指定则使用当前文件夹下文件
    -o --optout 指定结果输出文件位置, 若不指定则输出到当前文件夹下. 同时将覆盖上一次查询内容
    ''')


if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    opts = ()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:k:o", ['help', 'url=', 'keyword=', 'putout='])
    except Exception as e:
        usage()

    url_fileName = os.path.join(BASE_DIR, 'url.txt')
    keyword_fileName = os.path.join(BASE_DIR, 'keyword.txt')
    putout_fileName = os.path.join(BASE_DIR, 'output.csv')

    for name, value in opts:
        if name in ['-h', '--help']:
            usage()
        if name in ['-u', '--url']:
            url_fileName = value
        if name in ['-k', '--keyword']:
            keyword_fileName = value
        if name in ['-o', '--putout']:
            putout_fileName = value

        logging.info('{}->{}, {}->{}, {}->{}'.format('url', url_fileName, 'keyword', keyword_fileName, 'putout',
                                                     putout_fileName))

    if not os.access(url_fileName, os.F_OK) or not os.access(url_fileName, os.F_OK):
        logging.error('url.txt 或 keyword.txt 文件不存在')
        exit(-1)

    url_file = open(url_fileName, 'r', encoding='utf8')
    putout_file = open(putout_fileName, 'w', encoding='utf8')

    with open(keyword_fileName, 'r', encoding='utf8') as _file:
        tasks = _file.readlines()
        # 去重
        tasks = list(set(tasks))
        urls = list(set([url.replace('\n', '').replace('\r', '') for url in url_file.readlines()]))

        logging.info('需查询的关键词共 {} 个, 域名共 {} 个'.format(len(tasks), len(urls)))
        pool = ThreadPool(4, urls, putout_file)

        for task in tasks:
            task = task.replace('\r', '')
            task = task.replace('\n', '')
            pool.loop_task(task)
        pool.wait_completion()
