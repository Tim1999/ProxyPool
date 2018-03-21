import time
from multiprocessing import Process
import asyncio
import aiohttp
try:
    from aiohttp.errors import ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError
except:
    from aiohttp import ClientProxyConnectionError as ProxyConnectionError,ServerDisconnectedError,ClientResponseError,ClientConnectorError,ClientOSError
from proxypool.db import RedisClient
from proxypool.error import ResourceDepletionError
from proxypool.getter import FreeProxyGetter
from proxypool.setting import *
from asyncio import TimeoutError


class ValidityTester(object):
    """代理有效性测试器"""

    # 测试用的url
    test_api = TEST_API

    def __init__(self):
        # 为测试有效性的代理ip
        self._raw_proxies = None
        # 可用的代理ip
        self._usable_proxies = []

    def set_raw_proxies(self, proxies):
        """
        设置为测试有效性的代理ip列表
        :param proxies:代理ip列表
        :return:
        """
        self._raw_proxies = proxies
        self._conn = RedisClient()

    async def test_single_proxy(self, proxy):
        """
        测试一个代理，如果有效就添加到_usable_proxies列表中
        :param proxy: 代理
        :return:
        """
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    if isinstance(proxy, bytes):
                        proxy = proxy.decode('utf-8')
                    real_proxy = 'http://' + proxy
                    print('Testing', proxy)
                    async with session.get(self.test_api, proxy=real_proxy, timeout=get_proxy_timeout) as response:
                        if response.status == 200:
                            self._conn.put(proxy)
                            print('Valid proxy', proxy)
                except (ProxyConnectionError, TimeoutError, ValueError,ClientOSError):
                    print('Invalid proxy', proxy)
        except (ServerDisconnectedError, ClientResponseError,ClientConnectorError) as s:
            print('Invalid proxy', proxy)

    def test(self):
        """
        异步测试所用代理
        """
        print('ValidityTester is working')
        try:
            loop = asyncio.get_event_loop()
            tasks = [self.test_single_proxy(proxy) for proxy in self._raw_proxies]
            loop.run_until_complete(asyncio.wait(tasks))
        except ValueError:
            print('Async Error')


class PoolAdder(object):
    """
    添加代理到代理池
    """

    def __init__(self, threshold):
        # 临界值
        self._threshold = threshold
        self._conn = RedisClient()
        self._tester = ValidityTester()
        # 免费代理爬虫
        self._crawler = FreeProxyGetter()

    def is_over_threshold(self):
        """
        判断代理数量是否超出临界值。
        """
        if self._conn.queue_len >= self._threshold:
            return True
        else:
            return False

    def add_to_queue(self):
        print('PoolAdder is working')
        proxy_count = 0
        while not self.is_over_threshold():
            for callback_label in range(self._crawler.__CrawlFuncCount__):
                callback = self._crawler.__CrawlFunc__[callback_label]
                raw_proxies = self._crawler.get_raw_proxies(callback)
                # test crawled proxies
                self._tester.set_raw_proxies(raw_proxies)
                self._tester.test()
                proxy_count += len(raw_proxies)
                if self.is_over_threshold():
                    print('IP is enough, waiting to be used')
                    break
            if proxy_count == 0:
                raise ResourceDepletionError


class Schedule(object):
    """动态代理池调度器类，调用各种类实现动态维护代理池"""

    @staticmethod
    def valid_proxy(cycle=VALID_CHECK_CYCLE):
        """
        Get half of proxies which in redis
        """
        # 实例化redis客户端
        conn = RedisClient()
        # 实例化ip有效性测试器
        tester = ValidityTester()

        while True:
            print('Refreshing ip')
            # 获取redis数据库列表中一半的个数
            count = int(0.5 * conn.queue_len)

            if count == 0:
                print('Waiting for adding')
                # 等待一个周期时间
                time.sleep(cycle)
                continue

            # 获取redis数据库列表中一半的代理
            raw_proxies = conn.get(count)
            # 添加到为测试有效性列表中
            tester.set_raw_proxies(raw_proxies)
            # 测试所用代理
            tester.test()
            # 等待一个周期时间
            time.sleep(cycle)

    @staticmethod
    def check_pool(lower_threshold=POOL_LOWER_THRESHOLD,
                   upper_threshold=POOL_UPPER_THRESHOLD,
                   cycle=POOL_LEN_CHECK_CYCLE):
        """
        如果代理的数量小于lower_threshold,添加代理
        :param lower_threshold: 上限
        :param upper_threshold: 下限
        :param cycle: 检查周期
        :return:
        """
        conn = RedisClient()
        adder = PoolAdder(upper_threshold)
        while True:
            if conn.queue_len < lower_threshold:
                adder.add_to_queue()
            time.sleep(cycle)

    def run(self):
        """
        运行调度器
        :return:
        """
        print('Ip processing running')
        # 创建ip地址有效性测试进程
        valid_process = Process(target=Schedule.valid_proxy)
        # 创建检测代理池数量进程
        check_process = Process(target=Schedule.check_pool)
        # 启动子进程
        valid_process.start()
        check_process.start()
