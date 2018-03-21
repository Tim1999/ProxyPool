import redis
from proxypool.error import PoolEmptyError
from proxypool.setting import HOST, PORT, PASSWORD


class RedisClient(object):
    """redis数据库客户端类,对数据库进行增删查改"""

    def __init__(self, host=HOST, port=PORT):
        if PASSWORD:
            self._db = redis.Redis(host=host, port=port, password=PASSWORD)
        else:
            self._db = redis.Redis(host=host, port=port)

    def get(self, count=1):
        """
        从redis数据库中获取指定数量的代理ip并删除
        :param count:代理ip的数量
        :return:代理ip列表
        """
        # 获取redis数据库中键为'proxies'的列表0到count-1之间的值
        proxies = self._db.lrange("proxies", 0, count - 1)
        # 截取redis数据库中键为'proxies'的列表,保留count到-1（最后）的值
        self._db.ltrim("proxies", count, -1)
        return proxies

    def put(self, proxy):
        """
        往redis数据库中列表的右边添加代理ip
        """
        self._db.rpush("proxies", proxy)

    def pop(self):
        """
        返回并删除列表中的右边的一个值
        :return: 代理ip的二进制
        """
        try:
            return self._db.rpop("proxies").decode('utf-8')
        except:
            raise PoolEmptyError

    def get_new(self):
        """获取最新的可用代理并把代理重新发到列表头"""
        try:
            proxy = self._db.rpop("proxies")
            self._db.lpush('proxies',proxy)
            return proxy
        except:
            raise PoolEmptyError

    @property
    def queue_len(self):
        """
        获取redies数据库中列表的长度
        :return:列表的长度
        """
        return self._db.llen("proxies")

    def flush(self):
        """
        清空redis数据库
        :return:
        """
        self._db.flushall()


if __name__ == '__main__':
    conn = RedisClient()
    print(conn.pop())
