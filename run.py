from proxypool.api import app
from proxypool.schedule import Schedule

def main():
    """
    程序入口，运行程序
    :return:
    """

    # 实例化调度器
    s = Schedule()
    # 运行调度器
    s.run()
    # 运行flask实例
    app.run()


if __name__ == '__main__':
    main()

