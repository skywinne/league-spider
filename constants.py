# ip代理中间件地址
PROXY_POOL_URL = 'http://47.107.158.70:5555/random'

# 如果是False, 不使用redis队列，会使用python的set存储指纹和请求
SCHEDULER_PERSIST = False

# 指纹是否持久化
# 默认为True 即持久化请求对象的指纹,达到断点续爬的目的
# 如果为False,则在爬虫结束的时候清空指纹库
# 可以在项目的settings.py中重新赋值进行覆盖
FP_PERSIST = True

# ***************************
# HEADERS 常量
# ***************************

# 指定客户端能够接收的内容类型
ACCEPT = 'application/json, text/plain, */*'
# 指定浏览器可以支持的web服务器返回内容压缩编码类型
ACCPPT_ENCODING = 'gzip, deflate'
# 浏览器可接受的语言
ACCEPT_LANGUAGE = 'zh-CN,zh;q=0.9'
# 表示是否需要持久连接。（HTTP 1.1默认进行持久连接）
CONNECTION = 'keep-alive'
# cookie暂时不用
COOKIE = 'locale=zh-CN; pgv_pvi=4935117824; pgv_si=s1862196224; __cfduid=d5a0b4785b08a69fc4d2411009ab0917e1554271310; connect.sid=s%3AiEXWvxBzTk-et1xXUNLZpy1PIY0p3JVe.51lHe21iamlj9Ezb7UZPuHYR5eS1rxxIxqo3omQb1FE'
# 指定请求的服务器的域名和端口号
HOST = 'www.1zplay.com'
# 先前网页的地址，当前请求网页紧随其后,即来路,暂时不用
REFERER = 'http://www.1zplay.com/index'
# User-Agent的内容包含发出请求的用户信息
# 电脑版
USER_AGENT_COMPUTER = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;"
]
# 手机版
USER_AGENT_PHONE = [
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Mobile Safari/537.36",
    "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    "NOKIA5700/ UCWEB7.0.2.37/28/999"
]

# *************************
# Mysql数据库配置
# *************************
# 公司服务器地址
GYD_HOST = '139.180.147.165'
# 个人阿里云服务器地址
MY_HOST = '47.107.158.70'
# 账号用户名
MYSQL_USER = 'bluesky'
# 账号密码
MYSQL_PASSWD = 'mysql'
# 数据库名
MYSQL_DB = 'test'
# 编码方式
CHARSET = 'utf8'
# 端口号
MYSQL_PORT = 3306

# *************************
# MongoDB数据库配置
# *************************
# 端口号
MONGODB_PORT = 27017

# *************************
# Redis 数据库配置
# *************************
# 队列名称
REDIS_QUEUE_NAME = 'league_url_list'
# 去重容器名称
REDIS_SET_NAME = 'league_fp_set'
# Redis端口
REDIS_PORT = 6379
# Redis数据库
REDIS_DB = 1
