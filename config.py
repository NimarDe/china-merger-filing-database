CONFIG = {
    'BASE_URL': 'https://www.samr.gov.cn/fldes/ajgs/jyaj/index.html',  # 主页URL
    'API_URL': 'https://www.samr.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit',  # API URL
    'START_PAGE': 1,  # 开始页码
    'END_PAGE': 3,   # 结束页码
    'RATE_LIMIT': 5,  # 请求间隔（秒）
    'PLAYWRIGHT_TIMEOUT': 30000,  # Playwright超时时间（毫秒）
    'MAX_RETRIES': 3,  # 最大重试次数
    'RANDOM_DELAY': {  # 随机延迟范围（秒）
        'MIN': 3,
        'MAX': 7
    },
    'DATABASE': {
        'path': '/Users/fengxinqing/Desktop/New project/data/db/',
        'name': 'cases.db'
    },
    'DOWNLOAD_PATH': '/Users/fengxinqing/Desktop/New project/data/attachments/',
    'EXCEL_PATH': '/Users/fengxinqing/Desktop/New project/data/cases.xlsx'
}