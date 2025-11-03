import os
import logging
from config import CONFIG
import time
import requests

logger = logging.getLogger(__name__)

class AttachmentDownloader:
    def __init__(self, page):
        self.page = page

    def download_attachment(self, url, case_name, region):
        """下载附件"""
        try:
            if not url:
                return None

            # 创建地区特定的目录
            download_dir = os.path.join(CONFIG['DOWNLOAD_PATH'], region)
            os.makedirs(download_dir, exist_ok=True)

            # 生成文件路径
            file_path = os.path.join(download_dir, f"{case_name}.doc")
            
            # 使用requests下载文件
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            
            response = session.get(url, verify=False)  # 禁用SSL验证
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"成功下载附件到: {file_path}")
                return file_path
            else:
                logger.error(f"下载附件失败，状态码: {response.status_code}")
                return None
            
        except Exception as e:
            logger.error(f"下载附件失败 {url}: {e}")
            return None