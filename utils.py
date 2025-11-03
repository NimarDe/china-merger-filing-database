import re
from config import CONFIG
from datetime import datetime
import os

class DateParser:
    @staticmethod
    def parse_date_string(date_str):
        """解析各种格式的日期字符串"""
        try:
            # 移除所有空格
            date_str = date_str.strip()
            
            # 标准化年月日格式
            date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
            
            # 尝试不同的日期格式
            formats = [
                '%Y-%m-%d',  # 2025-03-20
                '%Y-%m-%d %H:%M:%S',  # 2025-03-20 12:00:00
                '%Y%m%d',  # 20250320
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            return None
            
        except Exception:
            return None

    @staticmethod
    def extract_date_range(text, pattern=None):
        """从文本中提取日期范围"""
        try:
            if pattern:
                match = re.search(pattern, text)
                if match:
                    date_range = match.group(1)
            else:
                date_range = text
                
            if '至' in date_range:
                start_date, end_date = date_range.split('至')
                return (
                    DateParser.parse_date_string(start_date.strip()),
                    DateParser.parse_date_string(end_date.strip())
                )
        except Exception as e:
            print(f"解析日期范围失败: {e}")
            pass
        return None, None

    @staticmethod
    def get_parser_for_type(page_type):
        """根据页面类型返回对应的解析方法名"""
        parser_map = {
            'samr': 'parse_samr_page',
            'beijing': 'parse_beijing_page',
            'chongqing': 'parse_chongqing_page',
            'shanghai': 'parse_shanghai_page',
            'guangdong': 'parse_guangdong_page'
        }
        return parser_map.get(page_type)

class FileHandler:
    @staticmethod
    def sanitize_filename(filename):
        """
        清理文件名中的非法字符
        """
        # 移除或替换非法字符
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()[:255]  # 限制文件名长度

    @staticmethod
    def ensure_directory(path):
        """
        确保目录存在
        """
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    @staticmethod
    def get_file_extension(url):
        """
        从URL中获取文件扩展名
        """
        extensions = ['.doc', '.docx', '.pdf']
        for ext in extensions:
            if url.lower().endswith(ext):
                return ext
        return '.doc'  # 默认扩展名

class PageTypeIdentifier:
    # 页面类型到地区的映射
    REGION_MAP = {
        'samr': '总局',
        'beijing': '北京',
        'chongqing': '重庆',
        'shanghai': '上海',
        'guangdong': '广东',
        'shaanxi': '陕西'
    }

    @staticmethod
    def identify_page_type(url):
        """识别页面类型"""
        if 'samr.gov.cn' in url:
            return 'samr'
        elif 'scjgj.beijing.gov.cn' in url:
            return 'beijing'
        elif 'scjgj.cq.gov.cn' in url:
            return 'chongqing'
        elif 'scjgj.sh.gov.cn' in url:
            return 'shanghai'
        elif 'amr.gd.gov.cn' in url:
            return 'guangdong'
        elif 'snamr.shaanxi.gov.cn' in url:
            return 'shaanxi'
        return None

    @staticmethod
    def get_region(page_type):
        """根据页面类型获取对应的地区"""
        return PageTypeIdentifier.REGION_MAP.get(page_type, '未知')