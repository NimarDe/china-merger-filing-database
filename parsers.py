import logging
from bs4 import BeautifulSoup
import re
from utils import DateParser
from urllib.parse import urljoin
from datetime import datetime

logger = logging.getLogger(__name__)

class ParserException(Exception):
    """自定义解析器异常"""
    pass

class BaseParser:
    def __init__(self, soup, base_url=None):
        self.soup = soup
        self.base_url = base_url
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse(self):
        try:
            title = self.get_title()
            if not title:
                raise ParserException("未找到标题")
                
            start_date, end_date = self.get_date_range()
            if not start_date or not end_date:
                self.logger.warning(f"未找到完整的日期范围")
                
            attachment_url = self.get_attachment_url()
            if not attachment_url:
                self.logger.warning(f"未找到附件URL")

            result = {
                'case_name': title,
                'notice_start_date': start_date,
                'notice_end_date': end_date,
                'attachment_url': attachment_url
            }
            
            self.logger.info(f"成功解析页面: {title}")
            return result
            
        except ParserException as e:
            self.logger.error(f"解析失败: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"未预期的错误: {str(e)}", exc_info=True)
            return None

    def get_title(self):
        raise NotImplementedError

    def get_date_range(self):
        raise NotImplementedError

    def get_attachment_url(self):
        raise NotImplementedError

    def extract_date_range(self, text):
        """提取日期范围的通用方法"""
        if not text:
            return None, None
            
        # 移除多余的空白字符
        text = ' '.join(text.split())
        
        # 匹配各种日期格式
        patterns = [
            # 标准格式：2025-03-19 至 2025-03-28
            r'(\d{4}[-年]\d{1,2}[-月]\d{1,2}[日]?)\s*[至到-]\s*(\d{4}[-年]\d{1,2}[-月]\d{1,2}[日]?)',
            # 简化格式：2025.3.19-2025.3.28
            r'(\d{4}[.年]\d{1,2}[.月]\d{1,2}[日]?)\s*[-至]\s*(\d{4}[.年]\d{1,2}[.月]\d{1,2}[日]?)',
            # 年份简化格式：2025.3.19-3.28
            r'(\d{4}[.年]\d{1,2}[.月]\d{1,2}[日]?)\s*[-至]\s*(\d{1,2}[.月]\d{1,2}[日]?)',
            # 公示期限格式：公示期限：2025年3月19日-2025年3月28日
            r'公示期限：\s*(\d{4}[年-]\d{1,2}[月-]\d{1,2}[日]?)\s*[-至]\s*(\d{4}[年-]\d{1,2}[月-]\d{1,2}[日]?)',
            # 括号格式：（2025.3.19-2025.3.28）
            r'[（(](\d{4}[.年]\d{1,2}[.月]\d{1,2}[日]?)\s*[-至]\s*(\d{4}[.年]\d{1,2}[.月]\d{1,2}[日]?)[)）]'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                start_date, end_date = match.groups()
                
                # 处理简化格式（只有月日的结束日期）
                if len(end_date.split('.')[0]) <= 2:
                    year = start_date.split('.')[0]
                    end_date = f"{year}.{end_date}"
                
                # 统一日期格式
                try:
                    # 处理不同的分隔符
                    start_date = start_date.replace('年', '-').replace('月', '-').replace('日', '').replace('.', '-')
                    end_date = end_date.replace('年', '-').replace('月', '-').replace('日', '').replace('.', '-')
                    
                    # 确保日期格式正确
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
                    
                    return start_date, end_date
                except Exception as e:
                    logger.warning(f"日期格式转换失败: {e}")
                    continue
        
        return None, None

    def normalize_date(self, date_str):
        """标准化日期格式"""
        if not date_str:
            return None
            
        try:
            # 移除多余的空白字符
            date_str = date_str.strip()
            
            # 替换中文字符
            date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
            
            # 替换其他分隔符
            date_str = date_str.replace('.', '-').replace('/', '-')
            
            # 解析日期
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"日期格式标准化失败: {e}")
            return None

class SamrParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 从meta标签获取标题
        meta_title = self.soup.find('meta', {'name': 'ArticleTitle'})
        if meta_title and meta_title.get('content'):
            return meta_title['content']
            
        # 从h1标签获取标题
        h1_title = self.soup.find('h1')
        if h1_title:
            return h1_title.text.strip()
            
        # 从title标签获取标题
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
            if ' - ' in title:
                title = title.split(' - ')[0]
            return title.strip()
        return None

    def get_date_range(self):
        """获取公示期范围"""
        # 首先尝试从 zt_xilan_07 类中获取日期
        date_div = self.soup.find('div', class_='zt_xilan_07')
        if date_div:
            # 收集所有文本，包括嵌套在不同span标签中的文本
            text_parts = []
            for element in date_div.stripped_strings:
                text_parts.append(element.strip())
            text = ''.join(text_parts)
            
            # 移除所有空白字符
            text = ''.join(text.split())
            
            # 匹配日期模式
            pattern = r'公示期[：:]*(\d{4})年(\d{1,2})月(\d{1,2})日至(\d{4})年(\d{1,2})月(\d{1,2})日'
            match = re.search(pattern, text)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = match.groups()
                try:
                    start_date = f"{start_year}-{int(start_month):02d}-{int(start_day):02d}"
                    end_date = f"{end_year}-{int(end_month):02d}-{int(end_day):02d}"
                    return start_date, end_date
                except (ValueError, TypeError):
                    self.logger.warning(f"日期转换失败: {text}")
        
        # 如果上面的方法失败，尝试其他方法
        content = self.soup.find('div', class_='article-content')
        if not content:
            content = self.soup
            
        if content:
            # 收集所有文本，包括嵌套在不同标签中的文本
            text_parts = []
            for element in content.stripped_strings:
                text_parts.append(element.strip())
            text = ' '.join(text_parts)
            
            # 尝试匹配不同的日期格式
            patterns = [
                # 标准格式
                r'公示期[为是：:]\s*(\d{4})[年\-\.]\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?\s*[至到\-~]\s*(\d{4})[年\-\.]\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?',
                # 简化格式（年份可能省略）
                r'(\d{4})[年\-\.]\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?\s*[至到\-~]\s*(?:(\d{4})[年\-\.]?)?\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?',
                # 纯数字格式
                r'(\d{4})\-(\d{1,2})\-(\d{1,2})\s*[至到\-~]\s*(\d{4})\-(\d{1,2})\-(\d{1,2})',
                # 分开的格式
                r'(?:开始时间|公示开始日期)[：:]\s*(\d{4})[年\-\.]\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?.*?(?:结束时间|公示结束日期)[：:]\s*(\d{4})[年\-\.]\s*(\d{1,2})[月\-\.]\s*(\d{1,2})[日号]?'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    groups = match.groups()
                    # 处理年份可能省略的情况
                    if len(groups) == 6 and not groups[3]:  # 结束年份被省略
                        start_year, start_month, start_day = groups[0:3]
                        end_year, end_month, end_day = start_year, groups[4], groups[5]
                    else:
                        start_year, start_month, start_day, end_year, end_month, end_day = groups
                    
                    try:
                        start_date = f"{start_year}-{int(start_month):02d}-{int(start_day):02d}"
                        end_date = f"{end_year}-{int(end_month):02d}-{int(end_day):02d}"
                        return start_date, end_date
                    except (ValueError, TypeError):
                        continue
                    
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 在正文区域查找附件链接
        content = self.soup.find('div', class_='article-content')
        if content:
            # 查找所有链接
            for link in content.find_all('a', href=True):
                href = link['href'].lower()
                # 检查链接是否是文档
                if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                    if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                        return urljoin(self.base_url, link['href'])
                        
        # 在整个页面中查找
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
        return None

class BeijingParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 尝试从 h2 标签获取标题
        h2_title = self.soup.find('h2')
        if h2_title:
            return h2_title.text.strip()
            
        # 如果找不到 h2，尝试从 title 标签获取
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
            # 移除可能的后缀（如"_北京市市场监督管理局"）
            if '_' in title:
                title = title.split('_')[0]
            return title
        return None

    def get_date_range(self):
        """获取公示期范围"""
        # 尝试从div_zhengwen中查找日期信息
        content_div = self.soup.find('div', id='div_zhengwen')
        if content_div:
            # 获取所有文本节点
            text_nodes = [node.strip() for node in content_div.stripped_strings]
            full_text = ' '.join(text_nodes)
            
            # 尝试多个日期格式
            patterns = [
                r'公示期[：:]\s*([\d年\-月]+\d+日至[\d年\-月]+\d+日)',
                r'公示时间[：:]\s*([\d年\-月]+\d+日至[\d年\-月]+\d+日)',
                r'公示日期[：:]\s*([\d年\-月]+\d+日至[\d年\-月]+\d+日)',
                r'([2\d]{4}年\d{1,2}月\d{1,2}日.*?至.*?[2\d]{4}年\d{1,2}月\d{1,2}日)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, full_text)
                if match:
                    date_text = match.group(1)
                    dates = DateParser.extract_date_range(date_text)
                    if all(dates):
                        return dates
                    
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 查找包含"附件"文本的div
        attachment_div = self.soup.find('div', string=lambda x: x and '附件：' in x if x else False)
        if attachment_div:
            link = attachment_div.find('a', href=True)
            if link:
                return urljoin(self.base_url, link['href'])
                
        # 查找所有链接
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            # 检查链接文本和URL
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
                    
        return None

class ChongqingParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 从meta标签获取标题
        meta_title = self.soup.find('meta', {'name': 'ArticleTitle'})
        if meta_title and meta_title.get('content'):
            return meta_title['content']
            
        # 从title标签获取标题（去掉后缀）
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
            if ' - ' in title:
                title = title.split(' - ')[0]
            return title.strip()
        return None

    def get_date_range(self):
        """获取公示期范围"""
        # 从Description中获取日期范围
        meta_desc = self.soup.find('meta', {'name': 'Description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc['content']
            match = re.search(r'公示日期：(.*?至.*?)(?:联系邮箱|$)', desc)
            if match:
                return DateParser.extract_date_range(match.group(1))
                
        # 从正文内容中查找
        content = self.soup.find('div', class_='zwxl-article')
        if content:
            text = content.get_text()
            match = re.search(r'公示日期：(.*?至.*?)(?:联系邮箱|$)', text)
            if match:
                return DateParser.extract_date_range(match.group(1))
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 从JavaScript变量中获取附件链接
        script_tags = self.soup.find_all('script')
        for script in script_tags:
            if script.string and 'hasFJ' in script.string:
                match = re.search(r'hasFJ\s*=\s*\'<a\s+href="([^"]+)"', script.string)
                if match:
                    return urljoin(self.base_url, match.group(1))
                    
        # 在正文区域查找附件链接
        content = self.soup.find('div', class_='zwxl-article')
        if content:
            # 查找所有链接
            for link in content.find_all('a', href=True):
                href = link['href'].lower()
                # 检查链接是否是文档
                if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                    if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                        return urljoin(self.base_url, link['href'])
                        
        # 在整个页面中查找
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
        return None

class ShanghaiParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 从meta标签获取标题
        meta_title = self.soup.find('meta', {'name': 'ArticleTitle'})
        if meta_title and meta_title.get('content'):
            return meta_title['content']
            
        # 从div标签获取标题
        title_div = self.soup.find('div', id='ivs_title')
        if title_div:
            return title_div.text.strip()
            
        # 从h1标签获取标题
        h1_title = self.soup.find('h1')
        if h1_title:
            return h1_title.text.strip()
        return None

    def get_date_range(self):
        content_div = self.soup.find('div', id='ivs_content')
        if not content_div:
            return None, None
            
        text = content_div.get_text()
        date_pattern = r'公示期：(\d{4})年(\d{1,2})月(\d{1,2})日至(\d{4})年(\d{1,2})月(\d{1,2})日'
        match = re.search(date_pattern, text)
        
        if match:
            start_year, start_month, start_day, end_year, end_month, end_day = match.groups()
            start_date = f"{start_year}-{int(start_month):02d}-{int(start_day):02d}"
            end_date = f"{end_year}-{int(end_month):02d}-{int(end_day):02d}"
            return start_date, end_date
            
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 查找包含"附件"文本的td
        for td in self.soup.find_all('td'):
            if td.text.strip().startswith('附件：'):
                link = td.find('a', href=True)
                if link:
                    return urljoin(self.base_url, link['href'])
                    
        # 查找所有链接
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            # 检查链接是否是文档
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
        return None

class GuangdongParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 从meta标签获取标题
        meta_title = self.soup.find('meta', {'name': 'ArticleTitle'})
        if meta_title and meta_title.get('content'):
            return meta_title['content']
            
        # 从h1标签获取标题
        h1_title = self.soup.find('h1', class_='article_t')
        if h1_title:
            return h1_title.text.strip()
        return None

    def get_date_range(self):
        """获取公示期范围"""
        content = self.soup.find('div', class_='article_con')
        if content:
            text = content.get_text()
            date_pattern = r'公示期：(\d{4})年(\d{1,2})月(\d{1,2})至(\d{4})年(\d{1,2})月(\d{1,2})日'
            match = re.search(date_pattern, text)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = match.groups()
                start_date = f"{start_year}-{int(start_month):02d}-{int(start_day):02d}"
                end_date = f"{end_year}-{int(end_month):02d}-{int(end_day):02d}"
                return start_date, end_date
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 查找带有特定class的链接
        attachment = self.soup.find('a', class_='nfw-cms-attachment')
        if attachment and attachment.get('href'):
            return urljoin(self.base_url, attachment['href'])
            
        # 查找所有链接
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
        return None

class ShaanxiParser(BaseParser):
    def get_title(self):
        """获取标题"""
        # 从meta标签获取标题
        meta_title = self.soup.find('meta', {'name': 'ArticleTitle'})
        if meta_title and meta_title.get('content'):
            return meta_title['content']
            
        # 从public-title-nav的div标签获取标题
        title_div = self.soup.find('div', class_='public-title-nav')
        if title_div:
            title = title_div.find('div', class_='title')
            if title:
                return title.text.strip()
                
        # 从title标签获取标题
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
            if '-' in title:
                return title.split('-')[0].strip()
        return None

    def get_date_range(self):
        """获取公示期范围"""
        content = self.soup.find('div', class_='news-content')
        if content:
            text = content.get_text()
            date_pattern = r'公\s*示\s*期：(\d{4})年(\d{1,2})月(\d{1,2})日至(\d{4})年(\d{1,2})月(\d{1,2})日'
            match = re.search(date_pattern, text)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = match.groups()
                start_date = f"{start_year}-{int(start_month):02d}-{int(start_day):02d}"
                end_date = f"{end_year}-{int(end_month):02d}-{int(end_day):02d}"
                return start_date, end_date
        return None, None

    def get_attachment_url(self):
        """获取附件URL"""
        # 查找带有特定class的链接
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                if '经营者集中' in link.text or '公示表' in link.text or '附件' in link.text:
                    return urljoin(self.base_url, link['href'])
                    
        # 查找所有链接
        for link in self.soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(ext in href for ext in ['.doc', '.docx', '.pdf', '.zip', '.rar']):
                return urljoin(self.base_url, link['href'])
        return None

def create_parser(page_type, soup, base_url=None):
    """创建对应的解析器实例"""
    parser_map = {
        'samr': SamrParser,
        'beijing': BeijingParser,
        'chongqing': ChongqingParser,
        'shanghai': ShanghaiParser,
        'guangdong': GuangdongParser,
        'shaanxi': ShaanxiParser
    }
    
    parser_class = parser_map.get(page_type)
    if not parser_class:
        logger.error(f"未知的页面类型: {page_type}")
        return None
        
    return parser_class(soup, base_url)