import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Case, Base
from config import CONFIG
import logging
from downloader import AttachmentDownloader
from exporter import DataExporter
from utils import PageTypeIdentifier
from parsers import create_parser, BeijingParser, ShanghaiParser, ChongqingParser, GuangdongParser, ShaanxiParser, SamrParser
from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright
from urllib.parse import urljoin
import asyncio
import urllib.parse
import aiohttp
import aiofiles
import sqlite3
from sqlalchemy.sql import text

# Configure logger
logger = logging.getLogger(__name__)

class SamrScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.engine = create_engine(f"sqlite:///data/cases.db")
        
        # 创建必要的目录
        os.makedirs('data', exist_ok=True)
        os.makedirs(os.path.dirname(CONFIG['EXCEL_PATH']), exist_ok=True)
        
        self.db_session = sessionmaker(bind=self.engine)()

    def get_case_list(self):
        try:
            response = self.session.get(CONFIG['BASE_URL'])
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            # 这里需要根据实际网页结构调整选择器
            cases = soup.find_all('a', href=True)  # 需要根据实际HTML结构修改
            return cases
        except Exception as e:
            logging.error(f"获取案件列表失败: {e}")
            return []

    def parse_case_detail(self, url):
        try:
            time.sleep(CONFIG['RATE_LIMIT'])  # 限速
            response = self.session.get(url)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            # 解析具体信息，需要根据实际网页结构调整
            # 返回解析后的数据
            return {
                'case_name': '',  # 需要实现
                'notice_start_date': None,  # 需要实现
                'notice_end_date': None,  # 需要实现
                'attachment_url': ''  # 需要实现
            }
        except Exception as e:
            logging.error(f"解析案件详情失败 {url}: {e}")
            return None

    async def download_attachment(self, url, case_name):
        """下载附件"""
        try:
            # 生成安全的文件名（保留中文和基本标点）
            safe_title = case_name  # 使用传入的case_name作为文件名
            # 替换Windows文件名中的非法字符
            illegal_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', ' ']
            for char in illegal_chars:
                safe_title = safe_title.replace(char, '')
            
            safe_title = safe_title.strip()
            if len(safe_title) > 100:  # 限制文件名长度
                safe_title = safe_title[:97] + "..."
            
            # 确保下载目录存在
            download_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'attachments')
            os.makedirs(download_path, exist_ok=True)
            
            # 获取文件扩展名
            file_ext = '.doc'  # 默认扩展名
            if url.lower().endswith('.docx'):
                file_ext = '.docx'
            elif url.lower().endswith('.pdf'):
                file_ext = '.pdf'
            
            # 构建文件名（使用案件名称）
            file_name = f"{safe_title}{file_ext}"
            file_path = os.path.join(download_path, file_name)
            
            # 检查文件是否已存在
            if os.path.exists(file_path):
                logger.info(f"附件已存在，跳过下载: {file_name}")
                return file_name
            
            # 如果文件已存在，添加数字后缀
            counter = 1
            while os.path.exists(file_path):
                name_parts = os.path.splitext(file_name)
                file_name = f"{name_parts[0]}_{counter}{name_parts[1]}"
                file_path = os.path.join(download_path, file_name)
                counter += 1

            # 使用aiohttp下载文件
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive'
                }, ssl=False) as response:
                    if response.status == 200:
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(await response.read())
                        logger.info(f"附件已保存为: {file_name}")  # 只记录文件名
                        return file_name  # 只返回文件名
                    else:
                        logger.error(f"下载附件失败，状态码: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"下载附件时发生错误: {str(e)}")
            return None

    async def export_to_excel(self):
        """导出数据到Excel"""
        try:
            # 获取所有案件数据
            cases = self.db_session.query(Case).all()
            
            # 准备数据
            data = []
            for case in cases:
                data.append({
                    '案件名称': case.title,
                    '案件类型': case.case_type,
                    '案件状态': case.status,
                    '立案日期': case.filing_date,
                    '结案日期': case.closing_date,
                    '案件链接': case.url,
                    '案件来源': case.source,
                    '案件地区': case.region,
                    '案件描述': case.description,
                    '附件链接': case.attachment_url,
                    '更新时间': case.updated_at
                })
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_path = f'data/cases_{timestamp}.xlsx'
            csv_path = f'data/cases_{timestamp}.csv'
            
            # 保存为Excel
            df.to_excel(excel_path, index=False, engine='openpyxl')
            logging.info(f"数据已导出到Excel: {excel_path}")
            
            # 保存为CSV
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logging.info(f"数据已导出到CSV: {csv_path}")
            
            return True
        except Exception as e:
            logging.error(f"导出数据失败: {str(e)}")
            return False

    async def run(self):
        cases = self.get_case_list()
        for case in cases:
            case_data = self.parse_case_detail(case['href'])
            if not case_data:
                continue

            # 检查数据库中是否已存在
            existing_case = self.db_session.query(Case).filter_by(
                case_name=case_data['case_name']
            ).first()
            
            if existing_case:
                continue

            # 下载附件
            attachment_path = await self.download_attachment(
                case_data['attachment_url'], 
                case_data['case_name']
            )

            # 保存到数据库
            new_case = Case(
                case_name=case_data['case_name'],
                notice_start_date=case_data['notice_start_date'],
                notice_end_date=case_data['notice_end_date'],
                source_url=case['href'],
                attachment_path=attachment_path
            )
            self.db_session.add(new_case)
            self.db_session.commit()

        # 导出到Excel
        await self.export_to_excel()

    def setup_database(self):
        """设置数据库连接"""
        self.engine = create_engine(f"sqlite:///data/cases.db")
        self.session = sessionmaker(bind=self.engine)()


class CaseScraper:
    def __init__(self):
        """初始化爬虫"""
        self.browser = None
        self.playwright = None
        self.context = None
        self.page = None
        self.current_page = None
        self.engine = None
        self.logger = logging.getLogger(__name__)
        self.setup_database()
        
        # 创建必要的目录
        os.makedirs('data/attachments', exist_ok=True)
        os.makedirs('data/exports', exist_ok=True)

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        self.logger.info("初始化 Playwright...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch()
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        self.page = await self.context.new_page()
        self.page.set_default_timeout(180000)  # 设置超时时间为180秒
        self.logger.info("Playwright 初始化完成")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法"""
        self.logger.info("清理 Playwright 资源...")
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("Playwright 资源清理完成")

    async def fetch_page(self, url):
        """使用requests获取页面内容"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        try:
            # 使用aiohttp或requests
            response = requests.get(url, headers=headers, verify=False)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return response.text
        except Exception as e:
            self.logger.error(f"获取页面失败: {str(e)}")
            return None

    async def parse_list_page_playwright(self, url, page_no):
        """使用Playwright解析列表页
        
        Args:
            url: 基础URL
            page_no: 目标页码
        """
        max_retries = 3
        retry_count = 0
        
        # 检查page对象是否正确初始化
        if not self.page:
            self.logger.error("Playwright page 对象未初始化")
            return None
        
        while retry_count < max_retries:
            try:
                # 第一种情况：首次访问或current_page为None
                if self.current_page is None:
                    self.logger.info(f"首次访问页面: {url}")
                    try:
                        await self.page.goto(url, wait_until='domcontentloaded', timeout=180000)
                        await asyncio.sleep(5)
                        self.current_page = 1
                        self.logger.info(f"初始化当前页码为: {self.current_page}")
                    except Exception as e:
                        self.logger.error(f"访问页面失败: {str(e)}")
                        retry_count += 1
                        await asyncio.sleep(5)
                        continue
                    
                    # 如果起始页不是第1页，需要翻到起始页
                    if page_no > 1:
                        self.logger.info(f"需要从第1页翻到起始页: {page_no}")
                        for i in range(page_no - 1):
                            success = await self._turn_to_next_page()
                            if not success:  # 如果翻页失败
                                return None
                            await asyncio.sleep(2)  # 翻页后短暂等待
                
                # 第二种情况：从当前页继续翻一页
                elif page_no > self.current_page:
                    self.logger.info(f"从当前页 {self.current_page} 翻到下一页 {page_no}")
                    success = await self._turn_to_next_page()
                    if not success:  # 如果翻页失败
                        return None
                
                # 等待内容加载并获取案件列表
                try:
                    await self.page.wait_for_selector('.content-3-left-text', timeout=180000)
                    elements = await self.page.query_selector_all('.content-3-left-text a')
                    
                    # 处理每个案件链接
                    results = []
                    for element in elements:
                        try:
                            title = await element.inner_text()
                            href = await element.get_attribute('href')
                            
                            if href and title:
                                if not href.startswith('http'):
                                    href = urljoin(url, href)
                                results.append({
                                    'title': title.strip(),
                                    'url': href
                                })
                        except Exception as e:
                            self.logger.error(f"处理案件链接时出错: {str(e)}")
                            continue
                    
                    if results:
                        self.logger.info(f"第 {self.current_page} 页成功获取 {len(results)} 个案件链接")
                        return results
                    else:
                        self.logger.error(f"第 {self.current_page} 页未获取到任何案件链接")
                        retry_count += 1
                        await asyncio.sleep(5)
                
                except Exception as e:
                    self.logger.error(f"等待页面元素或获取案件链接失败: {str(e)}")
                    retry_count += 1
                    await asyncio.sleep(5)
                    continue
            
            except Exception as e:
                self.logger.error(f"解析列表页失败: {str(e)}")
                retry_count += 1
                await asyncio.sleep(5)
                # 如果发生错误，重置current_page以便重新开始
                self.current_page = None
        
        self.logger.error(f"达到最大重试次数 ({max_retries})，放弃获取页面")
        return None

    async def process_case_page(self, url):
        """处理案件详情页面"""
        try:
            logger.info(f"正在处理案件页面: {url}")
            
            # 获取页面内容
            html_content = await self.fetch_page(url)
            if not html_content:
                logger.error(f"获取页面内容失败: {url}")
                return None
                
            # 识别页面类型
            page_type = PageTypeIdentifier.identify_page_type(url)
            if not page_type:
                logger.error(f"无法识别页面类型: {url}")
                return None
                
            region = PageTypeIdentifier.get_region(page_type)
            logger.info(f"页面类型: {page_type}, 地区: {region}")
            
            # 解析页面内容
            result = self.parse_detail_page(html_content, url)
            if not result:
                logger.error("页面解析失败")
                return None
                
            # 添加来源URL和地区信息
            result['source_url'] = url
            result['region'] = region
            
            logger.info("成功解析案件数据")
            return result
            
        except Exception as e:
            logger.error(f"处理案件页面失败: {str(e)}")
            return None
            
    def parse_detail_page(self, html_content, source_url):
        """解析详情页面"""
        try:
            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 识别页面类型
            page_type = PageTypeIdentifier.identify_page_type(source_url)
            if not page_type:
                logger.error(f"无法识别页面类型: {source_url}")
                return None
                
            # 创建对应的解析器
            parser = create_parser(page_type, soup, source_url)
            if not parser:
                logger.error(f"无法为页面类型 {page_type} 创建解析器")
                return None
                
            # 解析页面内容
            return parser.parse()
            
        except Exception as e:
            logger.error(f"解析详情页面失败: {str(e)}")
            return None
            
    async def download_attachment(self, url, case_name):
        """下载附件"""
        try:
            # 生成安全的文件名（保留中文和基本标点）
            safe_title = case_name  # 使用传入的case_name作为文件名
            # 替换Windows文件名中的非法字符
            illegal_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', ' ']
            for char in illegal_chars:
                safe_title = safe_title.replace(char, '')
            
            safe_title = safe_title.strip()
            if len(safe_title) > 100:  # 限制文件名长度
                safe_title = safe_title[:97] + "..."
            
            # 确保下载目录存在
            download_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'attachments')
            os.makedirs(download_path, exist_ok=True)
            
            # 获取文件扩展名
            file_ext = '.doc'  # 默认扩展名
            if url.lower().endswith('.docx'):
                file_ext = '.docx'
            elif url.lower().endswith('.pdf'):
                file_ext = '.pdf'
            
            # 构建文件名（使用案件名称）
            file_name = f"{safe_title}{file_ext}"
            file_path = os.path.join(download_path, file_name)
            
            # 检查文件是否已存在
            if os.path.exists(file_path):
                logger.info(f"附件已存在，跳过下载: {file_name}")
                return file_name
            
            # 如果文件已存在，添加数字后缀
            counter = 1
            while os.path.exists(file_path):
                name_parts = os.path.splitext(file_name)
                file_name = f"{name_parts[0]}_{counter}{name_parts[1]}"
                file_path = os.path.join(download_path, file_name)
                counter += 1

            # 使用aiohttp下载文件
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive'
                }, ssl=False) as response:
                    if response.status == 200:
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(await response.read())
                        logger.info(f"附件已保存为: {file_name}")  # 只记录文件名
                        return file_name  # 只返回文件名
                    else:
                        logger.error(f"下载附件失败，状态码: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"下载附件时发生错误: {str(e)}")
            return None

    async def save_to_db(self, case_data):
        """保存案件数据到数据库"""
        try:
            # 准备要保存的数据
            db_data = {
                'case_name': case_data.get('title', case_data.get('case_name')),
                'source_url': case_data.get('url', case_data.get('source_url')),
                'attachment_path': case_data.get('attachment_path'),
                'region': case_data.get('region', ''),
                'notice_start_date': case_data.get('notice_start_date'),
                'notice_end_date': case_data.get('notice_end_date')
            }

            # 检查案件是否已存在
            result = self.session.execute(
                text("SELECT id FROM cases WHERE case_name = :case_name"),
                {"case_name": db_data['case_name']}
            ).fetchone()
            
            if result:
                # 首先获取当前记录的值
                current_record = self.session.execute(
                    text("""
                        SELECT notice_start_date, notice_end_date, region 
                        FROM cases 
                        WHERE case_name = :case_name
                    """),
                    {"case_name": db_data['case_name']}
                ).fetchone()
                
                # 只在当前值为空且新值不为空时更新
                update_sql = """
                    UPDATE cases 
                    SET notice_start_date = CASE 
                            WHEN notice_start_date IS NULL AND :notice_start_date IS NOT NULL 
                            THEN :notice_start_date 
                            ELSE notice_start_date 
                        END,
                        notice_end_date = CASE 
                            WHEN notice_end_date IS NULL AND :notice_end_date IS NOT NULL 
                            THEN :notice_end_date 
                            ELSE notice_end_date 
                        END,
                        region = CASE 
                            WHEN (region IS NULL OR region = '') AND :region != '' 
                            THEN :region 
                            ELSE region 
                        END
                    WHERE case_name = :case_name
                """
                self.session.execute(text(update_sql), db_data)
                self.logger.info(f"更新已存在的案件记录: {db_data['case_name']}")
            else:
                # 创建新记录
                insert_sql = """
                    INSERT INTO cases (
                        case_name, source_url, attachment_path, region,
                        notice_start_date, notice_end_date, created_at
                    ) VALUES (
                        :case_name, :source_url, :attachment_path, :region,
                        :notice_start_date, :notice_end_date, CURRENT_TIMESTAMP
                    )
                """
                self.session.execute(text(insert_sql), db_data)
                self.logger.info(f"插入新案件记录: {db_data['case_name']}")
            
            self.session.commit()
            self.logger.info(f"成功保存案件到数据库: {db_data['case_name']}")
            return True
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"保存到数据库时发生错误: {str(e)}")
            self.logger.error(f"保存案件到数据库失败: {case_data.get('title', case_data.get('case_name'))}")
            return False

    def export_data(self):
        """导出数据到Excel"""
        try:
            # 直接使用已经存在的SQLAlchemy连接
            df = pd.read_sql_query('''
                SELECT 
                    case_name as '案件名称',
                    notice_start_date as '公示开始日期',
                    notice_end_date as '公示结束日期',
                    source_url as '来源网址',
                    attachment_path as '附件路径',
                    region as '地区',
                    created_at as '爬取时间'
                FROM cases
                ORDER BY created_at DESC
            ''', self.engine)
            
            # 使用固定的Excel文件路径
            excel_path = 'data/cases.xlsx'
            
            # 导出到Excel
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='案件列表')
                # 调整列宽
                worksheet = writer.sheets['案件列表']
                for idx, col in enumerate(df.columns):
                    max_length = max(df[col].astype(str).apply(len).max(), len(col)) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
            
            logger.info(f"数据已更新到Excel文件: {excel_path}")
            logger.info(f"共更新 {len(df)} 条记录")
            
            return {
                'excel_path': excel_path,
                'total_cases': len(df)
            }
            
        except Exception as e:
            logger.error(f"导出数据失败: {str(e)}")
            logger.exception(e)  # 添加详细的错误堆栈信息
            return None

    def run(self):
        """运行爬虫"""
        try:
            logger.info("开始爬取案件")
            asyncio.run(self.scrape_cases())
            
            # 导出数据
            export_result = self.export_data()
            if export_result:
                logger.info("爬虫运行完成，数据已导出")
                return export_result
            else:
                logger.error("爬虫运行完成，但数据导出失败")
                return None
            
        except Exception as e:
            logger.error(f"爬虫运行失败: {e}")
            return None

    def get_page_type(self, url):
        """根据URL确定页面类型"""
        domain_map = {
            'scjgj.beijing.gov.cn': 'beijing',
            'scjgj.sh.gov.cn': 'shanghai',
            'scjgj.cq.gov.cn': 'chongqing',
            'amr.gd.gov.cn': 'guangdong',
            'scjgj.shaanxi.gov.cn': 'shaanxi',
            'samr.gov.cn': 'samr'
        }
        
        for domain, page_type in domain_map.items():
            if domain in url:
                return page_type
        return None

    def get_region_name(self, page_type):
        """获取地区名称"""
        region_map = {
            'beijing': '北京',
            'shanghai': '上海',
            'chongqing': '重庆',
            'guangdong': '广东',
            'shaanxi': '陕西',
            'samr': '国家市场监督管理总局'
        }
        return region_map.get(page_type, '未知')

    async def process_case(self, case_data):
        """处理单个案件"""
        try:
            # 获取案件详情页面内容
            html_content = await self.fetch_page(case_data['url'])
            if not html_content:
                logger.error(f"获取案件详情失败: {case_data['url']}")
                return None
                
            # 解析详情页
            page_type = self.get_page_type(case_data['url'])
            if not page_type:
                logger.error(f"无法识别页面类型: {case_data['url']}")
                return None
                
            parser = create_parser(page_type, BeautifulSoup(html_content, 'html.parser'), case_data['url'])
            if not parser:
                logger.error(f"无法找到合适的解析器: {case_data['url']}")
                return None
                
            case_detail = parser.parse()
            if not case_detail:
                logger.error(f"解析案件详情失败: {case_data['url']}")
                return None
                
            # 检查数据库中是否已存在
            result = self.session.execute(
                text("SELECT id, attachment_path FROM cases WHERE case_name = :case_name"),
                {"case_name": case_detail.get('case_name')}
            ).fetchone()
            
            # 如果数据库中有记录，使用数据库中的附件路径
            if result:
                logger.info(f"案件已存在于数据库中: {case_detail.get('case_name')}")
                case_detail['attachment_path'] = result[1]
            else:
                # 如果数据库中不存在，下载附件
                attachment_path = None
                if 'attachment_url' in case_detail and case_detail['attachment_url']:
                    case_name = case_detail.get('case_name', '')
                    if not case_name:
                        logger.error("未找到案件名称")
                        return None
                        
                    attachment_path = await self.download_attachment(
                        case_detail['attachment_url'],
                        case_name
                    )
                    logger.info(f"使用案件名称 '{case_name}' 保存附件")
                    case_detail['attachment_path'] = attachment_path
            
            # 合并数据
            case_data.update(case_detail)
            case_data['region'] = self.get_region_name(page_type)
            
            # 保存到数据库
            save_result = await self.save_to_db(case_data)
            if not save_result:
                logger.error(f"保存案件到数据库失败: {case_data.get('case_name')}")
                return None
                
            logger.info(f"成功保存案件到数据库: {case_data.get('case_name')}")
            return case_data
            
        except Exception as e:
            logger.error(f"处理案件时发生错误: {str(e)}")
            return None

    def setup_database(self):
        """设置数据库连接"""
        self.engine = create_engine(f"sqlite:///data/cases.db")
        self.session = sessionmaker(bind=self.engine)()

    async def _turn_to_next_page(self):
        """辅助方法：翻到下一页
        
        Returns:
            bool: 翻页是否成功
        """
        try:
            # 获取当前页面的第一个标题作为参考
            first_element = await self.page.query_selector('.content-3-left-text a')
            if not first_element:
                self.logger.error("未找到参考标题元素")
                return False
            
            first_title = await first_element.inner_text()
            
            # 等待并点击下一页按钮
            next_button = await self.page.wait_for_selector('.layui-laypage-next', timeout=180000)
            if not next_button:
                self.logger.error("未找到下一页按钮")
                return False
            
            await next_button.click()
            await asyncio.sleep(3)  # 等待页面加载
            
            # 等待新内容加载
            await self.page.wait_for_selector('.content-3-left-text', timeout=180000)
            new_element = await self.page.query_selector('.content-3-left-text a')
            if not new_element:
                self.logger.error("翻页后未找到新的标题元素")
                return False
            
            new_title = await new_element.inner_text()
            
            # 验证页面是否已更新
            if new_title == first_title:
                self.logger.error("页面未更新，翻页失败")
                self.current_page = None  # 重置页码状态
                return False
            
            self.current_page += 1
            self.logger.info(f"成功翻到第 {self.current_page} 页")
            return True
            
        except Exception as e:
            self.logger.error(f"翻页过程出错: {str(e)}")
            self.current_page = None  # 重置页码状态
            return False
