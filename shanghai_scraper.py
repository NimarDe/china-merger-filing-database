import asyncio
import logging
import os
import requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from models import Case, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import traceback
import re

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShanghaiScraper:
    def __init__(self):
        """初始化上海爬虫"""
        self.base_url = "https://scjgj.sh.gov.cn/1571/"
        self.session = requests.Session()
        self.session.verify = False
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session.headers.update(headers)
        
        # 初始化数据库会话
        engine = create_engine('sqlite:///data/cases.db')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.db_session = Session()
        
        # 创建附件目录
        self.attachment_dir = 'data/attachments'
        os.makedirs(self.attachment_dir, exist_ok=True)

    async def parse_list_page(self, url):
        """解析列表页"""
        try:
            logger.info(f"访问列表页: {url}")
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取案件链接
            cases = []
            case_rows = soup.select('tr.table_list_tr1, tr.table_list_tr2')
            
            if case_rows:
                logger.info(f"在列表页找到 {len(case_rows)} 个案件")
                for row in case_rows:
                    link = row.select_one('td.overflow a')
                    date_td = row.select('td')[3]  # 第4个td是发布日期
                    
                    if link and date_td:
                        title = link.get_text(strip=True)
                        href = link.get('href')
                        list_date = date_td.get_text(strip=True)
                        
                        if href and title:
                            href = urljoin(url, href)
                            cases.append({
                                'title': title,
                                'url': href,
                                'list_date': list_date
                            })
                            logger.info(f"找到案件: {title}")
            
            if not cases:
                logger.error(f"未找到案件列表，页面内容长度: {len(response.text)}")
                return None
            
            # 获取当前页码和构造下一页链接
            current_url_parts = url.split('/')
            if url.endswith('/1571/') or url.endswith('/1571'):
                current_page = 1
                next_page = urljoin(url, 'index_2.html')
            else:
                # 从URL中提取当前页码
                current_page_str = current_url_parts[-1].split('_')[1].split('.')[0]
                try:
                    current_page = int(current_page_str)
                    next_page = f"{self.base_url}index_{current_page + 1}.html"
                except ValueError:
                    current_page = 1
                    next_page = None
            
            return {
                'cases': cases,
                'next_page': next_page,
                'current_page': current_page
            }
                
        except Exception as e:
            logger.error(f"解析列表页失败: {str(e)}")
            logger.exception("详细错误信息:")
            return None

    async def parse_detail_page(self, url):
        """解析详情页"""
        try:
            logger.info(f"访问详情页: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取内容容器
            content_div = soup.select_one('div#ivs_content')
            if not content_div:
                logger.error("未找到内容容器")
                return None
            
            content = content_div.get_text(strip=True)
            
            # 提取日期 - 修改这部分以处理不同格式
            start_date = None
            end_date = None
            
            # 尝试多个日期格式模式
            date_patterns = [
                # 标准格式：年月日至年月日
                r'公示期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)',
                # 带空格的格式：公 示 期：年月日至年月日
                r'公\s*示\s*期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)',
                # 带缩进的格式：[空格]公示期：年月日至年月日
                r'\s*公示期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)',
                # HTML缩进格式：&emsp;公示期：年月日至年月日
                r'&emsp;公示期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)',
                # 带HTML空格的格式：公&nbsp;示&nbsp;期：年月日至年月日
                r'公&nbsp;示&nbsp;期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)'
            ]
            
            # 获取原始HTML内容
            content_html = str(content_div)
            
            # 依次尝试各种模式
            date_match = None
            for pattern in date_patterns:
                date_match = re.search(pattern, content_html)
                if date_match:
                    try:
                        start_date = datetime.strptime(date_match.group(1), '%Y年%m月%d日')
                        end_date = datetime.strptime(date_match.group(2), '%Y年%m月%d日')
                        logger.info(f"成功提取日期范围: {start_date.date()} 至 {end_date.date()}")
                        break
                    except ValueError as e:
                        logger.error(f"日期转换失败: {str(e)}")
                        continue
            
            if not date_match:
                # 如果所有模式都失败，尝试直接从文本内容中提取
                content_text = content_div.get_text()
                for pattern in date_patterns:
                    date_match = re.search(pattern, content_text)
                    if date_match:
                        try:
                            start_date = datetime.strptime(date_match.group(1), '%Y年%m月%d日')
                            end_date = datetime.strptime(date_match.group(2), '%Y年%m月%d日')
                            logger.info(f"从文本内容成功提取日期范围: {start_date.date()} 至 {end_date.date()}")
                            break
                        except ValueError as e:
                            logger.error(f"日期转换失败: {str(e)}")
                            continue

            if not start_date or not end_date:
                logger.warning(f"未找到日期信息或日期格式不正确: {url}")

            # 获取附件链接
            attachment_url = None
            attachment_name = None
            attachment_row = soup.find('tr', string=lambda text: text and '附件：' in text if text else False)
            if attachment_row:
                attachment_link = attachment_row.find('a')
                if attachment_link:
                    attachment_url = attachment_link.get('href')
                    attachment_name = attachment_link.get_text(strip=True)
                    if attachment_url:
                        attachment_url = urljoin(url, attachment_url)
                        logger.info(f"找到附件: {attachment_name} ({attachment_url})")

            return {
                'content': content,
                'start_date': start_date,
                'end_date': end_date,
                'attachment_url': attachment_url,
                'attachment_name': attachment_name,
                'source_url': url,
            }
            
        except Exception as e:
            logger.error(f"解析详情页失败: {url}, 错误: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def download_attachment(self, url, save_path):
        """下载附件，按文件名查重"""
        try:
            # 检查文件是否已存在
            if os.path.exists(save_path):
                logger.info(f"附件已存在，跳过下载: {save_path}")
                return True
            
            # 创建保存目录（如果不存在）
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # 下载文件
            logger.info(f"开始下载附件: {url}")
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # 写入文件
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"附件下载完成: {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"下载附件失败: {url}, 错误: {str(e)}")
            if os.path.exists(save_path):
                os.remove(save_path)
            return False

    async def process_case(self, case):
        """处理单个案件，包含查重和更新逻辑"""
        try:
            case_title = case['title']
            case_url = case['url']
            
            # 检查案件是否已存在
            existing_case = self.db_session.query(Case).filter_by(case_name=case_title).first()
            
            # 解析详情页
            detail_data = await self.parse_detail_page(case_url)
            if not detail_data:
                logger.error(f"解析详情页失败: {case_url}")
                return False
            
            # 下载附件
            attachment_path = None
            if detail_data['attachment_url'] and detail_data['attachment_name']:
                file_name = detail_data['attachment_name']
                save_path = os.path.join(self.attachment_dir, file_name)
                
                # 下载附件（如果是新案件或者现有案件没有附件）
                if not existing_case or not existing_case.attachment_path:
                    if await self.download_attachment(detail_data['attachment_url'], save_path):
                        attachment_path = save_path
            
            if existing_case:
                # 检查是否需要更新
                need_update = False
                
                # 检查日期是否为空
                if not existing_case.notice_start_date and detail_data['start_date']:
                    existing_case.notice_start_date = detail_data['start_date']
                    need_update = True
                if not existing_case.notice_end_date and detail_data['end_date']:
                    existing_case.notice_end_date = detail_data['end_date']
                    need_update = True
                
                # 检查地区是否为上海
                if existing_case.region != '上海':
                    existing_case.region = '上海'
                    need_update = True
                
                # 如果有新的附件路径，更新附件路径
                if attachment_path and not existing_case.attachment_path:
                    existing_case.attachment_path = attachment_path
                    need_update = True
                
                if need_update:
                    try:
                        self.db_session.commit()
                        logger.info(f"更新案件信息: {case_title}")
                    except Exception as e:
                        self.db_session.rollback()
                        logger.error(f"更新案件失败: {case_title}, 错误: {str(e)}")
                else:
                    logger.info(f"案件已存在且无需更新: {case_title}")
                return False
            else:
                # 创建新案件记录
                try:
                    new_case = Case(
                        case_name=case_title,
                        notice_start_date=detail_data['start_date'],
                        notice_end_date=detail_data['end_date'],
                        source_url=case_url,
                        attachment_path=attachment_path,
                        region='上海'
                    )
                    self.db_session.add(new_case)
                    self.db_session.commit()
                    logger.info(f"成功添加新案件: {case_title}")
                    return True
                    
                except Exception as e:
                    self.db_session.rollback()
                    logger.error(f"添加新案件失败: {case_title}, 错误: {str(e)}")
                    return False

        except Exception as e:
            logger.error(f"处理案件失败: {case.get('title', 'N/A')}, 错误: {str(e)}")
            return False

    async def run(self, max_page=None):
        """运行爬虫，增加最高页码限制"""
        try:
            current_url = self.base_url
            total_new_cases = 0
            processed_pages = 0
            
            while current_url:
                # 解析列表页
                page_data = await self.parse_list_page(current_url)
                if not page_data:
                    logger.error(f"无法解析列表页或页面为空: {current_url}")
                    break

                cases = page_data['cases']
                current_page = page_data['current_page']
                processed_pages += 1
                logger.info(f"--- 开始处理第 {current_page} 页 ({current_url}) ---")

                # 检查是否超过最高页码限制
                if max_page and current_page > max_page:
                    logger.info(f"已达到最高页码限制: {max_page}，停止爬取。")
                    break

                if cases:
                    logger.info(f"第 {current_page} 页找到 {len(cases)} 个案件")
                    for case in cases:
                        try:
                            is_new = await self.process_case(case)
                            if is_new:
                                total_new_cases += 1
                            await asyncio.sleep(0.5)  # 每个案件处理后短暂延迟
                        except Exception as e:
                            logger.error(f"处理案件失败: {case.get('title', 'N/A')}, 错误: {str(e)}")
                else:
                    logger.warning(f"第 {current_page} 页未找到案件")
                
                # 获取下一页URL
                current_url = page_data['next_page']
                if current_url:
                    logger.info(f"准备处理下一页: {current_url}")
                    await asyncio.sleep(1)  # 页面间延迟
                else:
                    logger.info("已到达最后一页或无法找到下一页链接。")
                    break
            
            logger.info(f"爬虫运行完成，共处理 {processed_pages} 个页面，新增 {total_new_cases} 个案件到数据库。")
            
            # 导出数据到Excel
            export_result = self.export_data()
            if export_result:
                logger.info(f"数据已导出到Excel，总记录数：{export_result['total_cases']}")
            
        except Exception as e:
            logger.error(f"爬虫运行出错: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            logger.info("关闭数据库会话。")
            self.db_session.close()

    def export_data(self):
        """导出数据到Excel，包含查重和更新逻辑"""
        try:
            excel_path = 'data/cases.xlsx'
            logger.info(f"开始导出数据到: {excel_path}")

            # 从数据库读取所有记录
            query = self.db_session.query(Case)
            db_cases = query.all()
            
            # 转换为DataFrame格式
            db_records = []
            for case in db_cases:
                db_records.append({
                    '案件名称': case.case_name,
                    '公示开始日期': case.notice_start_date,
                    '公示结束日期': case.notice_end_date,
                    '来源链接': case.source_url,
                    '附件路径': case.attachment_path,
                    '地区': case.region
                })
            
            current_df = pd.DataFrame(db_records)
            
            # 如果Excel文件已存在，读取并合并数据
            if os.path.exists(excel_path):
                existing_df = pd.read_excel(excel_path)
                
                # 合并数据，保留最新的记录
                combined_df = pd.concat([existing_df, current_df])
                combined_df = combined_df.drop_duplicates(subset=['案件名称'], keep='last')
                
                # 按日期排序
                combined_df.sort_values(by=['公示开始日期'], ascending=False, inplace=True)
                
                # 保存到Excel
                combined_df.to_excel(excel_path, index=False)
                logger.info(f"更新Excel文件完成，总记录数: {len(combined_df)}")
                return {'total_cases': len(combined_df)}
            else:
                # 创建新的Excel文件
                current_df.to_excel(excel_path, index=False)
                logger.info(f"创建新Excel文件完成，总记录数: {len(current_df)}")
                return {'total_cases': len(current_df)}
            
        except Exception as e:
            logger.error(f"导出数据失败: {str(e)}")
            return None

if __name__ == "__main__":
    import warnings
    import urllib3
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    scraper = ShanghaiScraper()
    # 运行爬虫，设置最高页码为 6
    logger.info("开始运行上海爬虫，限制页码为 6 页...")
    asyncio.run(scraper.run(max_page=6))
    logger.info("爬虫运行结束（限制 6 页）。")
