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
from openpyxl.utils import get_column_letter

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BeijingScraper:
    def __init__(self):
        """初始化北京爬虫"""
        self.base_url = "https://scjgj.beijing.gov.cn/ztzl/jyzjzajgs/jyzjzjyajgs/"
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
            case_items = soup.select('div.public_list_team ul li')
            
            if case_items:
                logger.info(f"在列表页找到 {len(case_items)} 个案件")
                for item in case_items:
                    link = item.select_one('a')
                    date_span = item.select_one('span')
                    
                    if link and date_span:
                        title = link.get_text(strip=True)
                        href = link.get('href')
                        list_date = date_span.get_text(strip=True)
                        
                        if href and title:
                            # 处理相对路径
                            if href.startswith('./'):
                                href = href[2:]  # 移除开头的 ./
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
            last_part = current_url_parts[-1]

            # 处理第一页的情况（无论是目录形式还是index.html形式）
            if last_part == '' or last_part == 'index.html':
                current_page = 1
                # 构造下一页URL时，需要考虑当前URL的形式
                if last_part == '':
                    # 如果是目录形式（以/结尾），去掉最后的空字符串
                    base_path = '/'.join(current_url_parts[:-1])
                else:
                    # 如果是index.html形式，去掉index.html
                    base_path = '/'.join(current_url_parts[:-1])
                next_page = f"{base_path}/index_1.html"
                logger.info(f"第1页，下一页URL: {next_page}")
            else:
                # 处理后续页面（index_1.html, index_2.html 等）
                try:
                    # 从URL中提取当前页码数字
                    page_number = int(last_part.split('_')[1].split('.')[0])
                    current_page = page_number + 2  # index_1.html 实际是第2页
                    base_path = '/'.join(current_url_parts[:-1])
                    next_page = f"{base_path}/index_{current_page-1}.html"
                    logger.info(f"第{current_page}页，下一页URL: {next_page}")
                except (IndexError, ValueError) as e:
                    logger.error(f"解析页码失败: {str(e)}")
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
            content_div = soup.select_one('div#div_zhengwen')
            if not content_div:
                logger.error("未找到内容容器")
                return None
            
            content = content_div.get_text(strip=True)
            
            # 提取日期
            start_date = None
            end_date = None
            date_pattern = r'公示期：(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日)'
            date_match = re.search(date_pattern, content)
            
            if date_match:
                try:
                    start_date = datetime.strptime(date_match.group(1), '%Y年%m月%d日')
                    end_date = datetime.strptime(date_match.group(2), '%Y年%m月%d日')
                    logger.info(f"成功提取日期范围: {start_date.date()} 至 {end_date.date()}")
                except ValueError as e:
                    logger.error(f"日期转换失败: {str(e)}")
            else:
                logger.warning("未找到日期信息")

            # 获取附件链接
            attachment_url = None
            attachment_name = None
            attachment_div = soup.find('div', style="padding:10px 20px; line-height: 30px;")
            if attachment_div:
                attachment_link = attachment_div.find('a')
                if attachment_link:
                    attachment_url = attachment_link.get('href')
                    attachment_name = attachment_link.get_text(strip=True)
                    if attachment_url:
                        if attachment_url.startswith('./'):
                            attachment_url = attachment_url[2:]
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
        """下载附件"""
        try:
            # 创建保存目录（如果不存在）
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # 下载文件
            logger.info(f"开始下载附件: {url}")
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # 获取文件大小
            total_size = int(response.headers.get('content-length', 0))
            
            # 写入文件
            with open(save_path, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            # 计算下载进度
                            progress = (downloaded / total_size) * 100
                            if total_size > 1024*1024:  # 仅对大于1MB的文件显示进度
                                logger.info(f"下载进度: {progress:.1f}%")
            
            logger.info(f"附件下载完成: {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"下载附件失败: {url}, 错误: {str(e)}")
            # 如果下载失败，删除可能部分下载的文件
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                    logger.info(f"已删除未完成的下载文件: {save_path}")
                except Exception as del_e:
                    logger.error(f"删除未完成的下载文件失败: {save_path}, 错误: {str(del_e)}")
            return False

    async def process_case(self, case):
        """处理单个案件，包含附件下载和数据库更新逻辑"""
        try:
            case_title = case['title']
            case_url = case['url']
            
            # 检查案件是否已存在
            existing_case = self.db_session.query(Case).filter_by(case_name=case_title).first()
            
            detail_data = await self.parse_detail_page(case_url)
            if not detail_data:
                logger.error(f"无法解析案件详情: {case_title}")
                return False

            # 处理附件下载
            attachment_path = None
            if detail_data['attachment_url']:
                # 构造附件文件名（使用案件名）
                file_ext = detail_data['attachment_url'].split('.')[-1].lower()
                file_name = f"{case_title}.{file_ext}"
                # 清理文件名中的非法字符
                file_name = re.sub(r'[\\/:*?"<>|]', '_', file_name)
                attachment_path = os.path.join(self.attachment_dir, file_name)
                
                # 检查附件是否已存在
                if os.path.exists(attachment_path):
                    logger.info(f"附件已存在，跳过下载: {file_name}")
                else:
                    # 下载新附件
                    logger.info(f"开始下载新附件: {file_name}")
                    await self.download_attachment(detail_data['attachment_url'], attachment_path)

            if existing_case:
                # 更新已有记录
                need_update = False
                
                # 检查并更新地区
                if existing_case.region != '北京':
                    logger.info(f"更新案件地区 [{case_title}]: {existing_case.region} -> 北京")
                    existing_case.region = '北京'
                    need_update = True
                
                # 检查并更新日期（仅当原记录为空且新数据有值时）
                if not existing_case.notice_start_date and detail_data['start_date']:
                    logger.info(f"更新案件开始日期 [{case_title}]: None -> {detail_data['start_date']}")
                    existing_case.notice_start_date = detail_data['start_date']
                    need_update = True
                
                if not existing_case.notice_end_date and detail_data['end_date']:
                    logger.info(f"更新案件结束日期 [{case_title}]: None -> {detail_data['end_date']}")
                    existing_case.notice_end_date = detail_data['end_date']
                    need_update = True
                
                if need_update:
                    try:
                        self.db_session.commit()
                        logger.info(f"成功更新案件信息: {case_title}")
                    except Exception as e:
                        self.db_session.rollback()
                        logger.error(f"更新案件信息失败: {case_title}, 错误: {str(e)}")
                
                return False  # 不是新案件
            else:
                # 创建新案件记录
                try:
                    new_case = Case(
                        case_name=case_title,
                        notice_start_date=detail_data['start_date'],
                        notice_end_date=detail_data['end_date'],
                        source_url=case_url,
                        attachment_path=attachment_path,
                        region='北京'
                    )
                    self.db_session.add(new_case)
                    self.db_session.commit()
                    logger.info(f"成功添加新案件: {case_title}")
                    return True  # 是新案件
                    
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
        """导出数据到Excel"""
        try:
            excel_path = 'data/cases.xlsx'
            logger.info(f"开始导出数据到: {excel_path}")

            # 从数据库读取所有记录
            db_cases = self.db_session.query(Case).all()
            db_records = []
            for case in db_cases:
                # 安全处理日期
                try:
                    start_date = case.notice_start_date.strftime('%Y-%m-%d') if case.notice_start_date else None
                    end_date = case.notice_end_date.strftime('%Y-%m-%d') if case.notice_end_date else None
                except Exception as e:
                    logger.warning(f"日期格式化失败 [{case.case_name}]: {str(e)}")
                    start_date = None
                    end_date = None

                db_records.append({
                    '案件名称': case.case_name,
                    '公示开始日期': start_date,
                    '公示结束日期': end_date,
                    '来源链接': case.source_url,
                    '附件路径': case.attachment_path,
                    '地区': case.region
                })
            
            current_df = pd.DataFrame(db_records)
            
            # 如果Excel文件已存在，读取并合并
            if os.path.exists(excel_path):
                try:
                    existing_df = pd.read_excel(excel_path)
                    existing_df.columns = ['案件名称', '公示开始日期', '公示结束日期', 
                                         '来源链接', '附件路径', '地区']
                    
                    # 转换日期列
                    for date_col in ['公示开始日期', '公示结束日期']:
                        existing_df[date_col] = pd.to_datetime(existing_df[date_col], errors='coerce')
                        current_df[date_col] = pd.to_datetime(current_df[date_col], errors='coerce')

                    # 使用案件名称作为索引
                    existing_df.set_index('案件名称', inplace=True)
                    current_df.set_index('案件名称', inplace=True)

                    # 更新现有记录
                    for case_name in current_df.index:
                        if case_name in existing_df.index:
                            existing_case = existing_df.loc[case_name]
                            current_case = current_df.loc[case_name]
                            
                            if existing_case['地区'] != '北京':
                                existing_df.at[case_name, '地区'] = '北京'
                            
                            if pd.isna(existing_case['公示开始日期']) and not pd.isna(current_case['公示开始日期']):
                                existing_df.at[case_name, '公示开始日期'] = current_case['公示开始日期']
                            
                            if pd.isna(existing_case['公示结束日期']) and not pd.isna(current_case['公示结束日期']):
                                existing_df.at[case_name, '公示结束日期'] = current_case['公示结束日期']
                        else:
                            existing_df.loc[case_name] = current_df.loc[case_name]

                    existing_df.reset_index(inplace=True)
                    final_df = existing_df
                except Exception as e:
                    logger.error(f"处理现有Excel文件时出错: {str(e)}")
                    final_df = current_df.reset_index()
            else:
                final_df = current_df.reset_index()

            # 保存到Excel
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name='案件列表')
                
                # 调整列宽
                worksheet = writer.sheets['案件列表']
                column_widths = {
                    '案件名称': 60,
                    '公示开始日期': 15,
                    '公示结束日期': 15,
                    '来源链接': 50,
                    '附件路径': 50,
                    '地区': 10
                }
                
                for col_name, width in column_widths.items():
                    col_letter = get_column_letter(final_df.columns.get_loc(col_name) + 1)
                    worksheet.column_dimensions[col_letter].width = width

            logger.info(f"数据导出完成，共 {len(final_df)} 条记录")
            return {'total_cases': len(final_df)}

        except Exception as e:
            logger.error(f"导出数据失败: {str(e)}")
            return None

if __name__ == "__main__":
    import warnings
    import urllib3
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    scraper = BeijingScraper()
    # 运行爬虫，设置最高页码为 15
    logger.info("开始运行北京爬虫，限制页码为 15 页...")
    asyncio.run(scraper.run(max_page=15))
    logger.info("爬虫运行结束（限制 15 页）。")
