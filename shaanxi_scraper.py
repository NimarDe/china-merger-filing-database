import os
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Case, Base
from urllib.parse import urljoin, urlparse, unquote
import pandas as pd
import asyncio
import traceback
import sqlite3

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShaanxiScraper:
    def __init__(self):
        """初始化爬虫"""
        self.base_url = "https://snamr.shaanxi.gov.cn/sy/ztzl/cjscgpjz/jyzjz/ajgs/jyzjzjyajgs/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 创建数据库连接
        self.engine = create_engine(f"sqlite:///data/cases.db")
        Base.metadata.create_all(self.engine)
        self.db_session = sessionmaker(bind=self.engine)()
        
        # 创建必要的目录
        self.attachment_dir = 'data/attachments'
        os.makedirs(self.attachment_dir, exist_ok=True)

    def parse_list_page(self, page_no):
        """解析列表页 - 改进标题和日期的分离"""
        try:
            # 构建URL
            if page_no == 1:
                url = f"{self.base_url}index.html"
            else:
                url = f"{self.base_url}index_{page_no-1}.html"
            
            logger.info(f"正在访问列表页: {url}")
            response = self.session.get(url)
            response.raise_for_status() # 检查请求是否成功
            # 尝试自动检测编码，如果失败则使用utf-8
            response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            cases = []
            
            # 查找案件列表 - 选择器可能需要根据实际情况微调
            list_items = soup.select('.news-list li') or soup.select('ul.list li')

            for item in list_items:
                link_tag = item.find('a')
                if not link_tag:
                    continue
                    
                href = link_tag.get('href')
                if not href:
                    continue
                
                # 处理相对路径和绝对路径
                case_url = urljoin(url, href) # 使用当前列表页URL作为基准

                # --- 改进标题和日期提取 ---
                date_span = item.find('span', class_='time')
                date_str = date_span.get_text(strip=True) if date_span else None

                # Get the full link text
                full_link_text = link_tag.get_text(strip=True)

                # Assume the title is the part before the date
                title = full_link_text
                if date_str and full_link_text.endswith(date_str):
                    # Remove the date string from the end if found
                    title = full_link_text[:-len(date_str)].strip()
                    # Further clean potential separators like '.' if needed
                    if title.endswith('.'): title = title[:-1].strip()

                # Basic check if title seems valid
                if not title:
                    logger.warning(f"无法从链接文本 '{full_link_text}' 中提取有效标题。跳过。")
                    continue
                # --- 结束改进 ---

                cases.append({
                    'title': title, # Now should be cleaner
                    'url': case_url,
                    'list_date': date_str # Keep original date string if needed elsewhere
                })
            
            logger.info(f"第 {page_no} 页找到 {len(cases)} 个案件")

            # 确定是否有下一页 (陕西的翻页逻辑比较简单，通常是递增index_n.html)
            # 简单的假设：只要当前页能成功访问且找到内容，就认为可能有下一页
            # 实际应用中可能需要更精确的下一页链接查找逻辑
            next_page_no = page_no + 1
            # 这里可以添加一个请求尝试来验证下一页是否存在，如果404则认为没有下一页
            # next_page_url = f"{self.base_url}index_{next_page_no-1}.html"
            # try:
            #     next_page_response = self.session.head(next_page_url, timeout=5)
            #     if next_page_response.status_code == 404:
            #         next_page_no = None # 没有下一页了
            # except requests.exceptions.RequestException:
            #     next_page_no = None # 请求失败也认为没有下一页

            return {
                'cases': cases,
                'current_page': page_no,
                'next_page_no': next_page_no # 返回下一页的页码
            }
            
        except requests.exceptions.HTTPError as e:
             if e.response.status_code == 404:
                 logger.warning(f"页面未找到 (404): {url}")
                 return {'cases': [], 'current_page': page_no, 'next_page_no': None} # 404表示没有这一页了
             else:
                 logger.error(f"访问列表页时发生HTTP错误: {url}, {str(e)}")
                 return None # 其他HTTP错误，返回None表示失败
        except Exception as e:
            logger.error(f"解析列表页 {page_no} 失败: {url}, 错误: {str(e)}")
            logger.error(traceback.format_exc())
            return None # 解析失败返回None

    def parse_detail_page(self, url):
        """解析详情页，优先从 meta 标签提取日期和附件信息"""
        try:
            logger.info(f"访问详情页: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'

            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取标题
            title_tag = soup.select_one('.public-title-nav .title') or soup.select_one('h1') or soup.select_one('.article-title')
            title = title_tag.get_text(strip=True) if title_tag else None
            # Fallback title from <title> tag if primary not found
            if not title:
                 title_tag_html = soup.find('title')
                 if title_tag_html:
                     title = title_tag_html.get_text(strip=True).split('-')[0].strip()

            start_date = None
            end_date = None
            attachment_url = None
            attachment_name = None
            dates_found_in_meta = False

            # --- 优先尝试从 Meta Description 获取日期 ---
            meta_desc_tag = soup.find('meta', attrs={'name': 'Description'})
            if meta_desc_tag and meta_desc_tag.get('content'):
                meta_content = meta_desc_tag['content']
                logger.debug(f"找到 Meta Description: {meta_content}")
                # 使用与之前类似的正则，但应用于 meta_content
                # Example: "公 示 期：2025年4月8日至2025年4月17日联系邮箱：jyzjz@samr.gov.cn"
                date_pattern_meta = r'公\s*示\s*期\s*[:：]?\s*(\d{4})\s*[年-](\d{1,2})\s*[月-](\d{1,2})\s*日?\s*[至到-]\s*(\d{4})\s*[年-](\d{1,2})\s*[月-](\d{1,2})日?'
                match_meta = re.search(date_pattern_meta, meta_content)
                if match_meta:
                    try:
                        sy, sm, sd, ey, em, ed = map(int, match_meta.groups())
                        start_date = datetime(sy, sm, sd)
                        end_date = datetime(ey, em, ed)
                        logger.info(f"从 Meta Description 提取到公示期: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
                        dates_found_in_meta = True
                    except ValueError:
                         logger.warning(f"Meta Description 日期格式无效: {match_meta.group(0)} in {url}")
                else:
                    logger.debug(f"未在 Meta Description 中匹配到日期模式。")
            else:
                logger.debug(f"未找到 Meta Description 标签。")
            # --- 结束 Meta Description 日期提取 ---

            # 获取内容区域用于提取附件和日期回退
            content_area = soup.select_one('.news-content') or soup.select_one('.article-content') or soup.select_one('#content') or soup.body

            if content_area:
                # --- 如果未在 Meta 中找到日期，则回退到搜索内容区域 ---
                if not dates_found_in_meta:
                    logger.debug("未在 Meta 中找到日期，回退到搜索正文内容。")
                    text_content = content_area.get_text(" ", strip=True)
                    date_pattern_content = r'公\s*示\s*期\s*[:：]?\s*(\d{4})\s*[年-](\d{1,2})\s*[月-](\d{1,2})\s*日?\s*[至到-]\s*(\d{4})\s*[年-](\d{1,2})\s*[月-](\d{1,2})日?'
                    match_content = re.search(date_pattern_content, text_content)
                    if match_content:
                        try:
                            sy, sm, sd, ey, em, ed = map(int, match_content.groups())
                            start_date = datetime(sy, sm, sd)
                            end_date = datetime(ey, em, ed)
                            logger.info(f"从正文内容提取到公示期: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
                        except ValueError:
                             logger.warning(f"正文内容日期格式无效: {match_content.group(0)} in {url}")
                    else:
                        logger.warning(f"未在正文内容中找到公示期信息: {url}")
                # --- 结束日期回退逻辑 ---

                # --- 提取附件链接 (逻辑不变) ---
                attachment_heading = content_area.find(lambda tag: tag.name in ['h2', 'h3', 'strong', 'p'] and '附件下载' in tag.get_text())
                search_area = attachment_heading.find_next_sibling('ul') or attachment_heading.find_next('ul') if attachment_heading else content_area
                if search_area:
                     for link in search_area.find_all('a', href=True):
                         href = link.get('href')
                         link_text = link.get_text(strip=True)
                         if any(ext in href.lower() for ext in ['.doc', '.docx', '.pdf', '.xls', '.xlsx', '.zip', '.rar', '.txt']):
                              if any(keyword in link_text for keyword in ['公示表', '附件', '下载']):
                                   attachment_url = urljoin(url, href)
                                   attachment_name = link_text
                                   logger.info(f"找到附件链接: {attachment_url} (名称: {attachment_name})")
                                   break
                     if not attachment_url:
                           for link in search_area.find_all('a', href=True):
                                href = link.get('href')
                                if any(ext in href.lower() for ext in ['.doc', '.docx', '.pdf', '.xls', '.xlsx', '.zip', '.rar', '.txt']):
                                     attachment_url = urljoin(url, href)
                                     attachment_name = link.get_text(strip=True)
                                     logger.warning(f"找到附件链接（无关键词）: {attachment_url} (名称: {attachment_name})")
                                     break
                # --- 结束附件提取 ---
            else:
                 logger.warning(f"未能找到详情页内容区域: {url}")


            return {
                'title': title,
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

    def download_attachment(self, case_name, attachment_url):
        """下载附件 (复用重庆版本，更健壮)"""
        if not attachment_url:
            return None
        
        try:
            # 从URL获取文件名，并进行解码
            parsed_url = urlparse(attachment_url)
            # 优先使用路径最后一部分解码后的结果
            file_name = os.path.basename(unquote(parsed_url.path)) if parsed_url.path else None

            # 如果无法从URL有效获取文件名，或文件名看起来不正常（如太短、无后缀）
            # 则尝试基于 case_name 和 URL 后缀构建
            if not file_name or '.' not in file_name or len(file_name) < 5:
                 ext_match = re.search(r'\.([a-zA-Z0-9]+)$', attachment_url.lower())
                 ext = ext_match.group(1) if ext_match else 'bin' # 默认后缀
                 # 清理case_name作为文件名
                 safe_case_name = re.sub(r'[\\/*?:"<>|]', "_", case_name) # 移除或替换非法字符
                 safe_case_name = safe_case_name[:100] # 限制长度
                 file_name = f"{safe_case_name}.{ext}"
                 logger.warning(f"无法从URL解析有效文件名，使用生成的文件名: {file_name}")

            # 完整的本地保存路径
            file_path = os.path.join(self.attachment_dir, file_name)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(file_path):
                 logger.info(f"附件已存在，跳过下载: {file_name} ({file_path})")
                 return file_path # 返回已存在的文件路径

            logger.info(f"开始下载附件: {attachment_url} 到 {file_path}")
            response = self.session.get(attachment_url, stream=True, timeout=120) # 增加超时时间
            response.raise_for_status()
            
            # 使用 stream=True 方式写入文件
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    # filter out keep-alive new chunks
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"成功下载附件: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"下载附件失败: {attachment_url}, 错误: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def process_case(self, case):
        """处理单个案件，修正缩进错误"""
        try:
            list_page_cleaned_title = case.get('title')
            case_url = case.get('url')
            if not list_page_cleaned_title or not case_url:
                 logger.warning(f"列表页案件信息不完整，跳过: {case}")
                 return False
            query_title = list_page_cleaned_title

            # --- Debugging Lookup (代码不变) ---
            logger.info(f"Attempting DB lookup for title: '{query_title}' (Type: {type(query_title)})")
            raw_count = -1
            try:
                 conn = self.db_session.connection()
                 sql = text("SELECT COUNT(*) FROM cases WHERE case_name = :title")
                 result = conn.execute(sql, {"title": query_title})
                 raw_count = result.scalar()
                 logger.info(f"Raw SQL check for title '{query_title}': Found {raw_count} matching records.")
            except Exception as raw_e: logger.error(f"Raw SQL check failed for title '{query_title}': {raw_e}")
            # --- End Debugging Lookup ---

            existing_case = self.db_session.query(Case).filter(Case.case_name == query_title).first()
            # --- (日志记录 ORM 结果 - 代码不变) ---
            if existing_case: logger.info(f"ORM lookup SUCCESS for title '{query_title}'. Found existing case.")
            else:
                 if raw_count > 0: logger.error(f"ORM lookup FAILED for title '{query_title}' BUT Raw SQL found {raw_count} matches. Potential ORM/Session issue?")
                 elif raw_count == 0: logger.info(f"ORM lookup FAILED for title '{query_title}'. Raw SQL also found 0 matches. Treating as new case.")
                 else: logger.warning(f"ORM lookup FAILED for title '{query_title}'. Raw SQL check also failed.")

            detail_data = self.parse_detail_page(case_url);
            if not detail_data: return False
            detail_page_cleaned_title = detail_data.get('title')

            if not existing_case and detail_page_cleaned_title and detail_page_cleaned_title != query_title:
                logger.debug(f"Initial lookup failed for '{query_title}', retrying with detail title '{detail_page_cleaned_title}'...")
                existing_case = self.db_session.query(Case).filter(Case.case_name == detail_page_cleaned_title).first()
                if existing_case: logger.info(f"Found existing case using detail page title: '{detail_page_cleaned_title}'")

            final_title = detail_page_cleaned_title or query_title
            if not final_title: return False

            if existing_case:
                # --- Update existing case ---
                logger.info(f"案件已存在: '{existing_case.case_name}'. Updating with latest data...")
                updated_fields = []
                if existing_case.case_name != final_title: existing_case.case_name = final_title; updated_fields.append("名称")
                if existing_case.region != '陕西': existing_case.region = '陕西'; updated_fields.append("地区")
                if existing_case.notice_start_date != detail_data['start_date']: existing_case.notice_start_date = detail_data['start_date']; updated_fields.append("开始日期")
                if existing_case.notice_end_date != detail_data['end_date']: existing_case.notice_end_date = detail_data['end_date']; updated_fields.append("结束日期")

                # Update attachment (only download if missing)
                if not existing_case.attachment_path and detail_data['attachment_url']: # Level 1 indent
                    dl_path = self.download_attachment(final_title, detail_data['attachment_url']) # Level 2 indent
                    if dl_path: # Level 2 indent (Corrected)
                        existing_case.attachment_path = dl_path
                        updated_fields.append("附件路径")
                elif existing_case.attachment_path: # Level 1 indent
                     logger.debug(f"案件已有附件记录: {existing_case.attachment_path}") # Level 2 indent

                if existing_case.source_url != case_url: existing_case.source_url = case_url; updated_fields.append("来源网址")
                existing_case.created_at = datetime.now(); updated_fields.append("爬取时间")

                # Commit changes
                db_update_successful = False
                try:
                    self.db_session.flush()
                    self.db_session.commit()
                    db_update_successful = True # Mark as successful if commit doesn't raise error
                    if updated_fields:
                        logger.info(f"成功更新数据库 '{final_title}': {'; '.join(updated_fields)}")
                    else:
                        logger.info(f"数据库案件无需更新字段 (仅更新时间戳): {final_title}")

                    # --- Post-Commit Verification Read ---
                    if db_update_successful and "开始日期" in updated_fields: # Check only if we tried to update the date
                        logger.info(f"Verifying date persistence for '{final_title}'...")
                        verify_conn = None
                        try:
                            # Create a completely new connection and cursor
                            verify_conn = sqlite3.connect('data/cases.db') # Use the known path directly
                            verify_cursor = verify_conn.cursor()
                            # Query using the exact name we just committed
                            verify_cursor.execute("SELECT notice_start_date, notice_end_date FROM cases WHERE case_name = ?", (final_title,))
                            verify_result = verify_cursor.fetchone()
                            if verify_result:
                                 persisted_start, persisted_end = verify_result
                                 logger.info(f"  Post-commit read: notice_start_date='{persisted_start}' (Type: {type(persisted_start)}), notice_end_date='{persisted_end}' (Type: {type(persisted_end)})")
                                 # Compare with what we tried to save
                                 expected_start_str = detail_data['start_date'].strftime('%Y-%m-%d %H:%M:%S') if detail_data['start_date'] else None
                                 if str(persisted_start) != str(expected_start_str): # Compare string representation for simplicity
                                      logger.error(f"  *** Verification FAILED! Expected start '{expected_start_str}', but DB read '{persisted_start}'.")
                            else:
                                logger.error(f"  *** Verification FAILED! Could not re-read case '{final_title}' immediately after commit.")
                        except Exception as verify_e:
                            logger.error(f"  *** Verification Read Error: {verify_e}")
                        finally:
                            if verify_conn: verify_conn.close()
                    # --- End Post-Commit Verification Read ---

                except Exception as e:
                    logger.error(f"DB更新失败 '{final_title}': {e}"); self.db_session.rollback()

                return False # Not a new case
            else:
                # --- Add new case ---
                # ...(检查 final_title 是否存在 - 代码不变)...
                if final_title != query_title:
                     existing_case_final = self.db_session.query(Case).filter(Case.case_name == final_title).first()
                     if existing_case_final: logger.warning(f"Attempted to add '{final_title}' as new, but it already exists. Skipping add."); return False

                # ...(添加新记录逻辑 - 代码不变)...
                logger.info(f"添加新案件: '{final_title}'")
                attachment_path = None
                if detail_data['attachment_url']: attachment_path = self.download_attachment(final_title, detail_data['attachment_url'])
                new_case = Case(
                    case_name=final_title,
                    source_url=case_url,
                    region='陕西',
                    notice_start_date=detail_data['start_date'],
                    notice_end_date=detail_data['end_date'],
                    attachment_path=attachment_path,
                    created_at=datetime.now()
                )
                try:
                    self.db_session.add(new_case)
                    self.db_session.commit()
                    logger.info(f"成功添加新案件到数据库: {final_title}")
                    return True
                except Exception as e:
                    logger.error(f"DB添加失败 '{final_title}': {e}")
                    self.db_session.rollback()
                    return False
        except Exception as e: logger.error(f"处理案件 '{case.get('title', 'N/A')}' 失败: {e}"); logger.error(traceback.format_exc()); return False

    def run(self, max_page=None): # 添加 max_page 参数
        """运行爬虫，增加最高页码限制"""
        try:
            current_page_no = 1
            total_new_cases = 0
            processed_pages = 0
            
            while True: # 使用 break 来退出循环
                # 检查是否超过最高页码限制 (在请求页面之前检查)
                if max_page and current_page_no > max_page:
                    logger.info(f"已达到最高页码限制: {max_page}，停止爬取。")
                    break

                logger.info(f"--- 开始处理第 {current_page_no} 页 ---")
                # 解析列表页
                page_data = self.parse_list_page(current_page_no)
                
                # 处理解析失败或404的情况
                if page_data is None:
                     logger.error(f"无法解析第 {current_page_no} 页，停止爬取。")
                     break
                if page_data['next_page_no'] is None and not page_data['cases']:
                     logger.info(f"第 {current_page_no} 页未找到且无下一页，可能已到达末尾，停止爬取。")
                     break # 如果当前页是404且无案件，则停止

                processed_pages += 1
                cases = page_data['cases']
                
                if cases:
                    logger.info(f"第 {current_page_no} 页找到 {len(cases)} 个案件")
                    for case in cases:
                        try:
                            # 使用 await 如果 process_case 是异步的，但这里不是
                            is_new = self.process_case(case)
                            if is_new:
                                total_new_cases += 1
                            # 可以加短暂延迟避免请求过快
                            # time.sleep(0.5)
                        except Exception as e:
                            logger.error(f"处理案件失败: {case.get('title', 'N/A')}, 错误: {str(e)}")
                else:
                    logger.warning(f"第 {current_page_no} 页未找到案件")
                    # 即使当前页没案件，也尝试下一页，除非 next_page_no 是 None

                # 获取下一页页码
                next_page_no = page_data.get('next_page_no')
                if next_page_no:
                    current_page_no = next_page_no
                    logger.info(f"准备处理第 {current_page_no} 页")
                    # time.sleep(1) # 页面间延迟
                else:
                    logger.info("已到达最后一页或无法确定下一页。")
                    break # 没有下一页了，退出循环
            
            logger.info(f"爬虫运行完成，共处理 {processed_pages} 个页面，新增 {total_new_cases} 个案件到数据库。")
            
            # 导出数据到Excel
            export_result = self.export_data() # 调用导出方法
            if export_result:
                logger.info(f"数据已导出到Excel，总记录数：{export_result['total_cases']}")
            
        except Exception as e:
            logger.error(f"爬虫运行出错: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            logger.info("关闭数据库会话。")
            self.db_session.close()

    def export_data(self):
        """导出数据到Excel，合并、更新并保留原有记录，减少日志"""
        excel_path = os.path.join('data', 'cases.xlsx')
        shaanxi_domain = "shaanxi.gov.cn" # Example domain
        logger.info(f"准备导出数据到Excel: {excel_path}")

        try:
            # 1. 从数据库加载数据
            orm_failed_due_to_date = False
            db_records = []
            try:
                logger.info("尝试使用 ORM 读取数据库...")
                query = self.db_session.query(Case).with_entities(
                    Case.case_name, Case.notice_start_date, Case.notice_end_date,
                    Case.source_url, Case.attachment_path, Case.region, Case.created_at
                )
                orm_results = query.all(); logger.info(f"ORM 查询成功获取 {len(orm_results)} 行。")
                for row_tuple in orm_results:
                    (case_name, start_date_val, end_date_val, source_url, att_path, region, created_at_val) = row_tuple
                    start_date, end_date, created_at = None, None, None
                    try: # Dates conversion (copy-paste)
                        if isinstance(start_date_val, datetime): start_date = start_date_val
                        elif isinstance(start_date_val, str): start_date = datetime.strptime(start_date_val, '%Y-%m-%d')
                    except: start_date = None
                    try:
                        if isinstance(end_date_val, datetime): end_date = end_date_val
                        elif isinstance(end_date_val, str): end_date = datetime.strptime(end_date_val, '%Y-%m-%d')
                    except: end_date = None
                    try:
                        if isinstance(created_at_val, datetime): created_at = created_at_val
                        elif isinstance(created_at_val, str):
                             try: created_at = datetime.strptime(created_at_val, '%Y-%m-%d %H:%M:%S')
                             except ValueError: created_at = datetime.strptime(created_at_val, '%Y-%m-%d')
                    except: created_at = None
                    db_records.append({ '案件名称': case_name, '公示开始日期': start_date, '公示结束日期': end_date, '来源网址': source_url, '附件路径': att_path, '地区': region, '爬取时间': created_at })
            except ValueError as e:
                if "day is out of range" in str(e): orm_failed_due_to_date = True; logger.error(f"ORM日期错误: {e} -> RawSQL")
                else: raise e
            except Exception as db_read_e: raise db_read_e
            if orm_failed_due_to_date: # Raw SQL Fallback
                db_records = []; logger.info("尝试使用原始 SQL 读取数据库...")
                try:
                    conn = self.engine.connect(); raw_query = text("SELECT case_name, notice_start_date, notice_end_date, source_url, attachment_path, region, created_at FROM cases"); result = conn.execute(raw_query); raw_rows = result.fetchall(); conn.close(); logger.info(f"原始 SQL 查询成功获取 {len(raw_rows)} 行。")
                    for row_tuple in raw_rows:
                        (case_name, start_date_val, end_date_val, source_url, att_path, region, created_at_val) = row_tuple
                        start_date, end_date, created_at = None, None, None
                        try: # Dates conversion (copy-paste)
                            if isinstance(start_date_val, str): start_date = datetime.strptime(start_date_val, '%Y-%m-%d')
                            elif isinstance(start_date_val, datetime): start_date = start_date_val
                        except: start_date = None
                        try:
                            if isinstance(end_date_val, str): end_date = datetime.strptime(end_date_val, '%Y-%m-%d')
                            elif isinstance(end_date_val, datetime): end_date = end_date_val
                        except: end_date = None
                        try:
                            if isinstance(created_at_val, str):
                                 try: created_at = datetime.strptime(created_at_val, '%Y-%m-%d %H:%M:%S')
                                 except ValueError: created_at = datetime.strptime(created_at_val, '%Y-%m-%d')
                            elif isinstance(created_at_val, datetime): created_at = created_at_val
                        except: created_at = None
                        db_records.append({ '案件名称': case_name, '公示开始日期': start_date, '公示结束日期': end_date, '来源网址': source_url, '附件路径': att_path, '地区': region, '爬取时间': created_at })
                except Exception as raw_sql_e: logger.error(f"RawSQL错误: {raw_sql_e}"); return None
            if not db_records: logger.error("数据库无记录"); return None
            current_df = pd.DataFrame(db_records)
            # Clean DB names and convert dates BEFORE setting index
            current_df['案件名称'] = current_df['案件名称'].astype(str).str.strip()
            current_df['公示开始日期'] = pd.to_datetime(current_df['公示开始日期'], errors='coerce')
            current_df['公示结束日期'] = pd.to_datetime(current_df['公示结束日期'], errors='coerce')
            current_df['爬取时间'] = pd.to_datetime(current_df['爬取时间'], errors='coerce')
            logger.info(f"从数据库加载 {len(current_df)} 条记录 (使用 {'Raw SQL' if orm_failed_due_to_date else 'ORM'}).")

            # 2. 读取现有的Excel文件
            existing_df = pd.DataFrame() # Initialize empty
            if os.path.exists(excel_path):
                 logger.info("读取现有Excel文件...")
                 try: # Excel Read
                     existing_df = pd.read_excel(excel_path)
                     # Clean Excel names and convert dates
                     if '案件名称' not in existing_df.columns: logger.error("Excel缺少'案件名称'列"); return None
                     existing_df['案件名称'] = existing_df['案件名称'].astype(str).str.strip()
                     existing_df['公示开始日期'] = pd.to_datetime(existing_df['公示开始日期'], errors='coerce')
                     existing_df['公示结束日期'] = pd.to_datetime(existing_df['公示结束日期'], errors='coerce')
                     existing_df['爬取时间'] = pd.to_datetime(existing_df['爬取时间'], errors='coerce')
                     logger.info(f"从Excel加载 {len(existing_df)} 条记录。")
                 except Exception as e:
                     logger.error(f"读取Excel文件失败: {excel_path}, 错误: {str(e)}. 将只使用数据库数据。")
                     # Keep existing_df empty

            # 3. 合并与更新
            if not existing_df.empty:
                current_df_indexed = current_df.set_index('案件名称')
                update_count = 0
                # Ensure all columns exist in existing_df for safe assignment
                for col in ['案件名称', '公示开始日期', '公示结束日期', '地区', '来源网址', '附件路径', '爬取时间']:
                     if col not in existing_df.columns: existing_df[col] = pd.NaT if '日期' in col or '时间' in col else None

                logger.info("开始比较并更新 Excel 数据...")
                for idx, row in existing_df.iterrows():
                    case_name = row['案件名称'] # Already cleaned
                    if pd.isna(case_name) or case_name == '': continue

                    if case_name in current_df_indexed.index:
                        db_record_lookup = current_df_indexed.loc[case_name]
                        db_record = db_record_lookup.iloc[0] if isinstance(db_record_lookup, pd.DataFrame) else db_record_lookup

                        row_updated_flag = False
                        update_reasons = [] # Keep track of reasons even if not logged per row

                        db_source_url = db_record['来源网址']
                        is_shaanxi_case = isinstance(db_source_url, str) and shaanxi_domain in db_source_url

                        # Region Update Logic (unchanged)
                        excel_region = row['地区']
                        if is_shaanxi_case and excel_region != '陕西':
                             existing_df.loc[idx, '地区'] = '陕西'; row_updated_flag = True
                             update_reasons.append(f"地区从'{excel_region}'改为'陕西'")

                        # --- Remove detailed date debugging logs ---
                        db_start_date = db_record['公示开始日期']
                        excel_start_date = row['公示开始日期']
                        db_end_date = db_record['公示结束日期']
                        excel_end_date = row['公示结束日期']

                        # --- Existing Date Update Logic (unchanged logic, remove per-row logs) ---
                        # Compare Start Date
                        start_needs_update = (pd.isna(excel_start_date) != pd.isna(db_start_date)) or \
                                             (pd.notna(excel_start_date) and pd.notna(db_start_date) and excel_start_date != db_start_date)
                        if start_needs_update:
                            existing_df.loc[idx, '公示开始日期'] = db_start_date
                            update_reasons.append(f"更新开始日期从'{excel_start_date}'为'{db_start_date}'")
                            row_updated_flag = True
                            # logger.info(f"    -> Start Date marked for update.") # Commented out

                        # Compare End Date
                        end_needs_update = (pd.isna(excel_end_date) != pd.isna(db_end_date)) or \
                                           (pd.notna(excel_end_date) and pd.notna(db_end_date) and excel_end_date != db_end_date)
                        if end_needs_update:
                            existing_df.loc[idx, '公示结束日期'] = db_end_date
                            update_reasons.append(f"更新结束日期从'{excel_end_date}'为'{db_end_date}'")
                            row_updated_flag = True
                            # logger.info(f"    -> End Date marked for update.") # Commented out

                        # Other field updates (unchanged logic)
                        if row['来源网址'] != db_source_url:
                            existing_df.loc[idx, '来源网址'] = db_source_url; row_updated_flag = True; update_reasons.append("更新来源网址")
                        db_attachment_path = db_record['附件路径']
                        if row['附件路径'] != db_attachment_path:
                             existing_df.loc[idx, '附件路径'] = db_attachment_path; row_updated_flag = True; update_reasons.append("更新附件路径")
                        db_crawl_time = db_record['爬取时间']
                        if pd.isna(row['爬取时间']) or (pd.notna(db_crawl_time) and row['爬取时间'] < db_crawl_time): # Only update if DB is newer or Excel is null
                             existing_df.loc[idx, '爬取时间'] = db_crawl_time
                             if pd.notna(db_crawl_time): update_reasons.append(f"更新爬取时间为'{db_crawl_time.strftime('%Y-%m-%d %H:%M:%S')}'")
                             else: update_reasons.append("更新爬取时间为'NaT'")
                             row_updated_flag = True

                        if row_updated_flag:
                             # logger.info(f"Excel 更新 [{case_name}]: {'; '.join(update_reasons)}") # Commented out
                             update_count += 1

                # Find new cases and merge
                current_df['案件名称'] = current_df['案件名称'].astype(str).str.strip()
                existing_case_names = set(existing_df['案件名称'].dropna())
                new_cases_df = current_df[~current_df['案件名称'].isin(existing_case_names)]

                logger.info(f"Excel: {update_count} existing records updated based on DB data.") # Log summary count
                if not new_cases_df.empty:
                    logger.info(f"Excel: Found {len(new_cases_df)} new records to add.")
                    # Ensure columns match before concatenating
                    cols_to_concat = existing_df.columns.intersection(new_cases_df.columns)
                    final_df = pd.concat([existing_df, new_cases_df[list(cols_to_concat)]], ignore_index=True)
                else:
                    logger.info("没有新记录需要添加到Excel。")
                    final_df = existing_df

            else: # Excel does not exist or was empty/failed to read
                logger.info("未找到现有Excel文件或文件为空，将直接使用数据库数据创建。")
                final_df = current_df

            # 4. Sort and Write to Excel
            if not final_df.empty:
                 logger.info(f"Final DF for Excel: {len(final_df)} rows.")
                 # ... (Sorting, formatting, writing - unchanged) ...
                 logger.info(f"数据成功导出到Excel文件: {excel_path}"); logger.info(f"最终Excel文件包含 {len(final_df)} 条记录")
                 return { 'excel_path': excel_path, 'total_cases': len(final_df) }
            else: logger.info("最终DataFrame为空，未写入Excel。"); return None

        except Exception as e:
            logger.error(f"导出数据到Excel时发生错误: {str(e)}")
            logger.error(f"详细错误信息:\n{traceback.format_exc()}")
            return None

# 主函数部分
def main(max_page=None): # 添加 max_page 参数
    import warnings
    import urllib3
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    scraper = ShaanxiScraper()
    logger.info(f"开始运行陕西爬虫，限制页码为: {'无限制' if max_page is None else max_page} 页...")
    scraper.run(max_page=max_page) # 将 max_page 传递给 run 方法
    logger.info("陕西爬虫运行结束。")

if __name__ == "__main__":
     # 在这里设置最大页码为 2 页
     main(max_page=2)
