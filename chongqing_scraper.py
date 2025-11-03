import asyncio
import logging
import os
import requests
from datetime import datetime
from urllib.parse import urljoin, unquote
from bs4 import BeautifulSoup
from scraper import CaseScraper
from models import Case, Base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import traceback
import re

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChongqingScraper:
    def __init__(self):
        """初始化重庆爬虫"""
        self.base_url = "https://scjgj.cq.gov.cn/zt_225/jyzjzfldsc/ajgs/jyzjzjyajgs/index.html"
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
            case_links = soup.select('ul.gl-list li a')
            
            if case_links:
                logger.info(f"在列表容器中找到 {len(case_links)} 个链接")
                for link in case_links:
                    title = link.get_text(strip=True)
                    href = link.get('href')
                    
                    if href and title:
                        if not href.startswith('http'):
                            href = urljoin(url, href)
                        cases.append({
                            'title': title,
                            'url': href
                        })
                        logger.info(f"找到案件: {title}")
            
            if not cases:
                logger.error(f"未找到案件列表，页面内容长度: {len(response.text)}")
                # 打印页面中的列表容器内容
                list_container = soup.select_one('ul.gl-list')
                if list_container:
                    logger.debug(f"列表容器内容: {list_container.prettify()}")
                else:
                    logger.debug("未找到列表容器")
                return None
            
            logger.info(f"共找到 {len(cases)} 个案件")
            
            # 获取当前页码和构造下一页链接
            current_url_parts = url.split('/')
            if 'index.html' in current_url_parts[-1]:
                current_page = 1
                next_page = url.replace('index.html', 'index_1.html')
            else:
                # 从URL中提取当前页码
                current_page_str = current_url_parts[-1].split('_')[-1].split('.')[0]
                try:
                    current_page = int(current_page_str) + 1
                    # 构造下一页URL
                    base_path = '/'.join(current_url_parts[:-1])
                    next_page = f"{base_path}/index_{current_page}.html"
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
        """解析详情页，处理关键词中的空格"""
        try:
            logger.info(f"访问详情页: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取标题
            title_tag = soup.select_one('.view-title')
            title = title_tag.get_text(strip=True) if title_tag else None
            
            # 从 meta description 中提取日期
            start_date = None
            end_date = None
            meta_desc = soup.find('meta', {'name': 'Description'})
            if meta_desc and meta_desc.get('content'):
                content = meta_desc['content']
                # 使用更灵活的正则表达式匹配日期范围，处理关键词中的空格
                date_pattern = r'公\s*示\s*[期日]\s*[期]*\s*[：:]\s*(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)\s*至\s*(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)'
                match = re.search(date_pattern, content)
                if match:
                    try:
                        # 清理日期字符串中的空格
                        start_date_str = re.sub(r'\s+', '', match.group(1))
                        end_date_str = re.sub(r'\s+', '', match.group(2))
                        
                        # 转换中文日期格式为 datetime 对象
                        start_date = datetime.strptime(start_date_str, '%Y年%m月%d日')
                        end_date = datetime.strptime(end_date_str, '%Y年%m月%d日')
                        logger.info(f"从meta标签成功提取日期范围: {start_date.date()} 至 {end_date.date()}")
                    except ValueError as e:
                        logger.error(f"日期格式转换失败: {str(e)}, 原始日期字符串: '{match.group(1)}' 至 '{match.group(2)}'")
                else:
                    logger.warning(f"在meta标签中未找到日期范围，内容: {content}")
            else:
                logger.warning("未找到Description meta标签")
            
            # 如果meta标签中没有找到日期，尝试从正文内容中查找
            content_tag = soup.select_one('.view-content')
            if content_tag:
                content_text = content_tag.get_text(strip=True)
                # 在正文中使用相同的灵活匹配模式
                date_pattern = r'公\s*示\s*[期日]\s*[期]*\s*[：:]\s*(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)\s*至\s*(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)'
                match = re.search(date_pattern, content_text)
                if match:
                    try:
                        # 清理日期字符串中的空格
                        start_date_str = re.sub(r'\s+', '', match.group(1))
                        end_date_str = re.sub(r'\s+', '', match.group(2))
                        
                        start_date = datetime.strptime(start_date_str, '%Y年%m月%d日')
                        end_date = datetime.strptime(end_date_str, '%Y年%m月%d日')
                        logger.info(f"从正文内容成功提取日期范围: {start_date.date()} 至 {end_date.date()}")
                    except ValueError as e:
                        logger.error(f"正文日期格式转换失败: {str(e)}, 原始日期字符串: '{match.group(1)}' 至 '{match.group(2)}'")
        
            # 获取内容
            content_tag = soup.select_one('.view-content')
            content = content_tag.get_text(strip=True) if content_tag else None
            
            # 获取附件链接
            attachment_url = None
            attachment_name = None
            if content_tag:
                for link in content_tag.select('a'):
                    href = link.get('href')
                    if href and any(href.lower().endswith(ext) for ext in ['.doc', '.docx', '.pdf', '.xls', '.xlsx', '.zip', '.rar']):
                        attachment_url = urljoin(url, href)
                        attachment_name = link.get_text(strip=True)
                        logger.info(f"找到附件链接: {attachment_url}")
                        break

            return {
                'title': title,
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

    async def download_attachment(self, case_name, attachment_url):
        """下载附件"""
        if not attachment_url:
            return None
        
        try:
            # 从URL获取文件名，并进行解码
            parsed_url = urlparse(attachment_url)
            file_name = os.path.basename(unquote(parsed_url.path))

            # 如果无法从URL获取文件名，尝试使用case_name和文件后缀
            if not file_name or '.' not in file_name:
                 ext_match = re.search(r'\.(\w+)$', attachment_url.lower())
                 ext = ext_match.group(1) if ext_match else 'bin' # 默认后缀
                 # 清理case_name作为文件名
                 safe_case_name = re.sub(r'[\\/*?:"<>|]', "_", case_name)
                 file_name = f"{safe_case_name}.{ext}"

            # 完整的本地保存路径
            file_path = os.path.join(self.attachment_dir, file_name)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(file_path):
                 logger.info(f"附件已存在，跳过下载: {file_name}")
                 return file_path

            logger.info(f"开始下载附件: {attachment_url} 到 {file_path}")
            response = self.session.get(attachment_url, stream=True, timeout=120)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"成功下载附件: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"下载附件失败: {attachment_url}, 错误: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def process_case(self, case):
        """处理单个案件并更新数据库"""
        try:
            case_title = case['title']
            case_url = case['url']
            
            # 按案件名查找是否存在
            existing_case = self.db_session.query(Case).filter_by(case_name=case_title).first()
            
            detail_data = await self.parse_detail_page(case_url)
            if not detail_data:
                logger.error(f"无法解析案件详情: {case_title} ({case_url})")
                return False

            # 2. 处理附件下载（按案件名去重）
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
                    await self.download_attachment(case_title, detail_data['attachment_url'])

            # 3. 数据库更新逻辑
            if existing_case:
                # 更新已有记录
                need_update = False
                
                # 检查并更新地区
                if existing_case.region != '重庆':
                    logger.info(f"更新案件地区 [{case_title}]: {existing_case.region} -> 重庆")
                    existing_case.region = '重庆'
                    need_update = True
                
                # 检查并更新开始日期（如果原记录为空且新数据有值）
                if not existing_case.notice_start_date and detail_data['start_date']:
                    logger.info(f"更新案件开始日期 [{case_title}]: None -> {detail_data['start_date']}")
                    existing_case.notice_start_date = detail_data['start_date']
                    need_update = True
                
                # 检查并更新结束日期（如果原记录为空且新数据有值）
                if not existing_case.notice_end_date and detail_data['end_date']:
                    logger.info(f"更新案件结束日期 [{case_title}]: None -> {detail_data['end_date']}")
                    existing_case.notice_end_date = detail_data['end_date']
                    need_update = True
                
                # 如果有更新，提交到数据库
                if need_update:
                    try:
                        self.db_session.commit()
                        logger.info(f"成功更新案件信息: {case_title}")
                    except Exception as e:
                        self.db_session.rollback()
                        logger.error(f"更新案件信息失败: {case_title}, 错误: {str(e)}")
                else:
                    logger.info(f"案件信息无需更新: {case_title}")
                
                return False  # 返回False表示不是新案件
                
            else:
                # 创建新案件记录
                try:
                    new_case = Case(
                        case_name=case_title,
                        notice_start_date=detail_data['start_date'],
                        notice_end_date=detail_data['end_date'],
                        source_url=case_url,
                        attachment_path=attachment_path,
                        region='重庆'
                    )
                    self.db_session.add(new_case)
                    self.db_session.commit()
                    logger.info(f"成功添加新案件: {case_title}")
                    return True  # 返回True表示是新案件
                    
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
                    break # 如果列表页解析失败，则停止

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
                            await asyncio.sleep(0.5) # 每个案件处理后短暂延迟
                        except Exception as e:
                            logger.error(f"处理案件失败: {case.get('title', 'N/A')}, 错误: {str(e)}")
                else:
                    logger.warning(f"第 {current_page} 页未找到案件")
                    # 如果某一页没找到案件，可以考虑是否停止，这里我们继续，因为可能是网站临时问题
                
                # 获取下一页URL
                current_url = page_data['next_page']
                if current_url:
                    logger.info(f"准备处理下一页: {current_url}")
                    await asyncio.sleep(1) # 页面间延迟
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
        """导出数据到Excel，保持与数据库相同的更新逻辑"""
        try:
            excel_path = 'data/cases.xlsx'
            logger.info(f"开始导出数据到: {excel_path}")

            # 1. 从数据库读取所有记录
            db_cases = self.db_session.query(Case).all()
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
            
            # 转换为DataFrame
            current_df = pd.DataFrame(db_records)
            
            # 2. 如果Excel文件已存在，读取并与数据库记录合并
            if os.path.exists(excel_path):
                try:
                    existing_df = pd.read_excel(excel_path)
                    
                    # 确保列名一致
                    existing_df.columns = ['案件名称', '公示开始日期', '公示结束日期', 
                                         '来源链接', '附件路径', '地区']
                    
                    # 转换日期列
                    for date_col in ['公示开始日期', '公示结束日期']:
                        existing_df[date_col] = pd.to_datetime(existing_df[date_col], errors='coerce')
                        current_df[date_col] = pd.to_datetime(current_df[date_col], errors='coerce')

                    # 使用案件名称作为索引
                    existing_df.set_index('案件名称', inplace=True)
                    current_df.set_index('案件名称', inplace=True)

                    # 对每个案件进行检查和更新
                    for case_name in current_df.index:
                        if case_name in existing_df.index:
                            # 案件已存在，检查是否需要更新
                            existing_case = existing_df.loc[case_name]
                            current_case = current_df.loc[case_name]
                            
                            # 检查并更新地区
                            if existing_case['地区'] != '重庆':
                                existing_df.at[case_name, '地区'] = '重庆'
                                logger.info(f"Excel更新案件地区 [{case_name}]: {existing_case['地区']} -> 重庆")
                            
                            # 检查并更新日期（仅当原记录为空且新数据有值时）
                            if pd.isna(existing_case['公示开始日期']) and not pd.isna(current_case['公示开始日期']):
                                existing_df.at[case_name, '公示开始日期'] = current_case['公示开始日期']
                                logger.info(f"Excel更新案件开始日期 [{case_name}]")
                            
                            if pd.isna(existing_case['公示结束日期']) and not pd.isna(current_case['公示结束日期']):
                                existing_df.at[case_name, '公示结束日期'] = current_case['公示结束日期']
                                logger.info(f"Excel更新案件结束日期 [{case_name}]")
                        else:
                            # 新案件，添加到现有DataFrame
                            existing_df.loc[case_name] = current_df.loc[case_name]
                            logger.info(f"Excel添加新案件: {case_name}")

                    # 重置索引，准备保存
                    existing_df.reset_index(inplace=True)
                    final_df = existing_df
                except Exception as e:
                    logger.error(f"处理现有Excel文件时出错: {str(e)}")
                    final_df = current_df.reset_index()
            else:
                final_df = current_df.reset_index()

            # 3. 保存到Excel
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

async def main():
    """主函数"""
    try:
        # 创建数据库引擎
        engine = create_engine(f"sqlite:///data/cases.db")
        
        # 获取当前记录数
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM cases"))
            initial_count = result.scalar()
            logger.info(f"数据库中现有记录数: {initial_count}")
        
        # 创建并运行爬虫
        scraper = ChongqingScraper()
        await scraper.run()
        
        # 导出数据
        try:
            export_result = scraper.export_data()
            if export_result:
                # 计算新增记录数
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM cases"))
                    final_count = result.scalar()
                new_records = final_count - initial_count
                
                logger.info(f"数据导出成功:")
                logger.info(f"- 原有记录数: {initial_count}")
                logger.info(f"- 新增记录数: {new_records}")
                logger.info(f"- 当前总记录数: {final_count}")
            else:
                logger.error("数据导出失败")
        except Exception as e:
            logger.error(f"导出数据时发生错误: {str(e)}")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")

if __name__ == "__main__":
    import warnings
    from urllib.parse import urlparse # 确保导入
    import urllib3 # 确保导入
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    scraper = ChongqingScraper()
    # 运行爬虫，设置最高页码为 2
    logger.info("开始运行重庆爬虫，限制页码为 12 页...")
    asyncio.run(scraper.run(max_page=12))
    logger.info("爬虫运行结束（限制 12 页）。")
