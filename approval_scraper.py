import os
import sqlite3
from datetime import datetime
from playwright.sync_api import sync_playwright
import logging
from bs4 import BeautifulSoup
import re

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ApprovalScraper:
    def __init__(self):
        self.base_url = "https://www.samr.gov.cn/fldes/ajgs/wtjjz/index.html"
        self.db_path = "data/cases.db"
        
    def convert_chinese_date(self, date_str):
        """将中文日期格式转换为SQLite日期格式"""
        try:
            # 处理格式如 "2025年3月31日"
            pattern = re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日')
            match = pattern.match(date_str)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month:0>2}-{day:0>2}"
            return None
        except Exception as e:
            logger.error(f"日期转换错误: {date_str}, 错误: {str(e)}")
            return None

    def process_detail_page(self, table, db_conn, page_url):
        """处理详情页内容"""
        try:
            rows = table.select('tr')
            logger.info(f"处理表格，共有 {len(rows)} 行")
            
            cursor = db_conn.cursor()
            
            # 处理数据行
            for i, row in enumerate(rows[1:], 1):
                columns = row.select('td')
                if len(columns) >= 4:
                    case_name = columns[1].text.strip()
                    parties = columns[2].text.strip()
                    raw_date = columns[3].text.strip()
                    logger.info(f"处理第 {i} 行: 案件名称={case_name}, 参与者={parties}, 原始日期={raw_date}")
                    
                    decision_date = self.convert_chinese_date(raw_date)
                    if not decision_date:
                        logger.warning(f"日期转换失败: {raw_date}")
                        continue
                    
                    # 查找是否存在匹配的案件
                    cursor.execute("""
                        SELECT id FROM cases 
                        WHERE case_name = ?
                    """, (case_name,))
                    
                    existing_case = cursor.fetchone()
                    
                    if existing_case:
                        # 更新已存在的记录
                        cursor.execute("""
                            UPDATE cases 
                            SET 参与集中的经营者 = ?,
                                审结时间 = ?,
                                是否已匹配 = '是'
                            WHERE id = ?
                        """, (parties, decision_date, existing_case[0]))
                        logger.info(f"更新记录: {case_name}")
                    else:
                        # 创建新记录，包含 source_url
                        cursor.execute("""
                            INSERT INTO cases (
                                case_name, 参与集中的经营者, 审结时间, 是否已匹配, source_url
                            ) VALUES (?, ?, ?, '否', ?)
                        """, (case_name, parties, decision_date, page_url))
                        logger.info(f"新增记录: {case_name}")
            
            db_conn.commit()
            logger.info("完成表格处理并提交事务")
            
        except Exception as e:
            logger.error(f"处理表格错误: {str(e)}")
            logger.error("错误详情: ", exc_info=True)
            db_conn.rollback()

    def scrape(self):
        """开始爬取流程"""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                # 连接数据库
                db_conn = sqlite3.connect(self.db_path)
                logger.info(f"成功连接到数据库: {self.db_path}")
                
                # 访问主页
                logger.info(f"访问页面: {self.base_url}")
                page.goto(self.base_url)
                page.wait_for_load_state('networkidle')
                
                current_page = 1
                max_pages = 2  # 只爬取第1页和第2页
                
                while current_page <= max_pages:
                    logger.info(f"正在处理列表第 {current_page}/{max_pages} 页")
                    
                    # 等待列表页内容和分页组件加载
                    page.wait_for_selector('div.list-content', state='visible')
                    page.wait_for_selector('#layui-laypage-1', state='visible')
                    
                    # 获取所有详情页链接
                    links = page.query_selector_all('div.list-content a')
                    logger.info(f"找到 {len(links)} 个链接")
                    
                    # 筛选有效的详情页链接
                    valid_hrefs = []
                    for link in links:
                        href = link.get_attribute('href')
                        text = link.text_content()
                        # 筛选条件：链接文本包含"无条件批准经营者集中案件列表"
                        if href and "无条件批准经营者集中案件列表" in text:
                            full_url = 'https://www.samr.gov.cn' + href if href.startswith('/') else href
                            valid_hrefs.append(full_url)
                    
                    logger.info(f"筛选出 {len(valid_hrefs)} 个有效的详情页链接")
                    
                    # 访问每个详情页
                    for href in valid_hrefs:
                        logger.info(f"访问详情页: {href}")
                        detail_page = browser.new_page()
                        detail_page.goto(href)
                        detail_page.wait_for_load_state('networkidle')
                        
                        # 等待表格加载
                        detail_page.wait_for_selector('table', state='visible')
                        detail_page.wait_for_timeout(2000)
                        
                        # 获取详情页内容
                        detail_content = detail_page.content()
                        detail_soup = BeautifulSoup(detail_content, 'html.parser')
                        
                        # 查找所有表格
                        tables = detail_soup.find_all('table')
                        logger.info(f"详情页中找到 {len(tables)} 个表格")
                        
                        # 查找包含正确表头的表格
                        target_table = None
                        for table in tables:
                            header_row = table.select_one('tr')
                            if header_row:
                                header_cells = header_row.select('th, td')
                                header_texts = [cell.text.strip() for cell in header_cells]
                                # 检查表头是否包含所需的所有列
                                if (len(header_texts) >= 4 and 
                                    '序号' in header_texts[0] and 
                                    '案件名称' in header_texts[1] and 
                                    '参与集中的经营者' in header_texts[2] and 
                                    '审结时间' in header_texts[3]):
                                    target_table = table
                                    logger.info("找到目标表格，表头匹配")
                                    break
                        
                        if target_table:
                            self.process_detail_page(target_table, db_conn, href)
                        else:
                            logger.error("未找到符合条件的表格")
                        
                        # 关闭详情页
                        detail_page.close()
                    
                    if current_page >= max_pages:
                        break
                    
                    # 修改后的翻页逻辑
                    try:
                        # 等待分页组件加载
                        page.wait_for_selector('#layui-laypage-1', state='visible')
                        
                        # 找到下一页按钮
                        next_page = page.query_selector(f'a.layui-laypage-next')
                        
                        if next_page:
                            logger.info(f"点击下一页按钮 (页码 {current_page + 1})")
                            next_page.click()
                            # 等待页面加载完成
                            page.wait_for_load_state('networkidle')
                            # 等待新页面的内容加载
                            page.wait_for_selector('div.list-content', state='visible')
                            # 确保分页组件重新加载
                            page.wait_for_selector('#layui-laypage-1', state='visible')
                            # 额外等待确保内容更新
                            page.wait_for_timeout(2000)
                            current_page += 1
                        else:
                            logger.error(f"未找到下一页按钮 (当前页码 {current_page})")
                            break
                        
                    except Exception as e:
                        logger.error(f"翻页过程出错: {str(e)}")
                        logger.error("错误详情: ", exc_info=True)
                        break
                
            except Exception as e:
                logger.error(f"爬取过程错误: {str(e)}")
                logger.error("错误详情: ", exc_info=True)
            finally:
                db_conn.close()
                browser.close()

if __name__ == "__main__":
    scraper = ApprovalScraper()
    scraper.scrape()
