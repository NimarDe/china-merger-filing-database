import asyncio
import logging
import random
import os
import time
from datetime import datetime, timedelta
from scraper import CaseScraper
from config import CONFIG
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Case, Base
from bs4 import BeautifulSoup
from parsers import create_parser
import pandas as pd
from sqlalchemy import text
from urllib.parse import urljoin
import re

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    """将日期字符串转换为datetime对象"""
    try:
        if isinstance(date_str, datetime):
            return date_str
        return datetime.strptime(date_str, '%Y-%m-%d')
    except Exception as e:
        logger.error(f"日期转换失败: {date_str}, 错误: {str(e)}")
        return None

async def random_delay():
    """随机延迟函数"""
    delay = random.uniform(CONFIG['RANDOM_DELAY']['MIN'], CONFIG['RANDOM_DELAY']['MAX'])
    logger.info(f"等待随机延迟 {delay:.2f} 秒...")
    await asyncio.sleep(delay)

def format_time(seconds):
    """格式化时间"""
    return str(timedelta(seconds=int(seconds)))

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
        
        # 设置要爬取的页面范围
        start_page = 15  
        end_page = 27
        total_pages = end_page - start_page + 1  # 计算实际需要爬取的页数
        
        logger.info(f"开始爬取第 {start_page} 页到第 {end_page} 页，共 {total_pages} 页")
        
        # 基础URL
        base_url = "https://www.samr.gov.cn/fldes/ajgs/jyaj/"
        
        # 使用异步上下文管理器创建爬虫实例
        async with CaseScraper() as scraper:
            total_processed = 0
            total_success = 0
            
            # 遍历每一页
            pages_processed = 0
            for target_page in range(start_page, end_page + 1):
                pages_processed += 1
                logger.info(f"正在处理第 {target_page} 页 (进度: {pages_processed}/{total_pages})")
                
                # 使用改进的playwright方法获取案件列表
                cases = await scraper.parse_list_page_playwright(base_url, target_page)
                
                if not cases:
                    logger.error(f"第 {target_page} 页获取案件列表失败")
                    continue
                
                logger.info(f"第 {target_page} 页获取到 {len(cases)} 个案件")
                
                # 处理每个案件
                page_success = 0
                for case in cases:
                    try:
                        total_processed += 1
                        logger.info(f"正在处理第 {total_processed} 个案件: {case['title']}")
                        logger.info(f"当前页进度: {page_success + 1}/{len(cases)}")
                        
                        # 处理案件详情
                        result = await scraper.process_case(case)
                        if result:
                            total_success += 1
                            page_success += 1
                            logger.info(f"案件处理成功: {result['case_name']}")
                        else:
                            logger.error(f"案件处理失败: {case['title']}")
                            
                    except Exception as e:
                        logger.error(f"处理案件时发生错误: {str(e)}")
                        continue
                
                logger.info(f"第 {target_page} 页处理完成，成功: {page_success}/{len(cases)}")
                logger.info(f"总进度: {pages_processed}/{total_pages} 页 ({(pages_processed/total_pages*100):.1f}%)")
                
                # 每页处理完后等待随机时间
                await random_delay()
            
            logger.info(f"所有页面处理完成:")
            logger.info(f"- 总处理页数: {total_pages} 页")
            logger.info(f"- 总处理案件: {total_processed} 个")
            logger.info(f"- 成功处理: {total_success} 个")
            logger.info(f"- 处理成功率: {(total_success/total_processed*100):.1f}%")
            
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
                    logger.info(f"- Excel文件位置: {export_result['excel_path']}")
                else:
                    logger.error("数据导出失败")
            except Exception as e:
                logger.error(f"导出数据时发生错误: {str(e)}")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())