import asyncio
import logging
import random
from scraper import CaseScraper
from config import CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def random_delay():
    """随机延迟函数"""
    delay = random.uniform(CONFIG['RANDOM_DELAY']['MIN'], CONFIG['RANDOM_DELAY']['MAX'])
    logger.info(f"等待随机延迟 {delay:.2f} 秒...")
    await asyncio.sleep(delay)

async def main():
    """测试主页面解析"""
    try:
        # 创建爬虫实例
        scraper = CaseScraper()
        
        # 测试第1页到第3页
        for page in range(CONFIG['START_PAGE'], CONFIG['END_PAGE'] + 1):
            logger.info(f"\n=== 测试第 {page} 页 ===")
            
            # 添加随机延迟
            if page > 1:
                await random_delay()
            
            cases = await scraper.parse_list_page_playwright(CONFIG['BASE_URL'], page)
            if cases:
                logger.info(f"成功获取到 {len(cases)} 个案件")
                logger.info("前3个案件的详细信息：")
                for case in cases[:3]:  # 只显示前3个案件
                    logger.info(f"标题: {case['title']}")
                    logger.info(f"链接: {case['url']}")
                    logger.info(f"日期: {case['date']}")
                    logger.info("---")
                
                # 统计信息
                dates = [case['date'] for case in cases if case['date']]
                if dates:
                    logger.info(f"该页案件日期范围: {min(dates)} 至 {max(dates)}")
            else:
                logger.error(f"第 {page} 页未获取到案件")
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise e

if __name__ == "__main__":
    asyncio.run(main()) 