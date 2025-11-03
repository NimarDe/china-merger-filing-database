from apscheduler.schedulers.blocking import BlockingScheduler
from scraper import SamrScraper
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='scraper.log'
)

def run_scraper():
    scraper = SamrScraper()
    scraper.run()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    # 每天凌晨2点运行
    scheduler.add_job(run_scraper, 'cron', hour=2)
    scheduler.start()