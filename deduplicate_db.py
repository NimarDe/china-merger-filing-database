import sqlite3
import logging
import os
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/cases.db'
TABLE_NAME = 'cases'
COLUMN_TO_DEDUPLICATE = 'source_url'

def deduplicate_by_source_url():
    """
    Connects to the SQLite database, identifies duplicates based on source_url,
    and removes older duplicates, keeping only the one with the highest rowid.
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"数据库文件未找到: {DB_PATH}")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logger.info(f"已连接到数据库: {DB_PATH}")

        # --- 1. Count total rows before deduplication ---
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        initial_count = cursor.fetchone()[0]
        logger.info(f"去重前总记录数: {initial_count}")
        if initial_count == 0:
             logger.info("数据库表为空，无需去重。")
             return

        # --- 2. Identify and count duplicates (optional but informative) ---
        find_duplicates_sql = f"""
            SELECT {COLUMN_TO_DEDUPLICATE}, COUNT(*) as count
            FROM {TABLE_NAME}
            WHERE {COLUMN_TO_DEDUPLICATE} IS NOT NULL AND {COLUMN_TO_DEDUPLICATE} != ''
            GROUP BY {COLUMN_TO_DEDUPLICATE}
            HAVING COUNT(*) > 1;
        """
        cursor.execute(find_duplicates_sql)
        duplicates = cursor.fetchall()
        num_duplicate_urls = len(duplicates)
        total_duplicate_records = sum(count for _, count in duplicates)
        records_to_be_deleted_estimate = total_duplicate_records - num_duplicate_urls

        if num_duplicate_urls > 0:
            logger.info(f"发现 {num_duplicate_urls} 个重复的 {COLUMN_TO_DEDUPLICATE}，涉及 {total_duplicate_records} 条记录。")
            logger.info(f"预计将删除约 {records_to_be_deleted_estimate} 条重复记录。")
            # for url, count in duplicates[:5]: # Log first few duplicates
            #     logger.debug(f"  - URL: {url}, Count: {count}")
        else:
            logger.info("未发现重复的记录。")
            return # No need to proceed if no duplicates

        # --- 3. Delete duplicates, keeping the one with the highest rowid ---
        # Explanation:
        # - Select the MAX(rowid) for each unique source_url.
        # - Delete all rows whose rowid is NOT in this list of max rowids.
        delete_sql = f"""
            DELETE FROM {TABLE_NAME}
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM {TABLE_NAME}
                GROUP BY {COLUMN_TO_DEDUPLICATE}
            );
        """
        logger.info("正在执行删除重复记录的操作...")
        cursor.execute(delete_sql)
        deleted_count = cursor.rowcount
        logger.info(f"成功执行删除操作，影响行数: {deleted_count} (这可能包括非重复项，如果GROUP BY列有NULL)") # rowcount might not be exact for this type of query in older sqlite

        # --- 4. Commit changes ---
        conn.commit()
        logger.info("数据库更改已提交。")

        # --- 5. Count total rows after deduplication ---
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]
        actual_deleted = initial_count - final_count
        logger.info(f"去重后总记录数: {final_count}")
        logger.info(f"实际删除的记录数: {actual_deleted}")


    except sqlite3.Error as e:
        logger.error(f"数据库操作失败: {e}")
        logger.error(traceback.format_exc())
        if conn:
            conn.rollback() # Rollback changes on error
            logger.info("更改已回滚。")
    except Exception as e:
        logger.error(f"执行过程中发生意外错误: {e}")
        logger.error(traceback.format_exc())
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭。")

if __name__ == "__main__":
    logger.info("开始执行数据库去重脚本...")
    deduplicate_by_source_url()
    logger.info("数据库去重脚本执行完毕。")
