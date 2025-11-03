import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Case, Base
from config import CONFIG
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 地区代码到中文名称的映射
REGION_MAP = {
    'beijing': '北京市',
    'shanghai': '上海市',
    'chongqing': '重庆市',
    'guangdong': '广东省',
    'shaanxi': '陕西省',
    'samr': '国家市场监督管理总局'
}

def get_region_name(region_code):
    """将地区代码转换为中文名称"""
    return REGION_MAP.get(region_code, region_code)

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

if __name__ == "__main__":
    export_data() 