import pandas as pd
import logging
import os
from datetime import datetime
from config import CONFIG
from models import Case  # Add this import

logger = logging.getLogger(__name__)

class DataExporter:
    def __init__(self, db_session):
        self.db_session = db_session

    def _prepare_case_data(self, case):
        """准备单个案件的数据，确保所有值都是Python原生类型"""
        start_date = case.notice_start_date.strftime('%Y-%m-%d') if case.notice_start_date else None
        end_date = case.notice_end_date.strftime('%Y-%m-%d') if case.notice_end_date else None
        created_at = case.created_at.strftime('%Y-%m-%d %H:%M:%S') if case.created_at else None
        
        return {
            '案件名称': str(case.case_name) if case.case_name else None,
            '公示开始日期': str(start_date) if start_date else None,
            '公示结束日期': str(end_date) if end_date else None,
            '来源网址': str(case.source_url) if case.source_url else None,
            '地区': str(case.region) if case.region else None,
            '附件路径': str(case.attachment_path) if case.attachment_path else None,
            '创建时间': str(created_at) if created_at else None
        }

    def export_to_excel(self, custom_path=None):
        """导出数据到Excel"""
        try:
            cases = self.db_session.query(Case).all()
            data = []
            
            for case in cases:
                # 转换日期为字符串格式
                start_date = case.notice_start_date.strftime('%Y-%m-%d') if case.notice_start_date else None
                end_date = case.notice_end_date.strftime('%Y-%m-%d') if case.notice_end_date else None
                created_at = case.created_at.strftime('%Y-%m-%d %H:%M:%S') if case.created_at else None
                
                # 确保所有字段都是Python原生类型
                row = {
                    '案件名称': str(case.case_name) if case.case_name else None,
                    '公示开始日期': str(start_date) if start_date else None,
                    '公示结束日期': str(end_date) if end_date else None,
                    '来源网址': str(case.source_url) if case.source_url else None,
                    '地区': str(case.region) if case.region else None,
                    '附件路径': str(case.attachment_path) if case.attachment_path else None,
                    '创建时间': str(created_at) if created_at else None
                }
                data.append(row)
            
            # 创建DataFrame并指定数据类型
            df = pd.DataFrame(data)
            
            # 确定导出路径
            export_path = custom_path or CONFIG['EXCEL_PATH']
            os.makedirs(os.path.dirname(export_path), exist_ok=True)
            
            # 导出数据，使用openpyxl引擎
            with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            logger.info(f"数据已成功导出到: {export_path}")
            return export_path
            
        except Exception as e:
            logger.error(f"导出Excel失败: {str(e)}")
            return None

    def export_to_csv(self, custom_path=None):
        """导出数据到CSV"""
        try:
            cases = self.db_session.query(Case).all()
            data = [self._prepare_case_data(case) for case in cases]
            
            df = pd.DataFrame(data)
            
            # 生成默认CSV路径
            default_path = os.path.join(
                os.path.dirname(CONFIG['EXCEL_PATH']),
                f'cases_{datetime.now().strftime("%Y%m%d")}.csv'
            )
            
            export_path = custom_path or default_path
            os.makedirs(os.path.dirname(export_path), exist_ok=True)
            
            df.to_csv(export_path, index=False, encoding='utf-8-sig')
            logger.info(f"数据已成功导出到: {export_path}")
            return export_path
            
        except Exception as e:
            logger.error(f"导出CSV失败: {str(e)}")
            return None