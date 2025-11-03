import sqlite3
import os

def alter_database():
    # 确保数据库文件存在
    db_path = 'data/cases.db'
    if not os.path.exists(db_path):
        print(f"错误：数据库文件 {db_path} 不存在")
        return False

    try:
        # 连接到数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 添加新列
        alter_statements = [
            "ALTER TABLE cases ADD COLUMN 参与集中的经营者 TEXT;",
            "ALTER TABLE cases ADD COLUMN 审结时间 DATE;",
            "ALTER TABLE cases ADD COLUMN 是否已匹配 TEXT;"
        ]

        for statement in alter_statements:
            try:
                cursor.execute(statement)
                print(f"成功执行: {statement}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    print(f"列已存在，跳过: {statement}")
                else:
                    raise e

        # 提交更改
        conn.commit()
        print("数据库修改完成")

    except Exception as e:
        print(f"发生错误: {str(e)}")
        return False
    finally:
        conn.close()

    return True

if __name__ == "__main__":
    alter_database()
