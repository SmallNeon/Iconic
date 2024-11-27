import sqlite3
import os
import pydicom
import magic
from tqdm import tqdm

def add_column_if_not_exists(db_path, table_name, column_name, column_type):
    """
    如果表中不存在指定列，则添加该列。
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 检查列是否存在
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        if column_name not in columns:
            # 添加列
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")
            print(f"已在表 '{table_name}' 中添加列 '{column_name}'。")
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"添加列时出错: {e}")
    finally:
        if conn:
            conn.close()

def detect_file_format(file_path):
    """
    检测文件的实际格式。
    """
    try:
        # 尝试使用 pydicom 读取文件
        dicom_data = pydicom.dcmread(file_path, stop_before_pixels=True)
        if dicom_data:
            return "DICOM"
    except Exception:
        pass

    try:
        # 使用 magic 库检测文件类型
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(file_path)
        return file_type
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")
        return "unknown"

def update_actual_file_types(db_path):
    """
    使用 pydicom 和 magic 库更新数据库中的实际文件类型，并显示进度。
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 获取 actual_file_type 为 NULL 或空字符串的所有记录
        cursor.execute("SELECT id, file_path FROM patient_data WHERE actual_file_type IS NULL OR actual_file_type = '';")
        records = cursor.fetchall()

        total_files = len(records)
        print(f"需要处理的文件总数: {total_files}")

        # 使用 tqdm 显示进度条
        for record in tqdm(records, desc="处理文件", unit="文件"):
            record_id, file_path = record
            if os.path.exists(file_path):
                file_format = detect_file_format(file_path)
                # 更新数据库中的实际文件类型
                cursor.execute("UPDATE patient_data SET actual_file_type = ? WHERE id = ?", (file_format, record_id))
            else:
                print(f"文件未找到: {file_path}")

        conn.commit()
        print("文件类型已成功更新。")
    
    except sqlite3.Error as e:
        print(f"数据库错误: {e}")
    finally:
        if conn:
            conn.close()

# SQLite 数据库的路径
database_path = "/home/molloi-lab-linux2/Desktop/Andrew/Iconic/patient_data.db"

# 确保列存在
add_column_if_not_exists(database_path, "patient_data", "actual_file_type", "TEXT")

# 更新实际文件类型
update_actual_file_types(database_path)
