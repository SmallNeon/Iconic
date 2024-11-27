import sqlite3
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class DBManager:
    def __init__(self, db_path, full_dataset_path):
        self.db_path = db_path
        self.full_dataset_path = full_dataset_path

    def help(self, language=None):
        # 获取系统默认语言或使用用户指定语言
        if language is None:
            language = "zh"

        # 多语言帮助信息
        help_text = {
            "en": """
            DBManager Usage Instructions:

            1. Initialize Database:
               Initialize the database file and create the table structure. If the database already exists, you will be prompted to confirm.
               Method:
                   db_manager.initialize_database()

            2. Add New Patient Record:
               Add a new patient record to the database, including patient ID and NII file path. Records include creation and update timestamps.
               Method:
                   db_manager.add_new_patient(patient_id, nii_path)
               Parameters:
                   - patient_id: Unique patient identifier (string)
                   - nii_path: Path to the NII file (string)

            3. Update Patient Record:
               Update the NII file path for a patient by ID, with a modification timestamp.
               Method:
                   db_manager.update_patient_record(patient_id, new_nii_path)
               Parameters:
                   - patient_id: Unique patient identifier (string)
                   - new_nii_path: New NII file path (string)

            4. Scan Folder to Initialize Database:
               Scan the specified folder to initialize the database by adding all patient records from the folder.
               Existing records will be skipped.
               Method:
                   db_manager.scan_and_initialize()

            5. Scan Folder to Add Missing Patients:
               Scan the specified folder to add missing patient records to the database.
               Method:
                   db_manager.scan_and_add_missing_patients()

            Example:
                db_manager = DBManager('patients.db', 'shant')
                
                # Initialize Database
                db_manager.initialize_database()
                
                # Add New Patient Record
                db_manager.add_new_patient('patient_001', '/path/to/nii_file.nii')
                
                # Update Patient Record
                db_manager.update_patient_record('patient_001', '/new/path/to/nii_file.nii')
                
                # Scan Folder to Initialize Database
                db_manager.scan_and_initialize()
                
                # Scan Folder to Add Missing Patients
                db_manager.scan_and_add_missing_patients()

            Note:
            - Ensure that `full_dataset_path` points to a valid patient data folder.
            - The folder structure should consist of one folder per patient, with the folder name as the patient ID, containing `.nii` files.
            """,

            "zh": """
            DBManager 使用说明:

            1. 初始化数据库:
               初始化数据库文件并创建表结构。如果数据库已存在，会提示是否确认操作。
               方法:
                   db_manager.initialize_database()

            2. 添加新患者记录:
               向数据库添加一条新的患者记录，包括患者 ID 和 NII 文件路径。记录会包含创建和更新时间戳。
               方法:
                   db_manager.add_new_patient(patient_id, nii_path)
               参数:
                   - patient_id: 患者唯一标识符 (字符串)
                   - nii_path: NII 文件路径 (字符串)

            3. 更新患者记录:
               根据患者 ID 更新对应的 NII 文件路径，并记录修改时间。
               方法:
                   db_manager.update_patient_record(patient_id, new_nii_path)
               参数:
                   - patient_id: 患者唯一标识符 (字符串)
                   - new_nii_path: 新的 NII 文件路径 (字符串)

            4. 扫描文件夹初始化数据库:
               从指定的文件夹扫描患者数据并初始化数据库，添加文件夹中所有患者数据记录。
               如果患者记录已存在，将会跳过。
               方法:
                   db_manager.scan_and_initialize()

            5. 扫描文件夹添加缺失患者:
               从指定的文件夹扫描患者数据，添加数据库中缺失的患者记录。
               方法:
                   db_manager.scan_and_add_missing_patients()

            示例:
                db_manager = DBManager('patients.db', 'shant')
                
                # 初始化数据库
                db_manager.initialize_database()
                
                # 添加新患者记录
                db_manager.add_new_patient('patient_001', '/path/to/nii_file.nii')
                
                # 更新患者记录
                db_manager.update_patient_record('patient_001', '/new/path/to/nii_file.nii')
                
                # 扫描文件夹初始化数据库
                db_manager.scan_and_initialize()
                
                # 扫描文件夹添加缺失患者
                db_manager.scan_and_add_missing_patients()
            
            注意:
            - 确保 `full_dataset_path` 指向有效的患者数据文件夹。
            - 文件夹结构应为每个患者一个文件夹，文件夹名作为患者 ID，内部包含 `.nii` 文件。
            """
        }

        # 打印指定语言的帮助信息
        print(help_text.get(language, help_text["en"]))


    def connect_db(self):
        return sqlite3.connect(self.db_path)

    def initialize_database(self):
        if os.path.exists(self.db_path):
            while True:
                response = input(f"数据库 '{self.db_path}' 已存在。是否重新初始化？(y/n): ").strip().lower()
                if response == 'y':
                    # 重命名旧数据库文件
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    backup_path = f"{self.db_path}.{timestamp}.old"
                    try:
                        os.rename(self.db_path, backup_path)
                        print(f"原有数据库已重命名为 '{backup_path}'，正在重新初始化...")
                    except Exception as e:
                        print(f"重命名旧数据库失败: {e}")
                        return
                    break
                elif response == 'n':
                    print("初始化已取消。")
                    return
                else:
                    print("无效输入，请输入 'y' 或 'n'。")

        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            # 创建表结构
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patient_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT,
                    file_path TEXT UNIQUE,
                    file_type TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            conn.commit()
            print("数据库表 'patient_data' 已创建或确认存在。")
        except Exception as e:
            print(f"初始化数据库时发生错误: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    def add_new_patient(self, patient_id, file_path, file_type):
        conn = self.connect_db()
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        
        try:
            cursor.execute('''
                INSERT INTO patient_data (patient_id, file_path, file_type, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (patient_id, file_path, file_type, timestamp, timestamp))
            conn.commit()
            print(f"成功添加患者记录: {patient_id}，文件类型: {file_type}")
        except sqlite3.IntegrityError:
            print(f"患者 ID '{patient_id}' 已存在，添加失败。")
        finally:
            conn.close()

    def scan_and_add_patients(self):
        conn = self.connect_db()
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()

        # 获取所有患者文件夹
        patient_folders = [
            os.path.join(self.full_dataset_path, folder)
            for folder in os.listdir(self.full_dataset_path)
            if os.path.isdir(os.path.join(self.full_dataset_path, folder))
        ]

        added_files = 0
        tasks = []

        # 多线程扫描和添加记录
        def process_patient_folder(patient_path):
            nonlocal added_files
            patient_id = os.path.basename(patient_path)
            local_added = 0

            with self.connect_db() as local_conn:
                local_cursor = local_conn.cursor()
                for file in os.listdir(patient_path):
                    file_path = os.path.join(patient_path, file)
                    if os.path.isfile(file_path):
                        file_type = os.path.splitext(file)[-1].lower()
                        local_cursor.execute('SELECT 1 FROM patient_data WHERE file_path = ?', (file_path,))
                        if not local_cursor.fetchone():
                            try:
                                local_cursor.execute('''
                                    INSERT INTO patient_data (patient_id, file_path, file_type, created_at, updated_at)
                                    VALUES (?, ?, ?, ?, ?)
                                ''', (patient_id, file_path, file_type, timestamp, timestamp))
                                local_added += 1
                            except sqlite3.IntegrityError:
                                pass
                local_conn.commit()

            added_files += local_added
            return local_added

        with ThreadPoolExecutor(max_workers=16) as executor:
            for folder in patient_folders:
                tasks.append(executor.submit(process_patient_folder, folder))

            with tqdm(total=len(patient_folders), desc="扫描文件夹并添加患者", unit="folder") as pbar:
                for future in as_completed(tasks):
                    future.result()  # 等待线程完成
                    pbar.update(1)

        conn.close()
        print(f"扫描完成，共添加了 {added_files} 条记录。")

    def scan_and_add_missing_patients(self):
        conn = self.connect_db()
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()

        # 获取所有患者文件夹
        patient_folders = [
            os.path.join(self.full_dataset_path, folder)
            for folder in os.listdir(self.full_dataset_path)
            if os.path.isdir(os.path.join(self.full_dataset_path, folder))
        ]

        checked_files = 0
        added_files = 0
        tasks = []

        # 多线程扫描和处理患者文件夹
        def process_patient_folder(patient_path):
            nonlocal added_files, checked_files
            patient_id = os.path.basename(patient_path)
            local_checked = 0
            local_added = 0

            with self.connect_db() as local_conn:
                local_cursor = local_conn.cursor()
                for file in os.listdir(patient_path):
                    file_path = os.path.join(patient_path, file)
                    if os.path.isfile(file_path):
                        file_type = os.path.splitext(file)[-1].lower()
                        local_cursor.execute('SELECT 1 FROM patient_data WHERE file_path = ?', (file_path,))
                        local_checked += 1
                        if not local_cursor.fetchone():
                            try:
                                local_cursor.execute('''
                                    INSERT INTO patient_data (patient_id, file_path, file_type, created_at, updated_at)
                                    VALUES (?, ?, ?, ?, ?)
                                ''', (patient_id, file_path, file_type, timestamp, timestamp))
                                local_added += 1
                            except sqlite3.IntegrityError:
                                pass  # 忽略重复文件
                local_conn.commit()

            added_files += local_added
            checked_files += local_checked
            return local_checked, local_added

        # 使用线程池处理文件夹
        with ThreadPoolExecutor(max_workers=16) as executor:
            for folder in patient_folders:
                tasks.append(executor.submit(process_patient_folder, folder))

            with tqdm(total=len(patient_folders), desc="扫描文件夹", unit="folder") as pbar:
                for future in as_completed(tasks):
                    local_checked, local_added = future.result()
                    pbar.set_postfix(checked_files=checked_files, added_files=added_files)
                    pbar.update(1)

        conn.close()
        print(f"扫描完成，共检查了 {checked_files} 个文件，新增了 {added_files} 条记录。")