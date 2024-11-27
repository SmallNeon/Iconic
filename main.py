from dbmgr import DBManager

def main():
    # 文件位置配置
    db_path = 'patient_data.db'
    full_dataset_path = '/media/molloi-lab-linux2/HD-88/ICONIC CCTAS'

    # 初始化 DBManager 实例
    db = DBManager(db_path, full_dataset_path)

    try:
        print("1. 初始化数据库")
        print("2. 扫描并初始化数据库")
        print("3. 扫描并添加缺失患者记录")
        print("0. 退出")

        while True:
            choice = input("请选择操作（输入对应数字）：").strip()
            if choice == '1':
                db.initialize_database()
            elif choice == '2':
                db.scan_and_add_patients()
            elif choice == '3':
                db.scan_and_add_missing_patients()
            elif choice == '0':
                print("退出程序")
                break
            else:
                print("无效的选择，请重新输入！")

    except Exception as e:
        print(f"运行时发生错误：{e}")
    finally:
        print("程序已结束。")



if __name__ == "__main__":
    main()
