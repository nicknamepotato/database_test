import psycopg2
import time
from multiprocessing import Process, Event, Value
from CRUD import DBConn
from create_table import create_tables, insert_test_data
from functools import partial
import sys
import os
import random
import datetime

from fault_injector import inject_fault
from connection import DB_CONFIG
from multiple import MultiTableOperations

def log_database_counts(db_config, stop_event):
    host = db_config['host']
    database = db_config['database']
    user = db_config['user']
    password = db_config['password']
    port = db_config['port']

    while not stop_event.is_set():
        try:
            with DBConn() as conn:
                cur = conn.cursor

                cur.execute("SELECT COUNT(*) FROM \"User\";")
                user_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM \"Order\";")
                order_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM \"OrderItem\";")
                order_item_count = cur.fetchone()[0]

                print(f"[{time.strftime('%H:%M:%S')}] Users: {user_count}, Orders: {order_count}, OrderItems: {order_item_count}")
            time.sleep(2)
        except psycopg2.OperationalError as e:
            print(f"[{time.strftime('%H:%M:%S')}] 数据库连接丢失或失效 {e}")
            time.sleep(20)
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] 错误发生: {e}")
            time.sleep(5)
    print("实时数据库计数日志记录停止。")

def start_database(pg_data_dir):
    try:
        import subprocess
        command = ["su", "-", "postgres", "-c", f"pg_ctl start -D {pg_data_dir}"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"Database started successfully: {result.stdout}")
        if result.stderr:
            print(f"stderr: {result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to start database: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: 'su' or 'pg_ctl' command not found. Please ensure they are in the system's PATH.")
        return False

def check_database_connection(host, database, user, password, port, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=20
            )
            conn.close()
            print("数据库连接成功！")
            return True
        except psycopg2.OperationalError:
            print("等待数据库启动...", end='\r')
            time.sleep(1)
        except Exception as e:
            print(f"检查数据库连接时发生意外错误: {e}")
            time.sleep(1)
    print("数据库连接超时。")
    return False

def get_product_price(product_id):
    try:
        with DBConn() as conn:
            conn.cursor.execute("SELECT price FROM \"Product\" WHERE id = %s;", (product_id,))
            price = conn.cursor.fetchone()
            return float(price[0]) if price else None
    except psycopg2.Error as e:
        print(f"获取产品价格失败: {e}")
        return None

def find_an_existing_product_id():
    try:
        with DBConn() as conn:
            conn.cursor.execute("SELECT id FROM \"Product\" ORDER BY RANDOM() LIMIT 1;")
            product_id = conn.cursor.fetchone()
            return product_id[0] if product_id else None
    except psycopg2.Error as e:
        print(f"查找现有产品ID失败: {e}")
        return None

def custom_price_update_worker(worker_id, product_id, price_change_value, multi_ops_instance):
    try:
        print(f"工作器 {worker_id}: 尝试更新产品 {product_id} 的价格...")
        current_price = get_product_price(product_id)
        if current_price is None:
            print(f"工作器 {worker_id}: 无法获取产品 {product_id} 的当前价格，跳过更新。")
            return False

        new_price = round(current_price + price_change_value, 2)
        print(f"工作器 {worker_id}: 尝试将产品 {product_id} 的价格从 {current_price:.2f} 更新为 {new_price:.2f}")

        success = multi_ops_instance.modify_product_price_and_update_orders(product_id=product_id, new_price=new_price)
        if success:
            print(f"工作器 {worker_id}: 成功更新产品 {product_id} 的价格到 {new_price:.2f}")
        else:
            print(f"工作器 {worker_id}: 更新产品 {product_id} 的价格失败。")
        return success
    except Exception as e:
        print(f"工作器 {worker_id} 遇到错误: {e}")
        return False

def run_two_worker_dependent_transaction_fault_test():
    DB_HOST = DB_CONFIG['host']
    DB_NAME = DB_CONFIG['database']
    DB_USER = DB_CONFIG['user']
    DB_PASSWORD = DB_CONFIG['password']
    DB_PORT = DB_CONFIG['port']
    PG_DATA_DIR = "/data/pgsql" 

    try:
        create_tables() 

        with DBConn() as conn:
            conn.cursor.execute("SELECT COUNT(*) FROM \"User\";")
            user_count = conn.cursor.fetchone()[0]
            if user_count == 0:
                try:
                    insert_test_data(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
                    print("初始测试数据插入完成。")
                except Exception as e:
                    print(f"自动测试数据插入失败: {e}")
                    sys.exit(1)
            else:
                print(f"数据库中现有用户数据计数: {user_count}.")

        stop_logging_event = Event()
        log_process = Process(target=log_database_counts, args=(DB_CONFIG, stop_logging_event))
        log_process.start()

        product_id_to_test = find_an_existing_product_id()
        if not product_id_to_test:
            print("没有可用的产品ID进行测试,请确保有测试数据。")
            stop_logging_event.set()
            log_process.join()
            sys.exit(1)

        initial_price = get_product_price(product_id_to_test)
        print(f"\n1. 商品 {product_id_to_test} 的最初价格: {initial_price:.2f}")

        multi_ops_instance = MultiTableOperations() 

        print(f"\n2. 工作器 0 (事务 A): 修改产品 {product_id_to_test} 价格 (+10)")
        worker0_process = Process(target=custom_price_update_worker,
                                  args=(0, product_id_to_test, 10.00, multi_ops_instance))
        worker0_process.start()
        worker0_process.join() 

        price_after_worker0 = get_product_price(product_id_to_test)
        print(f"3. 工作器 0 执行后产品 {product_id_to_test} 的价格: {price_after_worker0:.2f}")

        print("\n4. 注入故障 (关闭数据库)")
        inject_fault(PG_DATA_DIR)

        stop_logging_event.set()
        log_process.join()

        print("\n5. 恢复数据库")
        recovery_start_time = time.time()
        if not start_database(PG_DATA_DIR):
            print("未能自动启动数据库,请手动启动并重试。")
            sys.exit(1)
        if check_database_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT):
            recovery_end_time = time.time()
            recovery_time = recovery_end_time - recovery_start_time
            print(f"数据库已恢复并可连接,恢复时间: {recovery_time:.2f} 秒。")
        else:
            print("数据库在故障注入后未能恢复并连接.")
            sys.exit(1)

        multi_ops_instance = MultiTableOperations()

        print(f"\n6. 工作器 1 (事务 B): 依赖事务 A,修改产品 {product_id_to_test} 价格 (+5)")
        worker1_process = Process(target=custom_price_update_worker,
                                  args=(1, product_id_to_test, 5.00, multi_ops_instance))
        worker1_process.start()
        worker1_process.join() 

        final_price = get_product_price(product_id_to_test)
        print(f"\n7. 商品 {product_id_to_test} 的最终价格:{final_price:.2f} ---")

        expected_price = initial_price + 10.00 + 5.00
        print(f"8. 预期最终价格: {expected_price:.2f}")

        if abs(final_price - expected_price) < 0.01:
            print("9. 验证结果: 商品价格一致性通过.")
        else:
            print("10. 验证结果: 商品价格一致性不通过!")

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        if 'stop_logging_event' in locals() and log_process.is_alive():
            stop_logging_event.set()
            log_process.join()
    finally:
        print("\n实验结束。")
