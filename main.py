# -*- coding: utf-8 -*-

import psycopg2
import time
from multiprocessing import Pool, Process, Event, Value
from CRUD import DBConn, OrderTransactionalWorker
from create_table import create_tables, insert_test_data
from functools import partial
import sys
import os
import random
import datetime 

from fault_injector import inject_fault
from connection import DB_CONFIG
from multiple import MultiTableOperations
from long import long_running_price_update
from settings import (
    K_WORKERS,
    NUM_TRANSACTIONS_PER_WORKER,
    FIRST_INJECTION_TRANSACTIONS,
    SECOND_INJECTION_TRANSACTIONS,
    FAULT_MODE,
    TRANSACTION_RATIOS,
    LONG_TRANSACTION_ITERATIONS,
    FAULT_INJECTION_ITERATION
)

# Import the new function from fault_mode.py
from fault_mode import run_fault_mode


def worker_function(worker_id, num_transactions, transaction_ratios, transaction_counter, multi_ops_instance, pg_data_dir, fault_mode_specific_injection=False, fault_point=None):
    try:
        with DBConn() as conn:
            worker = OrderTransactionalWorker(worker_id, conn, transaction_ratios=transaction_ratios)
            results = []

            if fault_mode_specific_injection and worker_id == 0:
                print(f"工作器 {worker_id}：正在执行带故障注入的特定多表操作") 
                if fault_point == 'delete_product_after_step2':
                    multi_ops_instance.delete_product_and_related_order_items(
                        inject_fault_at_point='after_step2',
                        pg_data_dir=pg_data_dir
                    )
                elif fault_point == 'modify_price_after_order_item_update':
                    multi_ops_instance.modify_product_price_and_update_orders(
                        inject_fault_at_point='after_order_item_update',
                        pg_data_dir=pg_data_dir
                    )
                elif fault_point == 'long_transaction_fault':
                    try:
                        with DBConn() as temp_conn:
                            temp_conn.cursor.execute("SELECT id FROM \"Product\" ORDER BY RANDOM() LIMIT 1;")
                            product_id_for_long_trx = temp_conn.cursor.fetchone()
                            if product_id_for_long_trx:
                                product_id_for_long_trx = product_id_for_long_trx[0]
                                print(f"工作器 {worker_id}: 选定产品 ID {product_id_for_long_trx} 进行长事务。") 
                                long_running_price_update(product_id_for_long_trx, pg_data_dir)
                            else:
                                print(f"工作器 {worker_id}: 数据库中没有产品可用于长事务。") 
                    except Exception as e:
                        print(f"工作器 {worker_id}: 获取产品 ID 失败: {e}") 


                results.append(f"特定多表故障注入在 {fault_point}") 
                return results

            for i in range(num_transactions):
                all_transaction_types = list(transaction_ratios.keys())
                all_transaction_weights = list(transaction_ratios.values())

                trx_type = random.choices(all_transaction_types, weights=all_transaction_weights, k=1)[0]

                if trx_type == 'insert':
                    result = worker.insert_order()
                elif trx_type == 'select':
                    result = worker.select_order()
                elif trx_type == 'update':
                    result = worker.update_order()
                elif trx_type == 'delete':
                    result = worker.delete_order()
                elif trx_type == 'delete_product_multi_table':
                    result = multi_ops_instance.delete_product_and_related_order_items()
                elif trx_type == 'modify_product_price_multi_table':
                    result = multi_ops_instance.modify_product_price_and_update_orders()
                else:
                    result = None

                results.append(result)
                with transaction_counter.get_lock():
                    transaction_counter.value += 1
                time.sleep(0.01)
            print(f"工作器 {worker_id} 完成.") 
            return results
    except Exception as e:
        print(f"工作器 {worker_id} 遇到错误: {e}") 
        return []

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

def monitor_and_inject_fault(transaction_counter, target_transactions, pg_data_dir):
    print(f"故障监控已启动，等待 {target_transactions} 个事务...")
    while True:
        with transaction_counter.get_lock():
            current_transactions = transaction_counter.value

        if current_transactions >= target_transactions:
            inject_fault(pg_data_dir)
            break
        time.sleep(0.5)
    print("监控结束。") 


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

if __name__ == '__main__':
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

    except Exception as e:
        print(f"未能完成数据库的初始化: {e}") 
        sys.exit(1)

    k = K_WORKERS
    num_transactions_per_worker = NUM_TRANSACTIONS_PER_WORKER
    total_transactions_to_execute = k * num_transactions_per_worker

    
    fault_mode = FAULT_MODE 

    first_injection_transactions = FIRST_INJECTION_TRANSACTIONS
    second_injection_transactions = SECOND_INJECTION_TRANSACTIONS

    transaction_ratios = TRANSACTION_RATIOS

    total_percentage = sum(transaction_ratios.values())
    if total_percentage != 100:
        print(f"事务比例之和为 {total_percentage}%,建议设置为100%以保证比例准确") 

    print(f"事务比例配置: {transaction_ratios}") 
    print(f"启动 {k} 个并发工作器，每个执行 {num_transactions_per_worker} 个事务，总计 {total_transactions_to_execute} 个事务。") 
    
    run_fault_mode(DB_CONFIG, PG_DATA_DIR, fault_mode, k, num_transactions_per_worker,
                   first_injection_transactions, second_injection_transactions, transaction_ratios)