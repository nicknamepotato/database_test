import psycopg2
import time
from multiprocessing import Pool, Process, Event, Value
from CRUD import DBConn, OrderTransactionalWorker
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
    FAULT_INJECTION_ITERATION,
    FIRST_INJECTION_TIME, 
    SECOND_INJECTION_TIME
)

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

def monitor_and_inject_fault_by_transaction_count(transaction_counter, target_transactions, pg_data_dir):
    print(f"故障监控已启动，等待 {target_transactions} 个事务...")
    while True:
        with transaction_counter.get_lock():
            current_transactions = transaction_counter.value

        if current_transactions >= target_transactions:
            inject_fault(pg_data_dir)
            break
        time.sleep(0.5)
    print("监控结束。")

def monitor_and_inject_fault_by_time(delay_time, pg_data_dir):
    print(f"故障监控已启动，等待 {delay_time} 秒...")
    start_time = time.time()
    while time.time() - start_time < delay_time:
        time.sleep(0.5)
    inject_fault(pg_data_dir)
    print(f"时间到，故障注入完成（{delay_time} 秒后）。")


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

def run_fault_mode(db_config, pg_data_dir, fault_mode, k_workers, num_transactions_per_worker,
                   first_injection_transactions, second_injection_transactions, transaction_ratios):

    DB_HOST = db_config['host']
    DB_NAME = db_config['database']
    DB_USER = db_config['user']
    DB_PASSWORD = db_config['password']
    DB_PORT = db_config['port']

    total_experiment_start_time = time.time() # 记录整个实验的开始时间

    transaction_counter = Value('i', 0)
    stop_logging_event = Event()

    log_process = Process(target=log_database_counts, args=(DB_CONFIG, stop_logging_event))
    log_process.start()

    multi_ops_instance = MultiTableOperations()

    # Initialize recovery_time to None, will be updated in each fault mode branch
    recovery_time = None
    recovery_time_1 = None
    recovery_time_2 = None

    if fault_mode == 'single_injection':
        print(f"当前运行在单次故障注入模式。将在执行 {first_injection_transactions} 个事务后注入故障。")

        monitor_process = Process(target=monitor_and_inject_fault_by_transaction_count,
                                  args=(transaction_counter, first_injection_transactions, pg_data_dir))
        monitor_process.start()

        worker_func_with_args = partial(worker_function,
                                        num_transactions=num_transactions_per_worker,
                                        transaction_ratios=transaction_ratios,
                                        transaction_counter=transaction_counter,
                                        multi_ops_instance=multi_ops_instance,
                                        pg_data_dir=pg_data_dir)

        pool = Pool(k_workers)
        async_results = [pool.apply_async(worker_func_with_args, (i,)) for i in range(k_workers)]

        monitor_process.join()
        print("故障注入完成。")

        stop_logging_event.set()
        log_process.join()

        recovery_start_time = time.time()
        print("数据库已关闭，等待数据库恢复...")
        if not start_database(pg_data_dir):
            print("未能自动启动数据库，请手动启动并重试。")
            sys.exit(1)
        if check_database_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT):
            recovery_end_time = time.time()
            recovery_time = recovery_end_time - recovery_start_time
            print(f"数据库已恢复并可连接，恢复时间: {recovery_time:.2f} 秒。")
            print("验证第一次恢复后的数据一致性...")
            try:
                with DBConn() as conn:
                    conn.cursor.execute("SELECT COUNT(*) FROM \"User\";")
                    post_recovery_user_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的User数量: {post_recovery_user_count}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"Order\";")
                    post_recovery_order_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的Order数量: {post_recovery_order_count}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"OrderItem\";")
                    post_recovery_order_item_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的OrderItem数量: {post_recovery_order_item_count}.")
            except Exception as e:
                print(f"数据一致性验证失败: {e}")
        else:
            print("数据库在第一次注入后未能恢复并连接。")

        pool.close()
        pool.join()
        print("\n所有并发事务已完成,单次注入模式。")

    elif fault_mode == 'two_phase_injection':
        print(f"当前运行在双阶段故障注入模式。")
        print(f"阶段 1: 故障将在执行 {FIRST_INJECTION_TIME} 秒后注入。")

        # First phase workers
        worker_func_with_args_1 = partial(worker_function,
                                        num_transactions=num_transactions_per_worker, # Workers will run for a set number of transactions
                                        transaction_ratios=transaction_ratios,
                                        transaction_counter=transaction_counter,
                                        multi_ops_instance=multi_ops_instance,
                                        pg_data_dir=pg_data_dir)

        pool_1 = Pool(k_workers)
        async_results_1 = [pool_1.apply_async(worker_func_with_args_1, (i,)) for i in range(k_workers)]

        # Monitor for first fault injection based on time
        monitor_process_1 = Process(target=monitor_and_inject_fault_by_time,
                                    args=(FIRST_INJECTION_TIME, pg_data_dir))
        monitor_process_1.start()
        monitor_process_1.join()
        print("阶段1:故障注入完成。")

        # Stop workers after first injection (optional, depends on desired behavior)
        pool_1.close()
        pool_1.join()


        recovery_start_time_1 = time.time()
        if not start_database(pg_data_dir):
            print("阶段 1: 未能自动启动数据库，请手动启动并重试。")
            sys.exit(1)
        if check_database_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT):
            recovery_end_time_1 = time.time()
            recovery_time_1 = recovery_end_time_1 - recovery_start_time_1
            print(f"阶段 1: 数据库已恢复并可连接，恢复时间: {recovery_time_1:.2f} 秒。")
        else:
            print("阶段 1: 数据库在第一次注入后未能恢复并连接。")
            sys.exit(1)


        print(f"\n阶段 2: 开始新的事务。故障将在执行 {SECOND_INJECTION_TIME} 秒后注入。")

        # Second phase workers (reset transaction_counter for logging, though injection is time-based)
        transaction_counter.value = 0 # Reset counter for the second phase's transaction logging
        num_transactions_per_worker_phase2 = num_transactions_per_worker # Or adjust if needed for phase 2
        worker_func_with_args_2 = partial(worker_function,
                                        num_transactions=num_transactions_per_worker_phase2,
                                        transaction_ratios=transaction_ratios,
                                        transaction_counter=transaction_counter,
                                        multi_ops_instance=multi_ops_instance,
                                        pg_data_dir=pg_data_dir)

        pool_2 = Pool(k_workers)
        async_results_2 = [pool_2.apply_async(worker_func_with_args_2, (i,)) for i in range(k_workers)]

        # Monitor for second fault injection based on time
        monitor_process_2 = Process(target=monitor_and_inject_fault_by_time,
                                    args=(SECOND_INJECTION_TIME, pg_data_dir))
        monitor_process_2.start()
        monitor_process_2.join()
        print("阶段 2: 第二次故障注入完成。")

        # Stop workers after second injection
        pool_2.close()
        pool_2.join()

        recovery_start_time_2 = time.time()
        if not start_database(pg_data_dir):
            print("阶段 2: 未能自动启动数据库，请手动启动并重试。")
            sys.exit(1)
        if check_database_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT):
            recovery_end_time_2 = time.time()
            recovery_time_2 = recovery_end_time_2 - recovery_start_time_2
            print(f"阶段 2: 数据库已恢复并可连接，恢复时间: {recovery_time_2:.2f} 秒。")
            print("阶段 2: 验证第二次恢复后的数据一致性...")
            try:
                with DBConn() as conn:
                    conn.cursor.execute("SELECT COUNT(*) FROM \"User\";")
                    post_recovery_user_count_2 = conn.cursor.fetchone()[0]
                    print(f"阶段 2: 恢复后的 User计数: {post_recovery_user_count_2}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"Order\";")
                    post_recovery_order_count_2 = conn.cursor.fetchone()[0]
                    print(f"阶段 2: 恢复后的 Order 计数: {post_recovery_order_count_2}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"OrderItem\";")
                    post_recovery_order_item_count_2 = conn.cursor.fetchone()[0]
                    print(f"阶段 2: 恢复后的 OrderItem 计数: {post_recovery_order_item_count_2}.")
            except Exception as e:
                print(f"阶段 2: 数据一致性验证失败: {e}")
        else:
            print("阶段 2: 数据库未能恢复并连接。")
            sys.exit(1)

        print("\n所有并发事务已完成,适用于两阶段注入模式。")

    elif fault_mode == 'none':
        print(f"当前运行在无故障注入模式。所有事务将正常执行，不进行故障注入或数据库恢复。")

        worker_func_with_args = partial(worker_function,
                                        num_transactions=num_transactions_per_worker,
                                        transaction_ratios=transaction_ratios,
                                        transaction_counter=transaction_counter,
                                        multi_ops_instance=multi_ops_instance,
                                        pg_data_dir=pg_data_dir)

        pool = Pool(k_workers)
        async_results = [pool.apply_async(worker_func_with_args, (i,)) for i in range(k_workers)]

        pool.close()
        pool.join()
        print("所有工作器进程已完成。")
        recovery_time = 0.0 

    elif fault_mode == 'long_fault':
        print(f"运行在长事务故障注入模式。")

        worker_func_with_args = partial(worker_function,
                                        num_transactions=1,
                                        transaction_ratios={},
                                        transaction_counter=transaction_counter,
                                        multi_ops_instance=multi_ops_instance,
                                        pg_data_dir=pg_data_dir,
                                        fault_mode_specific_injection=True,
                                        fault_point='long_transaction_fault')

        pool = Pool(k_workers)
        async_results = [pool.apply_async(worker_func_with_args, (i,)) if i == 0 else
                         pool.apply_async(worker_function, (i, num_transactions_per_worker, transaction_ratios, transaction_counter, multi_ops_instance, pg_data_dir, False, None))
                         for i in range(k_workers)]

        pool.close()
        pool.join()
        print("所有工作器进程已完成或退出。")

        print("尝试在长事务故障注入后重新启动数据库...")
        recovery_start_time = time.time()
        if not start_database(pg_data_dir):
            print("未能自动启动数据库，请手动启动并重试。")
            sys.exit(1)
        if check_database_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT):
            recovery_end_time = time.time()
            recovery_time = recovery_end_time - recovery_start_time
            print(f"数据库已恢复并可连接，恢复时间: {recovery_time:.2f} 秒。")
            print("验证长事务故障注入后的数据一致性...")
            try:
                with DBConn() as conn:
                    conn.cursor.execute("SELECT COUNT(*) FROM \"User\";")
                    post_recovery_user_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的 User 计数: {post_recovery_user_count}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"Order\";")
                    post_recovery_order_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的 Order 计数: {post_recovery_order_count}.")
                    conn.cursor.execute("SELECT COUNT(*) FROM \"OrderItem\";")
                    post_recovery_order_item_count = conn.cursor.fetchone()[0]
                    print(f"恢复后的 OrderItem 计数: {post_recovery_order_item_count}.")

                    conn.cursor.execute("SELECT id, price FROM \"Product\" ORDER BY id LIMIT 1;")
                    affected_product = conn.cursor.fetchone()
                    if affected_product:
                        product_id = affected_product[0]
                        price_after_recovery = affected_product[1]
                        print(f"恢复后产品 ID {product_id} 的价格: {price_after_recovery}.")
                    else:
                        print("未能获取任何产品价格进行验证。")

            except Exception as e:
                print(f"数据一致性验证失败: {e}")
        else:
            print("数据库未能恢复并连接。")
            sys.exit(1)

    stop_logging_event.set()
    log_process.join()

    total_experiment_end_time = time.time() # 记录整个实验的结束时间
    total_task_time = total_experiment_end_time - total_experiment_start_time

    # Print recovery times based on fault mode
    if fault_mode == 'two_phase_injection':
        if recovery_time_1 is not None:
            print(f"\n数据库第一次恢复服务时间: {recovery_time_1:.2f} 秒。")
        if recovery_time_2 is not None:
            print(f"数据库第二次恢复服务时间: {recovery_time_2:.2f} 秒。")
    elif recovery_time is not None:
        print(f"\n数据库恢复服务时间: {recovery_time:.2f} 秒。")
    else:
        print("\n未记录到数据库恢复服务时间（可能没有发生故障注入或恢复）。")

    print(f"数据库完成所有任务的总时间: {total_task_time:.2f} 秒。")
    print("实验结束。")