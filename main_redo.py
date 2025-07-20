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
    FAULT_INJECTION_ITERATION,
    FIRST_INJECTION_TIME,
    SECOND_INJECTION_TIME
)

# Import the new function from fault_mode.py
from fault_mode import run_fault_mode

def run_redo_test():
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

if __name__ == '__main__':
    print("正在执行 Redo 测试...")
    run_redo_test()