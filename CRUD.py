import psycopg2
import time
import random
import math
import datetime
from multiprocessing import Pool, Value, Lock
import os
from connection import DSN

class Random:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.C_255 = self.rng.randint(0, 256)
        self.C_1023 = self.rng.randint(0, 1024)
        self.C_8191 = self.rng.randint(0, 8191)

    def nurand(self, A, x, y):
        if A == 255:
            C = self.C_255
        elif A == 1023:
            C = self.C_1023
        elif A == 8191:
            C = self.C_8191
        else:
            C = 0

        return (((self.rng.randint(0, A + 1) | self.rng.randint(x, y + 1)) + C) % (y - x + 1)) + x

    def decision(self, frac):
        return self.rng.random() < frac

    def randint_inclusive(self, low, high):
        return self.rng.randint(low, high)

    def sample(self):
        return self.rng.random()

    def gaussian(self, mean, variance):
        return self.rng.gauss(mean, variance)

    def shuffle(self, l):
        self.rng.shuffle(l)

    def from_list(self, l, length=1):
        return self.rng.choices(l, k=length)


class DBConn:
    def __init__(self, statement_timeout=0):
        self.conn = None
        self.cursor = None
        self.server_side_cursor = None
        self.statement_timeout = statement_timeout

    def __enter__(self):
        options = f'-c statement_timeout={self.statement_timeout}'
        try:
            self.conn = psycopg2.connect(
                dsn=DSN,
                options=options
            )
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            print(f"数据库连接失败: {e}") 
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if exc_type:
            print(f"事务中发生异常: {exc_val}") 

class OrderTransactionalWorker:
    def __init__(self, worker_id, conn, transaction_ratios=None):
        self.conn = conn
        self.random = Random(worker_id)
        self.worker_id = worker_id
        self.user_ids = []
        self.existing_order_codes = set()
        self._load_initial_data()
        self.lock = Lock()

        if transaction_ratios is None:
            self.transaction_types = ['insert', 'select', 'update', 'delete']
            self.transaction_weights = [1, 1, 1, 1]
        else:
            self.transaction_types = list(transaction_ratios.keys())
            self.transaction_weights = list(transaction_ratios.values())

        if sum(self.transaction_weights) == 0:
            raise ValueError("事务比例的总和不能为零。")


    def _load_initial_data(self):
        try:
            with DBConn() as conn:
                conn.cursor.execute("SELECT id FROM \"User\";")
                self.user_ids = [row[0] for row in conn.cursor.fetchall()]
                if not self.user_ids:
                    conn.cursor.execute("INSERT INTO \"User\" (username) VALUES (%s) RETURNING id;", (f"temp_user_{self.worker_id}",))
                    self.user_ids.append(conn.cursor.fetchone()[0])
                    conn.conn.commit()
                    print(f"为工作器 {self.worker_id} 插入了一个临时用户。")
                conn.cursor.execute("SELECT order_code FROM \"Order\";")
                self.existing_order_codes = {row[0] for row in conn.cursor.fetchall()}
        except psycopg2.Error as e:
            print(f"加载初始数据失败: {e}")
            self.user_ids = [1]
            self.existing_order_codes = set()


    def execute_sql(self, sql, args=None, explain=False, analyze=False):
        try:
            if not explain:
                self.conn.cursor.execute(sql, args)
                return None

            if analyze:
                sql_prefix = "EXPLAIN (ANALYZE, FORMAT JSON) "
            else:
                sql_prefix = "EXPLAIN (FORMAT JSON) "

            self.conn.cursor.execute(sql_prefix + sql, args)
            return self.conn.cursor.fetchone()[0][0]
        except psycopg2.Error as e:
            print(f"工作器 {self.worker_id} 执行 SQL 失败: {e} SQL: {sql} Args: {args}")
            return None

    def insert_order(self, explain=False, analyze=False):
        if not self.user_ids:
            print(f"工作器 {self.worker_id}: 无法插入订单，用户 ID 不可用。")
            return None

        user_id = self.random.choice(self.user_ids)

        order_code = None
        max_attempts = 100
        for _ in range(max_attempts):
            potential_order_code = self.random.randint_inclusive(100000000000, 999999999999)
            with self.lock:
                if potential_order_code not in self.existing_order_codes:
                    order_code = potential_order_code
                    self.existing_order_codes.add(order_code)
                    break
        if order_code is None:
            print(f"工作器 {self.worker_id}: 生成唯一订单号失败，跳过插入。") 
            return None

        payment = round(self.random.uniform(10.00, 5000.00), 2)
        create_time = datetime.datetime(2025, random.randint(1, 12), random.randint(1, 28),
                                        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        update_time = create_time + datetime.timedelta(minutes=self.random.randint_inclusive(0, 60)) 

        sql = """
            INSERT INTO "Order" (order_code, user_id, payment, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s) RETURNING id;
        """
        args = (order_code, user_id, payment, create_time, update_time)
        result = self.execute_sql(sql, args, explain, analyze)
        if not explain and result is not None:
            print(f"工作器 {self.worker_id}: 订单插入成功，订单 ID: {result[0]}, 订单号: {order_code}") 
        elif explain and result is not None:
            print(f"工作器 {self.worker_id}: 插入订单的 EXPLAIN 计划: {result}")
        return result

    def select_order(self, explain=False, analyze=False):
        if not self.existing_order_codes:
            print(f"工作器 {self.worker_id}: 无法查询订单，当前没有订单号。")
            return None

        order_code_to_select = self.random.choice(list(self.existing_order_codes))

        sql = """
            SELECT id, order_code, user_id, payment, create_time, update_time
            FROM "Order"
            WHERE order_code = %s;
        """
        args = (order_code_to_select,)
        result = self.execute_sql(sql, args, explain, analyze)
        if not explain and result is None:
            fetched_row = self.conn.cursor.fetchone()
            if fetched_row:
                print(f"工作器 {self.worker_id}: 订单查询成功，订单号 {order_code_to_select}, 数据: {fetched_row}")
            else:
                print(f"工作器 {self.worker_id}: 订单号 {order_code_to_select} 未找到。")
        elif explain and result is not None:
            print(f"工作器 {self.worker_id}: 查询订单的 EXPLAIN 计划: {result}")
        return result

    def update_order(self, explain=False, analyze=False):
        if not self.existing_order_codes:
            print(f"工作器 {self.worker_id}: 无法更新订单，当前没有订单号。") 
            return None

        order_code_to_update = self.random.choice(list(self.existing_order_codes))
        new_payment = round(self.random.uniform(50.00, 10000.00), 2)
        current_update_time = datetime.datetime.now() 

        sql = """
            UPDATE "Order"
            SET payment = %s, update_time = %s
            WHERE order_code = %s;
        """
        args = (new_payment, current_update_time, order_code_to_update)
        result = self.execute_sql(sql, args, explain, analyze)
        if not explain and result is None:
            print(f"工作器 {self.worker_id}: 成功将订单 {order_code_to_update} 的付款更新为 {new_payment}。") 
        elif explain and result is not None:
            print(f"工作器 {self.worker_id}: 更新订单的 EXPLAIN 计划: {result}")
        return result

    def delete_order(self, explain=False, analyze=False):
        if not self.existing_order_codes:
            print(f"工作器 {self.worker_id}: 无法删除订单，当前没有订单号。")
            return None

        order_code_to_delete = self.random.choice(list(self.existing_order_codes))

        sql = """
            DELETE FROM "Order"
            WHERE order_code = %s;
        """
        args = (order_code_to_delete,)
        result = self.execute_sql(sql, args, explain, analyze)
        if not explain and result is None:
            with self.lock:
                self.existing_order_codes.discard(order_code_to_delete)
            print(f"工作器 {self.worker_id}: 成功删除订单 {order_code_to_delete}。")
        elif explain and result is not None:
            print(f"工作器 {self.worker_id}: 删除订单的 EXPLAIN 计划: {result}")
        return result

    def next_transaction(self, explain=False, analyze=False):
        trx_type = self.random.choices(self.transaction_types, weights=self.transaction_weights, k=1)[0]

        if trx_type == 'insert':
            return self.insert_order(explain, analyze)
        elif trx_type == 'select':
            return self.select_order(explain, analyze)
        elif trx_type == 'update':
            return self.update_order(explain, analyze)
        elif trx_type == 'delete':
            return self.delete_order(explain, analyze)
