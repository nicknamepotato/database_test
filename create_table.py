import psycopg2
import random
from datetime import datetime, timedelta
from connection import DB_CONFIG
import itertools
from settings import (
    K_WORKERS, 
    BASE_NUM_USERS,
    BASE_NUM_PRODUCTS,
    BASE_NUM_ORDERS
)

def create_tables():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS \"OrderItem\" CASCADE;")
        cur.execute("DROP TABLE IF EXISTS \"Order\" CASCADE;")
        cur.execute("DROP TABLE IF EXISTS \"Product\" CASCADE;")
        cur.execute("DROP TABLE IF EXISTS \"User\" CASCADE;")
        print("现有表已成功删除（如果存在）。")

        # create User table
        cur.execute("""
            CREATE TABLE "User" (
                id SERIAL PRIMARY KEY, -- 用户表id,自动递增主键
                username VARCHAR(50) NOT NULL UNIQUE -- 用户名，非空且唯一
            );
        """)
        print("表 'User' 创建成功。")

        # create Product table
        cur.execute("""
            CREATE TABLE "Product" (
                id SERIAL PRIMARY KEY, -- 商品id,自动递增主键
                name VARCHAR(255) NOT NULL, -- 商品名称
                description TEXT, -- 商品描述
                price DECIMAL(20, 2) NOT NULL, -- 商品单价, 单位是元, 保留两位小数
                stock INTEGER NOT NULL CHECK (stock >= 0), -- 库存数量，非空且不能为负
                create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 创建时间,默认为当前时间
                update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- 更新时间，默认为当前时间
            );
        """)
        print("表 'Product' 创建成功。")

        # create Order table
        cur.execute("""
            CREATE TABLE "Order" (
                id SERIAL PRIMARY KEY, -- 订单id,自动递增主键
                order_code BIGINT UNIQUE NOT NULL, -- 订单号，唯一且非空
                user_id INTEGER DEFAULT NULL, -- 用户id,允许为空（例如匿名订单）
                payment DECIMAL(20, 2) DEFAULT NULL, -- 付款金额,单位是元, 保留两位小数，允许为空
                create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 订单创建时间，默认为当前时间
                update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 订单更新时间，默认为当前时间
                CONSTRAINT fk_order_user FOREIGN KEY (user_id) REFERENCES "User"(id) ON DELETE SET NULL -- 外键约束,如果用户被删除,则用户ID设为NULL
            );
        """)
        print("表 'Order' 创建成功。")

        # create orderItem table
        cur.execute("""
            CREATE TABLE "OrderItem" (
                id SERIAL PRIMARY KEY, -- 订单子表id,自动递增主键
                user_id INTEGER DEFAULT NULL, -- 用户id,冗余字段,提高查询效率,允许为空
                order_code BIGINT DEFAULT NULL, -- 订单号,允许为空
                product_id INTEGER DEFAULT NULL, -- 商品id,允许为空
                current_unit_price DECIMAL(20, 2) DEFAULT NULL, -- 生成订单时的商品单价,单位是元,保留两位小数,允许为空
                quantity INTEGER DEFAULT NULL CHECK (quantity >= 0), -- 商品购买数量,允许为空且不能为负
                total_price DECIMAL(20, 2) DEFAULT NULL, -- 商品总价，单位是元,保留两位小数,允许为空
                create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 创建时间,默认为当前时间
                update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 更新时间，默认为当前时间
                CONSTRAINT fk_orderitem_order FOREIGN KEY (order_code) REFERENCES "Order"(order_code) ON DELETE SET NULL, -- 外键约束,如果订单被删除,则订单号设为NULL
                CONSTRAINT fk_orderitem_product FOREIGN KEY (product_id) REFERENCES "Product"(id) ON DELETE SET NULL -- 外键约束,如果商品被删除,则商品ID设为NULL
            );
        """)
        print("表 'OrderItem' 创建成功。")

        conn.commit()
        print("所有表创建成功。")

        print("表 'User' 的索引：'id' (主键) 和 'username' (唯一) 已隐式索引。")
        print("表 'Product' 的索引：'id' (主键) 已隐式索引。")

        # Order index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_order_user_id ON \"Order\" (user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_order_create_time ON \"Order\" (create_time DESC);")
        print("表 'Order' 的索引已创建。")

        # OrderItem index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orderitem_order_code ON \"OrderItem\" (order_code);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orderitem_product_id ON \"OrderItem\" (product_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orderitem_user_order_code ON \"OrderItem\" (user_id, order_code);")
        print("表 'OrderItem' 的索引已创建。")

        conn.commit()
        print("所有索引创建成功或已存在。")

    except psycopg2.Error as e:
        print(f"连接PostgreSQL或创建表/索引时出错: {e}")
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭。")

def insert_test_data(host, database, user, password):
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cur = conn.cursor()

        print("开始插入测试数据...")

        # 生成用户数据
        user_prefixes = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Theta", "Iota", "Kappa", "Lambda"]
        user_suffixes = ["User", "Knight", "Rider", "Mage", "Guardian", "Wanderer", "Explorer", "Seeker", "Dreamer", "Architect"]

        target_num_users = BASE_NUM_USERS

        generated_usernames = set()
        user_ids = []
        for prefix in user_prefixes:
            for suffix in user_suffixes:
                if len(user_ids) >= target_num_users:
                    break
                username = f"{prefix}{suffix}{random.randint(1, 9999)}" 
                if username not in generated_usernames:
                    cur.execute("INSERT INTO \"User\" (username) VALUES (%s) RETURNING id;", (username,))
                    user_ids.append(cur.fetchone()[0])
                    generated_usernames.add(username)
            if len(user_ids) >= target_num_users:
                break
        print(f"插入了 {len(user_ids)} 条用户数据。")

        # 生成商品数据
        product_adjectives = ["Premium", "Ultra", "Dynamic", "Compact", "Ergonomic", "Smart", "Portable", "Sleek", "Durable", "Advanced"]
        product_midfixes = ["Pro", "Max", "Lite", "Plus", "Core", "Prime", "Elite", "Connect", "Vision", "Master"] 
        product_nouns = ["Mouse", "Keyboard", "Monitor", "SSD", "Headphones", "Webcam", "Chair", "Speaker", "Charger", "Dock"]

        target_num_products = BASE_NUM_PRODUCTS

        products_data = []
        product_names_set = set()
        for adj in product_adjectives:
            for mid in product_midfixes:
                for noun in product_nouns:
                    if len(products_data) >= target_num_products:
                        break
                    product_name = f"{adj} {mid} {noun} {random.randint(1000, 9999)}" 
                    if product_name not in product_names_set:
                        description = f"A {adj.lower()}, {mid.lower()} and {noun.lower()} with excellent features."
                        price = round(random.uniform(50.00, 5000.00), 2)
                        stock = random.randint(10, 500)
                        create_time_prod = datetime(2025, random.randint(1, 12), random.randint(1, 28),
                                                    random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
                        update_time_prod = create_time_prod + timedelta(minutes=random.randint(0, 60))

                        products_data.append((product_name, description, price, stock, create_time_prod, update_time_prod))
                        product_names_set.add(product_name)
                if len(products_data) >= target_num_products:
                    break
            if len(products_data) >= target_num_products:
                break

        product_ids = []
        for name, desc, price, stock, create_time_prod, update_time_prod in products_data:
            cur.execute("INSERT INTO \"Product\" (name, description, price, stock, create_time, update_time) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                        (name, desc, price, stock, create_time_prod, update_time_prod))
            product_ids.append(cur.fetchone()[0])
        print(f"插入了 {len(product_ids)} 条商品数据。")

        # 生成订单数据
        orders_data = []
        order_codes = set()

        target_num_orders = BASE_NUM_ORDERS

        for i in range(target_num_orders):
            user_id = random.choice(user_ids)
            order_code = random.randint(100000000000, 999999999999)
            while order_code in order_codes:
                order_code = random.randint(100000000000, 999999999999)
            order_codes.add(order_code)

            create_time = datetime(2025, random.randint(1, 12), random.randint(1, 28),
                                   random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
            update_time = create_time + timedelta(minutes=random.randint(0, 60))

            orders_data.append((order_code, user_id, 0.00, create_time, update_time))

        inserted_orders = []
        for order_code, user_id, payment, create_time, update_time in orders_data:
            cur.execute("INSERT INTO \"Order\" (order_code, user_id, payment, create_time, update_time) VALUES (%s, %s, %s, %s, %s) RETURNING id, order_code;",
                        (order_code, user_id, payment, create_time, update_time))
            inserted_orders.append(cur.fetchone())
        print(f"插入了 {len(inserted_orders)} 条订单数据。")

        # 生成订单项数据
        total_order_items = 0
        for order_id, order_code in inserted_orders:
            num_items = random.randint(1, 10)
            order_total_payment = 0.00
            for _ in range(num_items): 
                product_id = random.choice(product_ids)
                cur.execute("SELECT price FROM \"Product\" WHERE id = %s;", (product_id,))
                product_price = cur.fetchone()[0]

                quantity = random.randint(1, 20)
                current_unit_price = product_price
                total_price = current_unit_price * quantity

                item_create_time = datetime(2025, random.randint(1, 12), random.randint(1, 28),
                                            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
                item_update_time = item_create_time + timedelta(minutes=random.randint(0, 60))

                cur.execute("""
                    INSERT INTO "OrderItem" (user_id, order_code, product_id, current_unit_price, quantity, total_price, create_time, update_time)
                    SELECT user_id, %s, %s, %s, %s, %s, %s, %s FROM "Order" WHERE id = %s;
                """, (order_code, product_id, current_unit_price, quantity, total_price, item_create_time, item_update_time, order_id))
                order_total_payment += total_price
                total_order_items += 1

            cur.execute("UPDATE \"Order\" SET payment = %s, update_time = %s WHERE id = %s;",
                        (order_total_payment, datetime.now(), order_id))

        print(f"插入了 {total_order_items} 条订单项数据。")

        conn.commit()
        print("所有测试数据插入成功。")

    except psycopg2.Error as e:
        print(f"连接PostgreSQL或插入测试数据时出错: {e}")
        if conn:
            conn.rollback()
            print("事务已回滚。")
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭。")
