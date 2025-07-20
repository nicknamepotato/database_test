import psycopg2
import random
import sys
import time
import datetime 
from connection import DSN
from fault_injector import inject_fault
from settings import LONG_TRANSACTION_ITERATIONS, FAULT_INJECTION_ITERATION

def long_running_price_update(product_id, pg_data_dir):
    conn = None
    try:
        conn = psycopg2.connect(dsn=DSN)
        conn.autocommit = False 

        cur = conn.cursor()

        cur.execute("SELECT price FROM \"Product\" WHERE id = %s;", (product_id,))
        initial_price_row = cur.fetchone()
        if not initial_price_row:
            print(f"产品 ID {product_id} 未找到，无法执行长事务。") 
            return False
        initial_price = float(initial_price_row[0])
        print(f"产品 ID {product_id} 的初始价格: {initial_price}")

        print(f"初始价格 + 1000: {initial_price + 1000:.2f}")

        current_price = initial_price
        for i in range(1, LONG_TRANSACTION_ITERATIONS + 1):
            current_price += 1.00  
            update_time = datetime.datetime.now()  

            cur.execute("""
                UPDATE "Product"
                SET price = %s, update_time = %s
                WHERE id = %s;
            """, (current_price, update_time, product_id))

            cur.execute("""
                UPDATE "OrderItem"
                SET current_unit_price = %s,
                    total_price = %s * quantity,
                    update_time = %s
                WHERE product_id = %s;
            """, (current_price, current_price, update_time, product_id))

            cur.execute("SELECT DISTINCT order_code FROM \"OrderItem\" WHERE product_id = %s;", (product_id,))
            affected_order_codes = [row[0] for row in cur.fetchall()]

            for order_code in affected_order_codes:
                cur.execute("""
                    SELECT COALESCE(SUM(total_price), 0.00)
                    FROM "OrderItem"
                    WHERE order_code = %s;
                """, (order_code,))
                new_total_order_payment = cur.fetchone()[0]

                cur.execute("""
                    UPDATE "Order"
                    SET payment = %s, update_time = %s
                    WHERE order_code = %s;
                """, (new_total_order_payment, datetime.datetime.now(), order_code))


            if i == FAULT_INJECTION_ITERATION:
                print(f"在第 {i} 次循环时注入故障...")
                inject_fault(pg_data_dir)
                sys.exit(1) 

            if i % 100 == 0:
                print(f"产品 ID {product_id}：第 {i} 次循环，当前价格: {current_price:.2f}") 

        conn.commit()
        print(f"产品 ID {product_id} 的长事务完成，最终价格: {current_price:.2f}")
        return True

    except psycopg2.Error as e:
        print(f"长事务执行失败: {e}")
        if conn:
            conn.rollback()
            print("事务已回滚。") 
        return False
    finally:
        if conn:
            conn.close()

        print("\n--- 验证数据一致性 ---") 
        try:
            temp_conn = psycopg2.connect(dsn=DSN)
            temp_cur = temp_conn.cursor()

            temp_cur.execute("SELECT price FROM \"Product\" WHERE id = %s;", (product_id,))
            final_product_price = temp_cur.fetchone()
            if final_product_price:
                print(f"程序运行后 Product.price (ID {product_id}): {float(final_product_price[0]):.2f}") 
            else:
                print(f"未找到产品 ID {product_id} 的最终价格。") 

            temp_cur.execute("""
                SELECT current_unit_price, total_price 
                FROM "OrderItem" 
                WHERE product_id = %s;
            """, (product_id,))
            order_items_data = temp_cur.fetchall()
            
            if order_items_data:
                total_order_items_price = sum(float(item[1]) for item in order_items_data)
                first_order_item_unit_price = float(order_items_data[0][0])
                print(f"程序运行后 OrderItem.current_unit_price (for product ID {product_id}): {first_order_item_unit_price:.2f}")
                print(f"程序运行后 OrderItem.total_price (总和 for product ID {product_id}): {total_order_items_price:.2f}")
            else:
                print(f"未找到产品 ID {product_id} 相关的订单项数据。") 

            temp_cur.execute("SELECT DISTINCT order_code FROM \"OrderItem\" WHERE product_id = %s;", (product_id,))
            affected_order_codes_final = [row[0] for row in temp_cur.fetchall()]

            if affected_order_codes_final:
                total_orders_payment = 0.00
                for order_code in affected_order_codes_final:
                    temp_cur.execute("SELECT payment FROM \"Order\" WHERE order_code = %s;", (order_code,))
                    order_payment_row = temp_cur.fetchone()
                    if order_payment_row:
                        total_orders_payment += float(order_payment_row[0])
                print(f"程序运行后 Order.payment (总和 for affected orders): {total_orders_payment:.2f}")
            else:
                print(f"未找到受影响的订单数据。")

            temp_conn.close()

        except psycopg2.OperationalError as e:
            print(f"无法连接到数据库以验证最终数据一致性: {e}") 
            print("请确保数据库已运行。") 
        except Exception as e:
            print(f"验证最终数据一致性时发生错误: {e}")

