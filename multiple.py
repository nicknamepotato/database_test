import psycopg2
import random
from connection import DSN, DB_CONFIG
from CRUD import DBConn
from fault_injector import inject_fault
import sys
import datetime # Import datetime for update_time

class MultiTableOperations:
    def __init__(self):
        self.existing_product_ids = self._load_product_ids()
        self.existing_order_codes = self._load_order_codes()
        self.existing_user_ids = self._load_user_ids()

    def _load_product_ids(self):
        try:
            with DBConn() as conn:
                conn.cursor.execute("SELECT id FROM \"Product\";")
                return [row[0] for row in conn.cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"加载产品ID失败 {e}") # Failed to load product IDs
            return []

    def _load_order_codes(self):
        try:
            with DBConn() as conn:
                conn.cursor.execute("SELECT order_code FROM \"Order\";")
                return [row[0] for row in conn.cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"加载订单号失败 {e}") # Failed to load order codes
            return []

    def _load_user_ids(self):
        try:
            with DBConn() as conn:
                conn.cursor.execute("SELECT id FROM \"User\";")
                return [row[0] for row in conn.cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"加载用户ID失败 {e}") # Failed to load user IDs
            return []

    def delete_product_and_related_order_items(self, product_id=None, inject_fault_at_point=None, pg_data_dir=None):
        if not self.existing_product_ids:
            print("没有可删除的产品。") # No products to delete.
            return False

        if product_id is None:
            product_id = random.choice(self.existing_product_ids)

        conn = None
        try:
            conn = psycopg2.connect(dsn=DSN)
            conn.autocommit = False # Set autocommit to False
            cur = conn.cursor()

            cur.execute("SELECT DISTINCT order_code FROM \"OrderItem\" WHERE product_id = %s;", (product_id,))
            affected_order_codes = [row[0] for row in cur.fetchall()]

            cur.execute("DELETE FROM \"OrderItem\" WHERE product_id = %s;", (product_id,))
            deleted_order_items_count = cur.rowcount
            print(f"删除了 {deleted_order_items_count} 个与产品 ID {product_id} 相关的订单项。") # Deleted related order items for product ID.

            if inject_fault_at_point == 'after_step2' and pg_data_dir:
                print(f" 在删除产品 ID {product_id} 的订单项后注入故障 ") # Injecting fault after deleting order items for product ID.
                inject_fault(pg_data_dir)
                sys.exit(1)

            cur.execute("DELETE FROM \"Product\" WHERE id = %s;", (product_id,))
            deleted_product_count = cur.rowcount

            if deleted_product_count == 0:
                print(f"未找到 ID 为 {product_id} 的产品。") # Product with ID not found.
                conn.rollback()
                return False

            print(f"已删除 ID 为 {product_id} 的产品。") # Product with ID deleted.

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
                """, (new_total_order_payment, datetime.datetime.now(), order_code)) # Update update_time
                print(f"重新计算并更新订单 {order_code} 的总支付金额为 {new_total_order_payment}。") # Recalculated and updated total payment for order.

            conn.commit()
            print(f"成功删除产品 ID {product_id}，其 {deleted_order_items_count} 个相关订单项，并更新受影响的订单。") # Successfully deleted product ID, its related order items, and updated affected orders.

            if product_id in self.existing_product_ids:
                self.existing_product_ids.remove(product_id)
            return True

        except psycopg2.Error as e:
            print(f"删除产品及相关订单项时出错: {e}") # Error deleting product and related order items
            if conn:
                conn.rollback()
                print("事务已回滚。") # Transaction rolled back.
            return False
        finally:
            if conn:
                conn.close()

    def modify_product_price_and_update_orders(self, product_id=None, new_price=None, inject_fault_at_point=None, pg_data_dir=None):
        if not self.existing_product_ids:
            print("没有产品可以修改价格。") # No products to modify price.
            return False

        if product_id is None:
            product_id = random.choice(self.existing_product_ids)

        conn = None
        try:
            conn = psycopg2.connect(dsn=DSN)
            conn.autocommit = False   # Set to not autocommit, treated as one transaction
            cur = conn.cursor()

            cur.execute("SELECT price FROM \"Product\" WHERE id = %s;", (product_id,))
            current_product_data = cur.fetchone()
            if not current_product_data:
                print(f" ID为{product_id} 的产品未找到。") # Product with ID not found.
                conn.rollback()
                return False

            if new_price is None:
                current_price = current_product_data[0]
                price_change_factor = random.uniform(0.5, 1.5)
                new_price = round(float(current_price) * price_change_factor, 2)
                if new_price <= 0: new_price = 0.01

            cur.execute("UPDATE \"Product\" SET price = %s, update_time = %s WHERE id = %s;", (new_price, datetime.datetime.now(), product_id)) # Update update_time
            updated_product_count = cur.rowcount
            if updated_product_count == 0:
                print(f"未能更新 ID 为 {product_id} 的产品价格。") # Failed to update product price for ID.
                conn.rollback()
                return False
            print(f"已将产品 ID {product_id} 的价格更新为 {new_price}。") # Product ID price updated.

            cur.execute("SELECT DISTINCT order_code FROM \"OrderItem\" WHERE product_id = %s;", (product_id,))
            affected_order_codes = [row[0] for row in cur.fetchall()]

            cur.execute("""
                UPDATE "OrderItem"
                SET current_unit_price = %s,
                    total_price = %s * quantity,
                    update_time = %s
                WHERE product_id = %s;
            """, (new_price, new_price, datetime.datetime.now(), product_id)) # Update update_time
            updated_order_items_count = cur.rowcount
            print(f"已更新 {updated_order_items_count} 个订单项，产品 ID 为 {product_id}。") # Updated order items for product ID.

            if inject_fault_at_point == 'after_order_item_update' and pg_data_dir:
                print(f"在更新ID为 {product_id} 的产品价格后注入故障。") # Injecting fault after updating product price for ID.
                inject_fault(pg_data_dir)
                sys.exit(1)

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
                """, (new_total_order_payment, datetime.datetime.now(), order_code)) # Update update_time
                print(f"已重新计算并更新订单 {order_code} 的总支付金额为 {new_total_order_payment}。") # Recalculated and updated total payment for order.

            conn.commit()
            print(f"成功修改产品 ID {product_id} 的价格，并更新了 {updated_order_items_count} 个相关订单项和 {len(affected_order_codes)} 个受影响的订单。") # Successfully modified product ID price, and updated related order items and affected orders.
            return True

        except psycopg2.Error as e:
            print(f"修改产品价格和更新订单时出错: {e}") # Error modifying product price and updating orders
            if conn:
                conn.rollback()
                print("事务已回滚。") # Transaction rolled back.
            return False
        finally:
            if conn:
                conn.close()
