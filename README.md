# database_test

### 数据库表结构

1. **`User` (用户表)**
   - **`id`**: 用户ID，`SERIAL`类型，主键，自动递增。
   - **`username`**: 用户名，`VARCHAR(50)`类型，非空且唯一。
   - **作用**: 存储系统中的用户信息。
2. **`Product` (商品表)**
   - **`id`**: 商品ID，`SERIAL`类型，主键，自动递增。
   - **`name`**: 商品名称，`VARCHAR(255)`类型，非空。
   - **`description`**: 商品描述，`TEXT`类型。
   - **`price`**: 商品单价，`DECIMAL(20, 2)`类型，非空，表示人民币价格，保留两位小数。
   - **`stock`**: 库存数量，`INTEGER`类型，非空，且不能为负。
   - **`create_time`**: 创建时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **`update_time`**: 更新时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **作用**: 存储系统中所有可售商品的信息。
3. **`Order` (订单表)**
   - **`id`**: 订单ID，`SERIAL`类型，主键，自动递增。
   - **`order_code`**: 订单号，`BIGINT`类型，唯一且非空。
   - **`user_id`**: 用户ID，`INTEGER`类型，可为空（例如匿名订单），与 `User` 表的 `id` 字段建立外键关联，如果用户被删除，此字段设为NULL (`ON DELETE SET NULL`)。
   - **`payment`**: 订单总付款金额，`DECIMAL(20, 2)`类型，可为空，保留两位小数。
   - **`create_time`**: 订单创建时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **`update_time`**: 订单更新时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **作用**: 存储用户提交的订单信息，包括订单总价和关联用户。
4. **`OrderItem` (订单项表)**
   - **`id`**: 订单项ID，`SERIAL`类型，主键，自动递增。
   - **`user_id`**: 用户ID，`INTEGER`类型，可为空，冗余字段，用于提高查询效率。
   - **`order_code`**: 订单号，`BIGINT`类型，可为空，与 `Order` 表的 `order_code` 字段建立外键关联，如果订单被删除，此字段设为NULL (`ON DELETE SET NULL`)。
   - **`product_id`**: 商品ID，`INTEGER`类型，可为空，与 `Product` 表的 `id` 字段建立外键关联，如果商品被删除，此字段设为NULL (`ON DELETE SET NULL`)。
   - **`current_unit_price`**: 生成订单项时的商品单价，`DECIMAL(20, 2)`类型，可为空，保留两位小数。
   - **`quantity`**: 商品购买数量，`INTEGER`类型，可为空，且不能为负。
   - **`total_price`**: 该订单项的总价 (`current_unit_price * quantity`)，`DECIMAL(20, 2)`类型，可为空，保留两位小数。
   - **`create_time`**: 创建时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **`update_time`**: 更新时间，`TIMESTAMP WITH TIME ZONE`类型，默认为当前时间。
   - **作用**: 存储订单中的具体商品项，记录了每个商品在订单生成时的价格和数量。

### 事务类型

项目定义了多种事务操作来模拟真实的业务场景，并测试数据库在这些操作和故障注入下的行为。

1. **`insert_order()` (插入订单)**
   - **功能**: 为随机选择的用户生成一个新订单，并为其分配一个唯一的订单号，设置付款金额，以及创建和更新时间。
   - **涉及表**: 主要涉及 `Order` 表。
   - **一致性考量**: 确保订单号的唯一性。
2. **`select_order()` (查询订单)**
   - **功能**: 随机选择一个现有订单号，并从 `Order` 表中查询其所有详细信息。
   - **涉及表**: 主要涉及 `Order` 表。
   - **一致性考量**: 验证查询到的订单数据是否与预期一致。
3. **`update_order()` (更新订单)**
   - **功能**: 随机选择一个现有订单号，更新其付款金额和更新时间。
   - **涉及表**: 主要涉及 `Order` 表。
   - **一致性考量**: 确保更新操作的原子性，即付款金额和更新时间同时生效。
4. **`delete_order()` (删除订单)**
   - **功能**: 随机选择一个现有订单号，从 `Order` 表中删除该订单。
   - **涉及表**: 主要涉及 `Order` 表。由于 `OrderItem` 表通过外键关联 `Order` 表的 `order_code` 并设置为 `ON DELETE SET NULL`，删除订单会将其相关联的订单项的 `order_code` 设为 `NULL`。
   - **一致性考量**: 验证级联操作的正确性，确保相关联的 `OrderItem` 记录被正确处理。
5. **`delete_product_and_related_order_items()` (删除产品及相关订单项)**
   - **功能**: 这是一个多表事务。它会随机选择一个产品，执行以下操作：
     1. 查询所有包含该产品的订单号。
     2. 删除 `OrderItem` 表中所有与该产品ID相关的订单项。
     3. 删除 `Product` 表中的该产品。
     4. 对于受影响的每个订单，重新计算其总支付金额，并更新 `Order` 表中的 `payment` 和 `update_time` 字段。
   - **涉及表**: `Product`, `OrderItem`, `Order`。
   - **一致性考量**: 确保在删除产品时，所有相关的订单项被删除，并且所有受影响的订单的总金额都被正确更新。如果在此过程中发生故障，需要验证事务是否正确回滚，保持数据一致性。
6. **`modify_product_price_and_update_orders()` (修改产品价格并更新订单)**
   - **功能**: 这是一个多表事务。它会随机选择一个产品，生成一个新价格，然后执行以下操作：
     1. 更新 `Product` 表中该产品的价格和更新时间。
     2. 更新 `OrderItem` 表中所有包含该产品的订单项的 `current_unit_price` 和 `total_price`，并更新其 `update_time`。
     3. 对于受影响的每个订单，重新计算其总支付金额，并更新 `Order` 表中的 `payment` 和 `update_time` 字段。
   - **涉及表**: `Product`, `OrderItem`, `Order`。
   - **一致性考量**: 确保产品价格的变更能够正确地反映到所有历史订单项（即订单项中的 `current_unit_price` 和 `total_price`），并且所有受影响的订单的总金额都被正确更新。如果在此过程中发生故障，需要验证事务是否正确回滚，保持数据一致性。
7. **`long_running_price_update()` (长事务价格更新)**
   - **功能**: 这是一个长时间运行的事务，用于模拟持续性的数据更新。它会不断地对一个产品进行价格更新，并相应地更新所有关联的 `OrderItem` 和 `Order` 表。在事务执行的特定迭代次数时，会模拟故障注入（数据库关闭）。
   - **涉及表**: `Product`, `OrderItem`, `Order`。
   - **一致性考量**: 专门用于测试数据库在长时间运行事务中发生故障时的恢复能力和数据一致性，验证事务的原子性和持久性。

### 故障注入模式

项目支持多种故障注入模式，通过在事务执行过程中突然关闭数据库来模拟系统崩溃，以测试数据库的ACID特性（原子性、一致性、隔离性、持久性）：

- **`single_injection` (单次注入)**: 在达到预设的事务数量后，数据库会被关闭一次。
- **`two_phase_injection` (双阶段注入)**: 数据库会在两个不同的事务数量点被关闭两次。
- **`multi_table_delete_fault_injection` (多表删除故障注入)**: 在执行 `delete_product_and_related_order_items` 事务的关键步骤（删除订单项之后，删除产品之前）注入故障。
- **`multi_table_price_fault_injection` (多表价格故障注入)**: 在执行 `modify_product_price_and_update_orders` 事务的关键步骤（更新订单项之后，更新订单之前）注入故障。
- **`long_fault` (长事务故障)**: 在一个长时间运行的事务的特定迭代中注入故障。

这些测试旨在验证数据库在面对意外故障时，能否确保所有已提交的事务数据保持一致，未提交的事务被正确回滚，从而保证数据库的可靠性。