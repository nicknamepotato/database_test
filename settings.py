# 程序设置
K_WORKERS = 5  # 并发工作器数目
NUM_TRANSACTIONS_PER_WORKER = 2000  # 每个工作器执行的事务数

# 长事务相关参数
LONG_TRANSACTION_ITERATIONS = 1000 # 长事务循环次数
FAULT_INJECTION_ITERATION = 998 # 故障注入发生的迭代次数

# 故障注入相关参数
FIRST_INJECTION_TRANSACTIONS = 5000  # 第一个故障注入的事务数
SECOND_INJECTION_TRANSACTIONS = 2000 # 第二个故障注入的事务数（用于两阶段注入）

# 故障注入时间设置
FIRST_INJECTION_TIME = 10  # 第一个故障注入的时间（秒）
SECOND_INJECTION_TIME = FIRST_INJECTION_TIME / 2   # 第二个故障注入的时间（秒）

# 故障注入类型
# 选择: 'single_injection', 'two_phase_injection',
# ‘long_fault','none'
FAULT_MODE = 'multi_table_price_fault_injection'

# 不同事务类型的比例，百分比，总和应为100
TRANSACTION_RATIOS = {
    'insert': 40,
    'select': 20,
    'update': 10,
    'delete': 10,
    'delete_product_multi_table': 10,
    'modify_product_price_multi_table': 10
}

# 基础数据量设置  
BASE_NUM_USERS = 10  # 用户数目
BASE_NUM_PRODUCTS = BASE_NUM_USERS * 20  # 商品数目
BASE_NUM_ORDERS = BASE_NUM_USERS * 40  # 订单数目