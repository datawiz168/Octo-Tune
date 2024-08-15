import time
from postgresql_tuner import PostgreSQLTuner
import itertools

class PerformanceTestSuite:
    def __init__(self, tuner):
        self.tuner = tuner

    def setup_test_environment(self):
        cursor = self.tuner.pg_conn.cursor()
        try:
            # 创建表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    total_amount DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 插入更多示例数据
            cursor.execute("INSERT INTO users (name) SELECT 'User' || generate_series(1, 10000)")
            cursor.execute('''
                INSERT INTO orders (user_id, total_amount)
                SELECT 
                    floor(random() * 10000 + 1)::int,
                    (random() * 1000)::numeric(10,2)
                FROM generate_series(1, 100000)
            ''')
            
            self.tuner.pg_conn.commit()
            print("Test environment setup completed successfully.")
        except Exception as e:
            self.tuner.pg_conn.rollback()
            print(f"Error setting up test environment: {e}")
        finally:
            cursor.close()

    def run_query(self, query):
        cursor = self.tuner.pg_conn.cursor()
        start_time = time.time()
        cursor.execute(query)
        cursor.fetchall()  # 确保查询完全执行
        end_time = time.time()
        cursor.close()
        return end_time - start_time

    def run_test_suite(self):
        tests = [
            ("Simple SELECT", "SELECT * FROM users LIMIT 10000;"),
            ("JOIN operation", "SELECT orders.*, users.name FROM orders JOIN users ON orders.user_id = users.id LIMIT 10000;"),
            ("Aggregation", "SELECT user_id, COUNT(*), AVG(total_amount) FROM orders GROUP BY user_id;"),
            ("Complex query", """
                SELECT u.name, COUNT(o.id) as order_count, AVG(o.total_amount) as avg_order_amount
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE u.created_at > '2023-01-01'
                GROUP BY u.id
                HAVING COUNT(o.id) > 0
                ORDER BY avg_order_amount DESC
                LIMIT 1000;
            """)
        ]

        results = {}
        for test_name, query in tests:
            execution_time = self.run_query(query)
            results[test_name] = execution_time
            print(f"{test_name}: {execution_time:.4f} seconds")

        return results

    def save_results(self, results, work_mem, effective_cache_size, random_page_cost):
        cursor = self.tuner.sqlite_conn.cursor()

        for test_name, execution_time in results.items():
            cursor.execute('''
                INSERT INTO performance_tests 
                (work_mem, effective_cache_size, random_page_cost, execution_time, test_name)
                VALUES (?, ?, ?, ?, ?)
            ''', (work_mem, effective_cache_size, random_page_cost, execution_time, test_name))

        self.tuner.sqlite_conn.commit()
        cursor.close()
        print(f"Test results saved to SQLite database.")

def run_multi_parameter_tests(parameter_values):
    tuner = PostgreSQLTuner()
    tuner.connect_postgresql()
    tuner.connect_sqlite()
    tuner.create_sqlite_tables()
    tuner.clear_sqlite_data()  # 清理旧数据

    test_suite = PerformanceTestSuite(tuner)
    test_suite.setup_test_environment()

    # 生成所有参数组合
    combinations = list(itertools.product(
        parameter_values['work_mem'],
        parameter_values['effective_cache_size'],
        parameter_values['random_page_cost']
    ))

    for work_mem, effective_cache_size, random_page_cost in combinations:
        print(f"\nRunning tests with:")
        print(f"work_mem = {work_mem}")
        print(f"effective_cache_size = {effective_cache_size}")
        print(f"random_page_cost = {random_page_cost}")

        tuner.set_parameter_value('work_mem', work_mem)
        tuner.set_parameter_value('effective_cache_size', effective_cache_size)
        tuner.set_parameter_value('random_page_cost', str(random_page_cost))

        results = test_suite.run_test_suite()
        test_suite.save_results(results, work_mem, effective_cache_size, random_page_cost)

    tuner.close_connections()

if __name__ == "__main__":
    parameter_values = {
        'work_mem': ['4MB', '16MB', '64MB', ],
        'effective_cache_size': ['100MB', '200MB'],
        'random_page_cost': ['2.0', '3.0' ]
    }
    run_multi_parameter_tests(parameter_values)
