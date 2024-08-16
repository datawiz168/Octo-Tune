import time
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from postgresql_tuner import PostgreSQLTuner
import threading


class TimeoutException(Exception):
    pass


def run_with_timeout(func, args=(), kwargs={}, timeout_duration=10, default=None):
    class FuncThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result = default
            self.exc = None

        def run(self):
            try:
                self.result = func(*args, **kwargs)
            except Exception as e:
                self.exc = e

    func_thread = FuncThread()
    func_thread.start()
    func_thread.join(timeout_duration)

    if func_thread.is_alive():
        raise TimeoutException("Timed out!")

    if func_thread.exc:
        raise func_thread.exc

    return func_thread.result


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

            # 插入示例数据
            cursor.execute("INSERT INTO users (name) SELECT 'User' || generate_series(1, 10000) ON CONFLICT DO NOTHING")
            cursor.execute('''
                INSERT INTO orders (user_id, total_amount)
                SELECT 
                    floor(random() * 10000 + 1)::int,
                    (random() * 1000)::numeric(10,2)
                FROM generate_series(1, 100000)
                ON CONFLICT DO NOTHING
            ''')

            self.tuner.pg_conn.commit()
            print("Test environment setup completed successfully.")
        except Exception as e:
            self.tuner.pg_conn.rollback()
            print(f"Error setting up test environment: {e}")
        finally:
            cursor.close()

    def run_query(self, query, timeout=30):  # 默认30秒超时
        cursor = self.tuner.pg_conn.cursor()
        try:
            def execute_query():
                cursor.execute(query)
                return cursor.fetchall()

            start_time = time.time()
            run_with_timeout(execute_query, timeout_duration=timeout)
            end_time = time.time()
            return end_time - start_time
        except TimeoutException:
            print(f"Query timed out after {timeout} seconds: {query[:50]}...")  # 只打印查询的前50个字符
            return timeout  # 返回超时时间作为执行时间
        except Exception as e:
            print(f"Error executing query: {e}")
            return -1  # 返回-1表示查询执行出错
        finally:
            cursor.close()

    def run_test_suite(self):
        tests = [
            ("Simple SELECT", "SELECT * FROM users LIMIT 10000;"),
            ("JOIN operation",
             "SELECT orders.*, users.name FROM orders JOIN users ON orders.user_id = users.id LIMIT 10000;"),
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
            ''', (work_mem, effective_cache_size, float(random_page_cost), execution_time, test_name))

        self.tuner.sqlite_conn.commit()
        cursor.close()
        print(f"Test results saved to SQLite database.")


class EnhancedTestSuite(PerformanceTestSuite):
    def run_complex_query(self):
        query = """
        WITH ranked_orders AS (
            SELECT 
                user_id,
                total_amount,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total_amount DESC) as rank
            FROM orders
        )
        SELECT 
            u.id,
            u.name,
            COUNT(ro.user_id) as total_orders,
            AVG(ro.total_amount) as avg_order_amount,
            MAX(CASE WHEN ro.rank = 1 THEN ro.total_amount END) as highest_order_amount
        FROM users u
        LEFT JOIN ranked_orders ro ON u.id = ro.user_id
        GROUP BY u.id, u.name
        HAVING COUNT(ro.user_id) > 5
        ORDER BY avg_order_amount DESC
        LIMIT 100;
        """
        return self.run_query(query)

    def run_concurrent_queries(self):
        queries = [
            "SELECT AVG(total_amount) FROM orders;",
            "SELECT COUNT(*) FROM users WHERE created_at > CURRENT_DATE - INTERVAL '30 days';",
            "SELECT user_id, SUM(total_amount) FROM orders GROUP BY user_id ORDER BY SUM(total_amount) DESC LIMIT 10;",
            "SELECT * FROM users ORDER BY RANDOM() LIMIT 100;",
            "SELECT DATE(created_at), COUNT(*) FROM orders GROUP BY DATE(created_at) ORDER BY DATE(created_at);"
        ]

        def execute_query(query):
            return self.run_query(query)

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(execute_query, query) for query in queries]
            results = [future.result() for future in as_completed(futures)]
        end_time = time.time()

        return end_time - start_time, results

    def run_enhanced_test_suite(self):
        results = super().run_test_suite()
        print("Standard tests completed. Starting enhanced tests.")

        print("Running complex query...")
        complex_query_time = self.run_complex_query()
        results["Enhanced Complex Query"] = complex_query_time
        print(f"Complex query completed in {complex_query_time:.4f} seconds")

        print("Running concurrent queries...")
        concurrent_time, concurrent_results = self.run_concurrent_queries()
        results["Concurrent Queries"] = concurrent_time
        for i, time in enumerate(concurrent_results):
            results[f"Concurrent Query {i + 1}"] = time
        print(f"Concurrent queries completed in {concurrent_time:.4f} seconds")

        print("All enhanced tests completed.")
        return results


def run_multi_parameter_tests(parameter_values):
    try:
        tuner = PostgreSQLTuner()
        tuner.connect_postgresql()
        tuner.connect_sqlite()
        tuner.create_sqlite_tables()
        tuner.clear_sqlite_data()  # 清理旧数据

        test_suite = EnhancedTestSuite(tuner)
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

            try:
                results = test_suite.run_enhanced_test_suite()
                test_suite.save_results(results, work_mem, effective_cache_size, random_page_cost)
            except Exception as e:
                print(f"An error occurred during test execution or saving results: {e}")
                # 可以选择继续下一次迭代或者中断整个过程
                # 如果选择继续，取消注释下一行
                # continue

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'tuner' in locals():
            tuner.close_connections()


if __name__ == "__main__":
    parameter_values = {
        'work_mem': ['4MB', '16MB', '64MB'],
        'effective_cache_size': ['100MB', '200MB'],
        'random_page_cost': ['2.0', '3.0']
    }
    run_multi_parameter_tests(parameter_values)
