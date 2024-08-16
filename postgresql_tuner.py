import psycopg2
import sqlite3
from configparser import ConfigParser
import GPyOpt
import numpy as np


class PostgreSQLTuner:
    def __init__(self, config_file='database.ini'):
        self.config = self.read_config(config_file)
        self.pg_conn = None
        self.sqlite_conn = None

    def read_config(self, filename):
        parser = ConfigParser()
        parser.read(filename)
        return {section: dict(parser.items(section)) for section in parser.sections()}

    def connect_postgresql(self):
        try:
            self.pg_conn = psycopg2.connect(**self.config['postgresql'])
            print("已成功连接到 PostgreSQL 数据库!")
        except (Exception, psycopg2.Error) as error:
            print("连接 PostgreSQL 时出错", error)

    def connect_sqlite(self):
        try:
            self.sqlite_conn = sqlite3.connect(self.config['sqlite']['database'])
            print("已成功连接到 SQLite 数据库!")
        except sqlite3.Error as error:
            print("连接 SQLite 时出错", error)

    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        print("数据库连接已关闭。")

    def get_parameter_value(self, parameter_name):
        cursor = self.pg_conn.cursor()
        cursor.execute(f"SHOW {parameter_name};")
        value = cursor.fetchone()[0]
        cursor.close()
        return value

    def set_parameter_value(self, parameter_name, value):
        cursor = self.pg_conn.cursor()
        try:
            # 尝试设置参数
            cursor.execute(f"SET {parameter_name} = %s;", (value,))
            self.pg_conn.commit()
            print(f"参数 {parameter_name} 已设置为 {value}")

            # 验证参数是否正确设置
            cursor.execute(f"SHOW {parameter_name};")
            actual_value = cursor.fetchone()[0]

            # 对于数值型参数，进行浮点数比较
            if parameter_name in ['random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost']:
                set_value = float(value)
                actual_float = float(actual_value)
                if abs(actual_float - set_value) < 1e-6:
                    print(f"参数 {parameter_name} 已成功设置为 {actual_value}")
                else:
                    print(f"警告: {parameter_name} 设置为 {actual_value}，而不是 {value}")
                    print(f"差异: {abs(actual_float - set_value)}")
            else:
                if actual_value != value:
                    print(
                        f"警告: 无法完全设置 {parameter_name}。设置值: {value}, 当前值: {actual_value}")
                    print(f"可能是由于四舍五入、不足权限或服务器级别限制造成的。")
        except psycopg2.Error as e:
            print(f"设置 {parameter_name} 时出错: {e}")
        finally:
            cursor.close()

    def create_sqlite_tables(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                work_mem TEXT,
                effective_cache_size TEXT,
                random_page_cost REAL,
                execution_time REAL,
                test_name TEXT
            )
        ''')
        self.sqlite_conn.commit()
        cursor.close()
        print("SQLite 表已成功创建。")

    def clear_sqlite_data(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute("DELETE FROM performance_tests")
        self.sqlite_conn.commit()
        cursor.close()
        print("SQLite 数据已成功清除。")

class BayesianOptimizer:
    def __init__(self, tuner, test_suite):
        self.tuner = tuner
        self.test_suite = test_suite

    def objective_function(self, params):
        work_mem, effective_cache_size, random_page_cost = params[0]
        self.tuner.set_parameter_value('work_mem', f"{int(work_mem)}MB")
        self.tuner.set_parameter_value('effective_cache_size', f"{int(effective_cache_size)}MB")
        self.tuner.set_parameter_value('random_page_cost', str(random_page_cost))

        # 运行增强测试套件
        results = self.test_suite.run_enhanced_test_suite()

        # 将结果保存到数据库
        self.test_suite.save_results(results, work_mem, effective_cache_size, random_page_cost)

        # 返回总执行时间作为优化的目标函数值
        return np.sum(list(results.values()))

    def optimize(self, max_iter=10):
        bounds = [{'name': 'work_mem', 'type': 'continuous', 'domain': (4, 64)},
                  {'name': 'effective_cache_size', 'type': 'continuous', 'domain': (100, 1000)},
                  {'name': 'random_page_cost', 'type': 'continuous', 'domain': (1.0, 4.0)}]

        optimizer = GPyOpt.methods.BayesianOptimization(f=self.objective_function, domain=bounds)
        optimizer.run_optimization(max_iter=max_iter)

        return optimizer.x_opt, optimizer.fx_opt


if __name__ == "__main__":
    tuner = PostgreSQLTuner()
    tuner.connect_postgresql()
    tuner.connect_sqlite()
    tuner.create_sqlite_tables()
    tuner.clear_sqlite_data()

    # 示例用法
    print("当前 work_mem:", tuner.get_parameter_value("work_mem"))
    tuner.set_parameter_value("work_mem", "8MB")
    print("更新后的 work_mem:", tuner.get_parameter_value("work_mem"))

    print("当前 effective_cache_size:", tuner.get_parameter_value("effective_cache_size"))
    tuner.set_parameter_value("effective_cache_size", "1GB")
    print("更新后的 effective_cache_size:", tuner.get_parameter_value("effective_cache_size"))

    print("当前 random_page_cost:", tuner.get_parameter_value("random_page_cost"))
    tuner.set_parameter_value("random_page_cost", "2.0")
    print("更新后的 random_page_cost:", tuner.get_parameter_value("random_page_cost"))

    tuner.close_connections()
