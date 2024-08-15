import psycopg2
import sqlite3
from configparser import ConfigParser


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
            print("Connected to PostgreSQL database!")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def connect_sqlite(self):
        try:
            self.sqlite_conn = sqlite3.connect(self.config['sqlite']['database'])
            print("Connected to SQLite database!")
        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        print("Database connections closed.")

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
            print(f"Parameter {parameter_name} set to {value}")

            # 验证参数是否正确设置
            cursor.execute(f"SHOW {parameter_name};")
            actual_value = cursor.fetchone()[0]

            # 对于数值型参数，进行浮点数比较
            if parameter_name in ['random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost']:
                # 将值转换为浮点数进行比较
                set_value = float(value)
                actual_float = float(actual_value)
                if abs(actual_float - set_value) < 1e-6:
                    print(f"Parameter {parameter_name} successfully set to {actual_value}")
                else:
                    print(f"Warning: {parameter_name} set to {actual_value}, not exactly {value}")
                    print(f"Difference: {abs(actual_float - set_value)}")
            else:
                if actual_value != value:
                    print(
                        f"Warning: Failed to set {parameter_name} exactly. Set value: {value}, Current value: {actual_value}")
                    print(f"This might be due to rounding, insufficient privileges, or server-level restrictions.")
        except psycopg2.Error as e:
            print(f"Error setting {parameter_name}: {e}")
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
        print("SQLite tables created successfully.")

    def clear_sqlite_data(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute("DELETE FROM performance_tests")
        self.sqlite_conn.commit()
        cursor.close()
        print("SQLite data cleared successfully.")


if __name__ == "__main__":
    tuner = PostgreSQLTuner()
    tuner.connect_postgresql()
    tuner.connect_sqlite()
    tuner.create_sqlite_tables()
    tuner.clear_sqlite_data()

    # 示例用法
    print("Current work_mem:", tuner.get_parameter_value("work_mem"))
    tuner.set_parameter_value("work_mem", "8MB")
    print("Updated work_mem:", tuner.get_parameter_value("work_mem"))

    print("Current effective_cache_size:", tuner.get_parameter_value("effective_cache_size"))
    tuner.set_parameter_value("effective_cache_size", "1GB")
    print("Updated effective_cache_size:", tuner.get_parameter_value("effective_cache_size"))

    print("Current random_page_cost:", tuner.get_parameter_value("random_page_cost"))
    tuner.set_parameter_value("random_page_cost", "2.0")
    print("Updated random_page_cost:", tuner.get_parameter_value("random_page_cost"))

    tuner.close_connections()
