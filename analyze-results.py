import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from configparser import ConfigParser
import numpy as np
from pandas.plotting import parallel_coordinates
import matplotlib.pyplot as plt
from matplotlib import font_manager

# 指定中文字体（以 SimHei 为例）
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号无法正常显示的问题

def read_config(filename='database.ini'):
    parser = ConfigParser()
    parser.read(filename)
    return {section: dict(parser.items(section)) for section in parser.sections()}


def load_data_from_sqlite():
    config = read_config()
    db_path = config['sqlite']['database']

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file '{db_path}' not found. Please run performance-test-suite.py first.")

    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM performance_tests ORDER BY timestamp DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def preprocess_data(df):
    # 分离并发查询和其他查询
    concurrent_queries = df[df['test_name'].str.contains('Concurrent Query')]
    other_queries = df[~df['test_name'].str.contains('Concurrent Query')]

    # 计算并发查询的平均执行时间
    concurrent_avg = concurrent_queries.groupby(['work_mem', 'effective_cache_size', 'random_page_cost', 'timestamp'])[
        'execution_time'].mean().reset_index()
    concurrent_avg['test_name'] = 'Concurrent Queries (Avg)'

    # 合并处理后的数据
    df_processed = pd.concat([other_queries, concurrent_avg])

    # 处理 'work_mem' 和 'effective_cache_size' 字段
    df_processed['work_mem'] = df_processed['work_mem'].str.replace('MB', '').astype(float).round().astype(int)
    df_processed['effective_cache_size'] = df_processed['effective_cache_size'].str.replace('MB', '').astype(float).round().astype(int)

    return df_processed



def create_boxplot(df):
    df_processed = preprocess_data(df)
    plt.figure(figsize=(16, 8))
    sns.boxplot(x='test_name', y='execution_time', hue='work_mem', data=df_processed)
    plt.title('Execution Time Distribution by Test Name and Work Mem')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title='work_mem', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.savefig(os.path.join('pic', 'boxplot_execution_time.png'), dpi=300, bbox_inches='tight')
    plt.close()


def create_improved_heatmaps(df):
    df_processed = preprocess_data(df)

    # 计算平均执行时间和95th百分位数
    aggregated = df_processed.groupby(['work_mem', 'effective_cache_size', 'random_page_cost']).agg({
        'execution_time': ['mean', lambda x: np.percentile(x, 95)]
    }).reset_index()
    aggregated.columns = ['work_mem', 'effective_cache_size', 'random_page_cost', 'mean_time', '95th_percentile']

    # 创建平均执行时间的热图
    create_single_heatmap(aggregated, 'mean_time', 'Mean Execution Time', 'heatmap_mean_execution_time.png')

    # 创建95th百分位执行时间的热图
    create_single_heatmap(aggregated, '95th_percentile', '95th Percentile Execution Time',
                          'heatmap_95th_execution_time.png')


def create_single_heatmap(data, value_column, title, filename):
    # 四舍五入到两位小数
    data['work_mem'] = data['work_mem'].round(2)
    data['effective_cache_size'] = data['effective_cache_size'].round(2)
    data['random_page_cost'] = data['random_page_cost'].round(2)

    # 创建数据透视表
    pivot = data.pivot_table(values=value_column,
                             index='work_mem',
                             columns=['effective_cache_size', 'random_page_cost'])

    # 创建热图
    plt.figure(figsize=(14, 10))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap='coolwarm', cbar_kws={'label': 'Execution Time (s)'})
    plt.title(f'Heatmap of {title} by Work Mem, Effective Cache Size, and Random Page Cost')
    plt.xlabel('Effective Cache Size (MB) / Random Page Cost')
    plt.ylabel('Work Mem (MB)')
    plt.tight_layout()
    plt.savefig(os.path.join('pic', filename), dpi=300, bbox_inches='tight')
    plt.close()


def create_time_series_plot(df):
    df_processed = preprocess_data(df)
    plt.figure(figsize=(14, 7))
    for test_name in df_processed['test_name'].unique():
        subset = df_processed[df_processed['test_name'] == test_name]
        plt.plot(subset['timestamp'], subset['execution_time'], label=test_name)
    plt.xticks(rotation=45)
    plt.title('Execution Time Over Time')
    plt.legend()
    plt.savefig(os.path.join('pic', 'time_series_execution_time.png'))
    plt.close()


def create_parallel_coordinates_plot(df):
    df_processed = preprocess_data(df)
    df_processed['work_mem'] = df_processed['work_mem'].astype(str)
    df_processed['effective_cache_size'] = df_processed['effective_cache_size'].astype(str)
    df_processed['random_page_cost'] = df_processed['random_page_cost'].astype(str)
    df_processed['execution_time'] = df_processed['execution_time'].astype(str)

    plt.figure(figsize=(12, 6))
    parallel_coordinates(
        df_processed[['test_name', 'work_mem', 'effective_cache_size', 'random_page_cost', 'execution_time']],
        'test_name', colormap='viridis')
    plt.title('Parallel Coordinates Plot for Test Parameters')
    plt.savefig(os.path.join('pic', 'parallel_coordinates_plot.png'))
    plt.close()


def create_correlation_matrix(df):
    df_processed = preprocess_data(df)
    plt.figure(figsize=(10, 8))
    corr = df_processed[['execution_time', 'work_mem', 'effective_cache_size', 'random_page_cost']].corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Correlation Matrix of Parameters')
    plt.savefig(os.path.join('pic', 'correlation_matrix.png'))
    plt.close()


def analyze_and_visualize(df):
    print("Data shape:", df.shape)
    print("\nData types:")
    print(df.dtypes)
    print("\nData head:")
    print(df.head())

    if not os.path.exists('pic'):
        os.makedirs('pic')

    create_boxplot(df)
    create_improved_heatmaps(df)
    create_time_series_plot(df)
    create_parallel_coordinates_plot(df)
    create_correlation_matrix(df)


if __name__ == "__main__":
    try:
        df = load_data_from_sqlite()
        analyze_and_visualize(df)
        print("Analysis complete. All images have been saved in the 'pic' folder.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback

        traceback.print_exc()
        print(
            "Please ensure that you have run performance-test-suite.py to create the database and insert data before running this analysis script.")
