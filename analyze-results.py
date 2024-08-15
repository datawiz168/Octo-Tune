import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from configparser import ConfigParser
import numpy as np
from pandas.plotting import parallel_coordinates


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


def preprocess_columns(df):
    # 将 'work_mem' 和 'effective_cache_size' 列转换为数值
    df['work_mem'] = df['work_mem'].str.replace('MB', '').astype(int)
    df['effective_cache_size'] = df['effective_cache_size'].str.replace('MB', '').astype(int)
    return df


def create_boxplot(df):
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='test_name', y='execution_time', hue='work_mem', data=df)
    plt.title('Execution Time Distribution by Test Name and Work Mem')
    plt.savefig(os.path.join('pic', 'boxplot_execution_time.png'))
    plt.close()


def create_heatmaps_with_three_params(df):
    # 计算平均执行时间的聚合数据
    aggregated_mean = df.groupby(['work_mem', 'effective_cache_size', 'random_page_cost']).agg(
        {'execution_time': 'mean'}).reset_index()

    # 计算95th百分位执行时间的聚合数据
    aggregated_95th = df.groupby(['work_mem', 'effective_cache_size', 'random_page_cost']).agg(
        {'execution_time': lambda x: np.percentile(x, 95)}).reset_index()

    # 将 'work_mem' 和 'effective_cache_size' 转换为数值
    aggregated_mean['work_mem'] = aggregated_mean['work_mem'].str.replace('MB', '').astype(int)
    aggregated_mean['effective_cache_size'] = aggregated_mean['effective_cache_size'].str.replace('MB', '').astype(int)
    aggregated_95th['work_mem'] = aggregated_95th['work_mem'].str.replace('MB', '').astype(int)
    aggregated_95th['effective_cache_size'] = aggregated_95th['effective_cache_size'].str.replace('MB', '').astype(int)

    # 创建第一个热力图 - 显示平均执行时间
    pivot_mean = aggregated_mean.pivot_table(values='execution_time', index='work_mem',
                                             columns=['effective_cache_size', 'random_page_cost'])
    plt.figure(figsize=(14, 10))
    sns.heatmap(pivot_mean, annot=True, fmt=".2f", cmap='coolwarm', cbar_kws={'label': 'Mean Execution Time (s)'})
    plt.title('Heatmap of Mean Execution Time by Work Mem, Effective Cache Size, and Random Page Cost')
    plt.xlabel('Effective Cache Size (MB) / Random Page Cost')
    plt.ylabel('Work Mem (MB)')
    plt.tight_layout()
    plt.savefig(os.path.join('pic', 'heatmap_mean_execution_time.png'))
    plt.close()

    # 创建第二个热力图 - 显示95th百分位执行时间
    pivot_95th = aggregated_95th.pivot_table(values='execution_time', index='work_mem',
                                             columns=['effective_cache_size', 'random_page_cost'])
    plt.figure(figsize=(14, 10))
    sns.heatmap(pivot_95th, annot=True, fmt=".2f", cmap='coolwarm',
                cbar_kws={'label': '95th Percentile Execution Time (s)'})
    plt.title('Heatmap of 95th Percentile Execution Time by Work Mem, Effective Cache Size, and Random Page Cost')
    plt.xlabel('Effective Cache Size (MB) / Random Page Cost')
    plt.ylabel('Work Mem (MB)')
    plt.tight_layout()
    plt.savefig(os.path.join('pic', 'heatmap_95th_execution_time.png'))
    plt.close()


def create_time_series_plot(df):
    plt.figure(figsize=(14, 7))
    for test_name in df['test_name'].unique():
        subset = df[df['test_name'] == test_name]
        plt.plot(subset['timestamp'], subset['execution_time'], label=test_name)
    plt.xticks(rotation=45)
    plt.title('Execution Time Over Time')
    plt.legend()
    plt.savefig(os.path.join('pic', 'time_series_execution_time.png'))
    plt.close()


def create_parallel_coordinates_plot(df):
    # 将 'work_mem' 和 'effective_cache_size' 列转换为数值
    df = preprocess_columns(df)

    # 转换为字符串类型以进行绘图
    df['work_mem'] = df['work_mem'].astype(str)
    df['effective_cache_size'] = df['effective_cache_size'].astype(str)
    df['random_page_cost'] = df['random_page_cost'].astype(str)
    df['execution_time'] = df['execution_time'].astype(str)

    plt.figure(figsize=(12, 6))
    parallel_coordinates(df[['test_name', 'work_mem', 'effective_cache_size', 'random_page_cost', 'execution_time']],
                         'test_name', colormap='viridis')
    plt.title('Parallel Coordinates Plot for Test Parameters')
    plt.savefig(os.path.join('pic', 'parallel_coordinates_plot.png'))
    plt.close()


def create_correlation_matrix(df):
    df = preprocess_columns(df)  # 预处理列，转换为数值类型
    plt.figure(figsize=(10, 8))
    corr = df[['execution_time', 'work_mem', 'effective_cache_size', 'random_page_cost']].corr()
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
    create_heatmaps_with_three_params(df)
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
