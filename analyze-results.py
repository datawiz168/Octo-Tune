import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from postgresql_tuner import PostgreSQLTuner

def load_data_from_sqlite():
    tuner = PostgreSQLTuner()
    conn = sqlite3.connect(tuner.config['sqlite']['database'])
    query = "SELECT * FROM performance_tests ORDER BY timestamp DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def analyze_and_visualize(df):
    # 转换参数为数值型
    df['work_mem_mb'] = df['work_mem'].str.extract('(\d+)').astype(int)
    df['effective_cache_size_gb'] = df['effective_cache_size'].str.extract('(\d+)').astype(int)
    df['random_page_cost'] = df['random_page_cost'].astype(float)

    # 创建热力图
    plt.figure(figsize=(15, 10))
    for i, test_name in enumerate(df['test_name'].unique()):
        plt.subplot(2, 2, i+1)
        pivot = df[df['test_name'] == test_name].pivot_table(
            values='execution_time', 
            index='work_mem_mb', 
            columns='effective_cache_size_gb', 
            aggfunc='mean'
        )
        sns.heatmap(pivot, annot=True, fmt='.2f', cmap='YlOrRd')
        plt.title(f'{test_name} - Execution Time (seconds)')
        plt.xlabel('Effective Cache Size (GB)')
        plt.ylabel('Work Mem (MB)')

    plt.tight_layout()
    plt.savefig('performance_heatmap.png')
    print("Performance heatmap saved as 'performance_heatmap.png'")

    # 显示基本统计信息
    print("\nBasic Statistics:")
    print(df.groupby('test_name')['execution_time'].describe())

    # 显示每个参数组合的平均执行时间
    print("\nAverage Execution Time for each parameter combination:")
    avg_times = df.groupby(['work_mem', 'effective_cache_size', 'random_page_cost', 'test_name'])['execution_time'].mean().unstack()
    print(avg_times)

if __name__ == "__main__":
    df = load_data_from_sqlite()
    analyze_and_visualize(df)