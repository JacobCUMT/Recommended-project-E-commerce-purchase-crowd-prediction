import os
import lightgbm as lgb
import pandas as pd
import numpy as np
from odps import ODPS
from odps.df import DataFrame
import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

project_name = ''
access_id = ''
access_key = ''
end_point = ''

o = ODPS(
    access_id,
    access_key,
    project_name,
    end_point,
)

brand_id = 'b47686'
# trees = '15'


# 定义分批加载和预测函数
def batch_predict(model_path):
    """
    分批预测函数
    :param model_path: 用于加载 LightGBM 模型的文件
    :return: 预测分数和标签的列表
    """
    # 加载模型
    model = lgb.Booster(model_file=model_path)
    predictions = []
    labels = []

    for num in range(10):
        # 读取数据。
        sql = '''
        SELECT *
        FROM tmall_rec_project.user_pay_sample_feature_join_eval
        WHERE ds='20130923' and brand_id='{brand}' and rnd>{start} and rnd<={end}
        ;
        '''.format(brand=brand_id, start=num/10.0, end=(num+1)/10.0)

        print(sql)

        query_job = o.execute_sql(sql)
        result = query_job.open_reader(tunnel=True)
        df = result.to_pandas(n_process=32)  # n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
        print('read data finish')

        # 特征列和标签列
        non_feature_cols = ['user_id', 'brand_id', 'label', 'bizdate', 'rnd', 'ds']
        features = [col for col in df.columns if col not in non_feature_cols]
        X_test = df[features]
        y_test = df['label']

        y_pred = model.predict(X_test)
        predictions.extend(y_pred)
        labels.extend(y_test)

    return predictions, labels


# 计算top范围内label=1的比例
def calculate_top_ratios(predictions, labels, tops):
    """
    计算 top 范围内 label=1 的比例
    :param predictions: 预测分数列表
    :param labels: 标签列表
    :param tops: 一个包含 top 范围的列表
    :return: 一个字典，key 为 top 范围，value 为比例
    """
    # 将预测和标签转换为DataFrame
    results_df = pd.DataFrame({'prediction': predictions, 'label': labels})

    # 按预测分数降序排序
    results_df = results_df.sort_values(by='prediction', ascending=False)

    # 计算总正例数
    total_positives = (results_df['label'] == 1).sum()

    top_ratios = {}
    for top in tops:
        top_k_df = results_df.head(top)
        top_k_positives = (top_k_df['label'] == 1).sum()
        ratio = top_k_positives / total_positives
        top_ratios[top] = ratio

    return top_ratios


brand_model = 'b47686_b56508'
trees = '70'

model_path = f'./models_{brand_model}/lgb_model_{trees}_trees.txt'
tops = [1000, 3000, 5000, 10000, 50000]  # top 范围

# 分批预测
predictions, labels = batch_predict(model_path)

# 计算 top 比例
top_ratios = calculate_top_ratios(predictions, labels, tops)

# 输出结果
for top, ratio in top_ratios.items():
    print(f'Top {top} ratio of positive labels: {ratio:.4f}\n')

with open(f'models_{brand_model}/top_results_{trees}_trees_{brand_id}.txt', 'w') as f:
    for top, ratio in top_ratios.items():
        f.write(f'Top {top} ratio of positive labels: {ratio:.4f}\n')