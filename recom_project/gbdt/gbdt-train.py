import os
import numpy as np
import pandas as pd
from odps import ODPS
from odps.df import DataFrame
import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

# 建立链接。
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

# 读取数据。
brand_id = 'b47686'
brand_id_2 = 'b78739'

sql = '''
SELECT *
FROM tmall_rec_project.user_pay_sample_feature_join
WHERE ds>='20130701' and ds<='20130916' and brand_id in ('{brand}', '{brand2}')
;
'''.format(brand=brand_id, brand2=brand_id_2)

print(sql)

query_job = o.execute_sql(sql)
result = query_job.open_reader(tunnel=True)
df = result.to_pandas(n_process=4)  # n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
print('read data finish')

# 特征列和标签列
non_feature_cols = ['user_id', 'brand_id', 'label', 'bizdate', 'rnd', 'ds']
features = [col for col in df.columns if col not in non_feature_cols]
X = df[features]
y = df['label']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 转换为LightGBM数据集
train_data = lgb.Dataset(X_train, label=y_train)
test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)
print('post process finish')

# 参数设置
params = {
    'objective': 'binary',
    'metric': 'auc',
    'boosting_type': 'gbdt',
    'learning_rate': 0.08,
    'num_leaves': 31,
    'max_depth': -1,
    'min_data_in_leaf': 20,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'min_gain_to_split': 0.01,
    'lambda_l1': 0.1,
    'lambda_l2': 0.1,
    'seed': 42,
    'verbosity': -1
}

# 创建 '品牌id' 文件夹（如果不存在）
output_dir = "models_{brand}_{brand2}".format(brand=brand_id, brand2=brand_id_2)
os.makedirs(output_dir, exist_ok=True)

# 保存AUC值的文件
auc_file = os.path.join(output_dir, "auc_results.txt")
with open(auc_file, "w") as f:
    f.write("n_trees\ttrain_auc\ttest_auc\n")

# 训练模型并保存每隔5棵树的模型
for num_trees in range(40, 101, 5):
    # 训练模型
    model = lgb.train(
        params,
        train_data,
        num_boost_round=num_trees,  # 使用函数参数控制树的数量
        valid_sets=[test_data],
    )

    # 预测训练集和测试集
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)

    # 计算AUC
    train_auc = roc_auc_score(y_train, train_pred)
    test_auc = roc_auc_score(y_test, test_pred)

    # 保存AUC值
    with open(auc_file, "a") as f:
        f.write(f"{num_trees}\t{train_auc:.6f}\t{test_auc:.6f}\n")

    # 保存模型到 '品牌id' 文件夹中
    model_path = os.path.join(output_dir, f"lgb_model_{num_trees}_trees.txt")
    model.save_model(model_path)

print(f"AUC values and models have been saved to the {output_dir} directory.")