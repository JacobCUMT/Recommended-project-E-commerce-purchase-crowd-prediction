# 该文件为代码结构化前的dnn模型训练文件，可用于理解项目大体结构
# 结构化后代码以及din、moe代码见src

import os
import numpy as np
import pandas as pd
from odps import ODPS
from odps.df import DataFrame
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset, DataLoader
import torch
from torch.nn.utils.rnn import pad_sequence
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.tensorboard import SummaryWriter

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
sql = '''
SELECT *
FROM tmall_rec_project.user_pay_sample_feature_join_dnn_seq_shuffle
limit 10000
;
'''

print(sql)

query_job = o.execute_sql(sql)
result = query_job.open_reader(tunnel=True)
df = result.to_pandas(n_process=4)  # n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
print('read data finish')

# 特征列和标签列
non_feature_cols = ['key_all']
features = [col for col in df.columns if col not in non_feature_cols]
X = df[features]
y = df['label']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

features_col = ["target_brand_id", "clk_brand_seq", "clt_brand_seq", "pay_brand_seq", "clk_cate_seq", "clt_cate_seq", "pay_cate_seq", "user_click_feature",
                "user_clt_feature", "user_cart_feature", "user_pay_feature", "brand_stat_feature", "user_cate2_cross_feature", "user_brand_cross_feature"]
train_feature_numpy = {}
test_feature_numpy = {}
for feature in features_col:
    train_feature_numpy[feature] = X_train[feature].values
    test_feature_numpy[feature] = X_test[feature].values
train_label = y_train.values
test_label = y_test.values

class MyDataset(Dataset):
    def __init__(self, features, labels):
        self.features = {}
        for ff in features_col:
            self.features[ff] = [torch.tensor([int(id) for id in seq.split(',')], dtype=torch.long) for seq in features[ff]]
        self.labels = torch.tensor(labels, dtype=torch.float32)
        
    def __len__(self):
        return len(self.labels)
    def __getitem__(self, idx):
        res_features = {}
        for ff in features_col:
            res_features[ff] = self.features[ff][idx]
        res_features['label'] = self.labels[idx]
        return res_features


class MyModel(nn.Module):
    def __init__(self, embedding_dim):
        super(MyModel, self).__init__()
        self.embedding = nn.Embedding(num_embeddings=20000, embedding_dim=embedding_dim, padding_idx=0)
        self.fc1 = nn.Linear(embedding_dim*len(features_col), 512)
        self.fc2 = nn.Linear(512, 128)
        self.fc3 = nn.Linear(128, 1)
        
    def forward(self, features):
        embedding_dict = {}
        for ff in features_col:
            embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)
        x = torch.cat([embedding_dict[ff] for ff in features_col], dim=1)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x
    
def my_collate_fn(batch):
    res_features_tmp = {}
    labels = []
    for ff in features_col:
        res_features_tmp[ff] = []
    for sample in batch:
        for ff in features_col:
            res_features_tmp[ff].append(sample[ff])
        labels.append(sample["label"])
    res_feature = {}
    for ff in features_col:
        res_feature[ff] = pad_sequence(res_features_tmp[ff], batch_first=True, padding_value=0)
    return res_feature, torch.tensor(labels)
        

dataset_train = MyDataset(train_feature_numpy, train_label)
dataloader_train = DataLoader(dataset_train, batch_size=512, shuffle=False, collate_fn=my_collate_fn)

dataset_test = MyDataset(test_feature_numpy, test_label)
dataloader_test = DataLoader(dataset_test, batch_size=512, shuffle=False, collate_fn=my_collate_fn)

model = MyModel(32)
criterion = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
writer = SummaryWriter()

def train_model(train_loader, test_loader, model, criterion, optimizer, num_epochs=10):
    total_step = 0
    for epoch in range(num_epochs):
        model.train()
        for features, labels in train_loader:
            optimizer.zero_grad()
            outputs = model(features)
            labels = torch.unsqueeze(labels, 1)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_step += 1
            if (total_step + 1) % 10 == 0:
                writer.add_scalar('Training Loss', loss.item(), total_step)
            if (total_step + 1) % 100 == 0:
                print(f'Epoch {epoch}: Loss: {loss.item():.4f}')
            if (total_step + 1) % 2000 == 0:
                with torch.no_grad():
                    model.eval()
                    test_preds = []
                    test_targets = []
                    for data, target in test_loader:
                        output = model(data)
                        test_preds.extend(output.sigmoid().squeeze().tolist())
                        test_targets.extend(target.squeeze().tolist())
                    test_auc = roc_auc_score(test_targets, test_preds)
                    writer.add_scalar('AUC/test', test_auc, total_step)
                torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}_{total_step}.pth') 
                model.train()
        torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}.pth')   
                
    with torch.no_grad():
        model.eval()
        test_preds = []
        test_targets = []
        for data, target in test_loader:
            output = model(data)
            test_preds.extend(output.sigmoid().squeeze().tolist())
            test_targets.extend(target.squeeze().tolist())
        test_auc = roc_auc_score(test_targets, test_preds)
        writer.add_scalar('AUC/test', test_auc, epoch)
        
train_model(dataloader_train, dataloader_test, model, criterion, optimizer, num_epochs=10)
writer.close()