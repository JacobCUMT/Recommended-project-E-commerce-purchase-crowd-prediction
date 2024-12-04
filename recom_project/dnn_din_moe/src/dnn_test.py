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

from dataset.dnn_dataset import MyDataset
from model.dnn_model import MyModel
from config.dnn_config import config as dnn_config
from config.ak_config import config as ak_config
from utils.get_data import get_data_test
from utils.get_data import calculate_top_k_ratio
from utils.get_data import my_collate_fn

brands = ['b47686','b56508','b62063','b78739']
model_path = './models/dnn/xxx.pth'
log_dir = './runs_dnn'  #可根据具体模型修改该文件夹名，存储top_n指标
for brand_id in brands:
    top_k_list = [1000, 3000, 5000, 10000, 50000]

    test_feature_numpy, test_label = get_data_test(ak_config, brand_id)
    dataset_test = MyDataset(test_feature_numpy, test_label, dnn_config)
    dataloader_test = DataLoader(dataset_test, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)

    model = MyModel(dnn_config)
    model.load_state_dict(torch.load(model_path))
    model.eval()
    test_preds = []
    test_targets = []
    for data, target in dataloader_test:
        output = model(data)
        test_preds.extend(output.sigmoid().squeeze().tolist())
        test_targets.extend(target.squeeze().tolist())

    # 计算top k的正例比例
    ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
    # 输出结果
    for k, ratio in ratios.items():
        print(f"{brand_id} Top {k} ratio of positive labels: {ratio:.4f}")
    # 如果需要保存结果到文件
    with open(f'{log_dir}/models_{brand_id}_top_results_dnn.txt', 'w') as f:
        for k, ratio in ratios.items():
            f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")