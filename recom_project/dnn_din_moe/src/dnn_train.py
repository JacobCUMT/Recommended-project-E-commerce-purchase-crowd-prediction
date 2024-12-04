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
from utils.get_data import get_data
from utils.get_data import my_collate_fn


train_feature_numpy, test_feature_numpy, train_label, test_label = get_data(ak_config)

dataset_train = MyDataset(train_feature_numpy, train_label, dnn_config)
dataloader_train = DataLoader(dataset_train, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)

dataset_test = MyDataset(test_feature_numpy, test_label, dnn_config)
dataloader_test = DataLoader(dataset_test, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)

model = MyModel(dnn_config)
criterion = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.004)
log_dir = './runs_dnn'
writer = SummaryWriter(log_dir)

def train_model(train_loader, test_loader, model, criterion, optimizer, num_epochs=3):
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
                        test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())    #思考为什么要to('cpu')
                        test_targets.extend(target.squeeze().tolist())
                    test_auc = roc_auc_score(test_targets, test_preds)
                    writer.add_scalar('AUC/train', test_auc, total_step)
                torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}_{total_step}.pth') 
                model.train()
        torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}.pth')   
                
    with torch.no_grad():
        model.eval()
        test_preds = []
        test_targets = []
        for data, target in test_loader:
            output = model(data)
            test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())
            test_targets.extend(target.squeeze().tolist())
        test_auc = roc_auc_score(test_targets, test_preds)
        writer.add_scalar('AUC/train', test_auc, epoch)
        
train_model(dataloader_train, dataloader_test, model, criterion, optimizer, num_epochs=10)
writer.close()