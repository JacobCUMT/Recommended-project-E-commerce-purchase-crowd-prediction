import os
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset, DataLoader
import torch
from torch.nn.utils.rnn import pad_sequence
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.tensorboard import SummaryWriter

class MyDataset(Dataset):
    def __init__(self, features, labels, config):
        self.config = config
        self.features = features
        #self.features = {}
        #for ff in self.config["features_col"]:
        #    self.features[ff] = [torch.tensor([int(id) for id in seq.split(',')], dtype=torch.long) for seq in features[ff]]
        self.labels = torch.tensor(labels, dtype=torch.float32)
        
    def __len__(self):
        return len(self.labels)
    def __getitem__(self, idx):
        res_features = {}
        for ff in self.config["features_col"]:
            res_features[ff] = torch.tensor([int(id) for id in self.features[ff][idx].split(',')])
        res_features['label'] = self.labels[idx]
        return res_features