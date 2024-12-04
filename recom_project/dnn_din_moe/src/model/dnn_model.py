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


class MyModel(nn.Module):
    def __init__(self, config):
        super(MyModel, self).__init__()
        self.config = config
        self.embedding = nn.Embedding(num_embeddings=self.config["num_embeddings"], embedding_dim=self.config["embedding_dim"], padding_idx=0)
        self.fc1 = nn.Linear(self.config["embedding_dim"]*len(self.config["features_col"]), 512)
        self.fc2 = nn.Linear(512, 128)
        self.fc3 = nn.Linear(128, 1)
        
    def forward(self, features):
        embedding_dict = {}
        for ff in self.config["features_col"]:
            embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)
        x = torch.cat([embedding_dict[ff] for ff in self.config["features_col"]], dim=1)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x