import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import torch
from torch.utils.data import DataLoader, TensorDataset
from joblib import load
from const.const import class_labels_Num, class_Binary, class_cats, X_columns, Importance_features


def read_dataset(path, type):
    df = pd.read_csv(path)
    if type == '34':
        df['label'] = df['label'].apply(lambda x: class_labels_Num.get(x, x))
    elif type == '8':
        df['label'] = df['label'].apply(lambda x: class_cats.get(x, x))
    else:
        df['label'] = df['label'].apply(lambda x: class_Binary.get(x, x))
    return df


def normalization_dataset(df):
    scaler = load("./scaler.joblib")
    df[X_columns] = scaler.fit_transform(df[X_columns])
    return df


def load_data(df):
    df = normalization_dataset(df)
    # df[X_columns]
    input = torch.tensor(
        df.drop('label', axis=1).values, dtype=torch.float32)
    output = torch.tensor(df['label'].values, dtype=torch.long)
    del df
    data_set = TensorDataset(input, output)
    del input
    del output
    # Load dataset into DataLoader
    data_loader = DataLoader(data_set, batch_size=64, shuffle=False)
    del data_set
    return data_loader
