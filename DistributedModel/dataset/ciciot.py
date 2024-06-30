import torch
from torch.utils.data import DataLoader, TensorDataset
from joblib import load

from const.const import X_columns, class_labels_Num

scaler = load("/home/touna/Downloads/project/scaler.joblib")
# Function to encode label


def encode_label(label):
    return class_labels_Num[label]
# Function to normalize a single row


def normalize_row(row):
    # Convert row to dictionary for easy access
    row_dict = row.asDict()

    # Extract values for X_columns and scale them
    scaled_values = scaler.transform([[row_dict[col] for col in X_columns]])

    # Replace original values with scaled values in row_dict
    for i, col in enumerate(X_columns):
        row_dict[col] = scaled_values[0][i]

    return row_dict


def preprocess(row):
    row = normalize_row(row)

    inputs = torch.tensor([row[k] for k in X_columns], dtype=torch.float32)
    # Encode label
    label = row['label']  # Assuming 'label' is the column name
    encoded_label = encode_label(label)
    outputs = torch.tensor(encoded_label, dtype=torch.long)
    return inputs, outputs
