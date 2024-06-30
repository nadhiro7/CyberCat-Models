import torch
import torch.nn as nn
import torch.optim as optim
from dataset.ciciot import preprocess
from model.lstm import LSTMModel
from model.train import train
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import time
from torch.utils.data import DataLoader, TensorDataset


# Initialize Spark
conf = SparkConf().setAppName("DistributedLSTM").setMaster("spark://ailab-uc2:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
input_size = 1
hidden_size = 64
num_layers = 2
batch_size = 64
learning_rate = 0.001
num_epochs = 15


# Load data from HDFS
data_path = "hdfs://127.0.0.1:9000/alldata/train.csv"
data = spark.read.csv(data_path, header=True, inferSchema=True)


rdd = data.rdd.map(preprocess).coalesce(2)


def train_on_partition(index, iterator):
    model = LSTMModel(input_size, hidden_size, num_layers, 34).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    file = open('/home/touna/Downloads/results/lstm_model_34_' +
                str(index)+'results.txt', 'w')
    file.write('LSTM Model_34_ Results'+str(index)+': \n')
    # print(str(index))
    data = list(iterator)
    if not data:
        return []

    inputs, outputs = zip(*data)
    dataset = TensorDataset(torch.stack(inputs), torch.tensor(outputs))
    train_loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

    for epoch in range(1, num_epochs + 1):
        start_time = time.time()
        train(model, device, optimizer, criterion,
              num_epochs, epoch, train_loader, file)
        end_time = time.time()
        epoch_time = end_time - start_time
        file.write(f"Epoch {epoch} took {epoch_time:.2f} seconds. \n")

    torch.save(model.state_dict(),
               '/home/touna/Downloads/results/lstm_model_34_'+str(index)+'.pth')
    file.close()
    return [model.state_dict()]


models = rdd.mapPartitionsWithIndex(train_on_partition).collect()


def average_models(models):
    avg_model = LSTMModel(input_size, hidden_size, num_layers, 34).to(device)
    avg_state_dict = avg_model.state_dict()
    for key in avg_state_dict.keys():
        avg_state_dict[key] = torch.stack(
            [model[key] for model in models]).mean(dim=0)
    avg_model.load_state_dict(avg_state_dict)
    return avg_model


final_model = average_models(models)


torch.save(final_model.state_dict(),
           "/home/touna/Downloads/results/final2_lstm_model.pth")

print("Training complete and model saved.")
