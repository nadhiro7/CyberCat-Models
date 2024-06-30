from model.lstm import LSTMModel
import torch.nn as nn
import torch.optim as optim
from model.train import train
from model.test import test
import torch
import os
import time

# Hyperparameters
input_size = 1
hidden_size = 64
num_layers = 2
batch_size = 64
learning_rate = 0.001
num_epochs = 15


DATASET_DIRECTORY = '/home/touna/Downloads/data/CICIoT2023/'
df_sets = [DATASET_DIRECTORY+k for k in os.listdir(DATASET_DIRECTORY) if k.endswith('.csv')]
df_sets.sort()
train_files = df_sets[:int(len(df_sets)*.8)]
test_files = df_sets[int(len(df_sets)*.8):]
classes = [ '34','8', '2']


def main():
    # Initialize the model loss function and optimizer
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    # print(device)
    torch.cuda.is_available()
    print(device)
    for type in classes:
        model = LSTMModel(input_size, hidden_size,
                          num_layers, int(type)).to(device)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=learning_rate)
        file = open('lstm_model_'+type+'results.txt', 'w')
        file.write('LSTM Model'+type+' Results: \n')
        for epoch in range(1, num_epochs + 1):
            start_time = time.time()
            train(model, device, optimizer, criterion,
                  num_epochs, epoch, train_files, type, file)
            test(model, device, criterion, num_epochs,
                 epoch, test_files, type, file)
            torch.save(model.state_dict(), 'lstm_model_' +
                       type+'checkpoint_'+str(epoch)+'.pth')
            end_time = time.time()
            epoch_time = end_time - start_time
            file.write(f"Epoch {epoch} took {epoch_time:.2f} seconds. \n")
        torch.save(model.state_dict(), 'lstm_model_'+type+'.pth')
        file.close()


if __name__ == '__main__':
    main()
