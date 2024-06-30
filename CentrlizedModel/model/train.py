from tqdm import tqdm
from dataset.ciciot import load_data, read_dataset
import time

def train(model, device, optimizer, criterion, num_epochs, epoch, files, type, file):
    model.train()
    running_loss = 0.0
    correct = 0
    total = 0
    train_len = 0
    start_time = time.time()
    for filename in tqdm(files):
        df = read_dataset(filename, type)
        train_loader = load_data(df)
        train_len += len(train_loader)
        for inputs, labels in train_loader:
            inputs = inputs.to(device)
            labels = labels.to(device)
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            _, predicted = outputs.max(1)
            total += labels.size(0)
            correct += predicted.eq(labels).sum().item()

    train_loss = running_loss / train_len
    train_acc = 100. * correct / total

    print(
        f'Epoch [{epoch}/{num_epochs}], Train Loss: {train_loss:.4f}, Train Acc: {train_acc:.2f}%')
    file.write(
        f'Epoch [{epoch}/{num_epochs}], Train Loss: {train_loss:.4f}, Train Acc: {train_acc:.2f}%, ')
    end_time = time.time()
    epoch_time = end_time - start_time
    file.write(f"train {epoch} took {epoch_time:.2f} seconds. \n")
