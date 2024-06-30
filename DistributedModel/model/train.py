
from tqdm import tqdm


def train(model, device, optimizer, criterion, num_epochs, epoch, train_loader, file):
    model.train()
    running_loss = 0.0
    correct = 0
    total = 0
    for inputs, labels in tqdm(train_loader):
        inputs, labels = inputs.to(device), labels.to(device)
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
        running_loss += loss.item()
        _, predicted = outputs.max(1)
        total += labels.size(0)
        correct += predicted.eq(labels).sum().item()

    train_acc = 100. * correct / total
    print(f'Epoch [{epoch}/{num_epochs}], Train Acc: {train_acc:.2f}%')
    file.write(f'Epoch [{epoch}/{num_epochs}], Train Acc: {train_acc:.2f}%, ')
