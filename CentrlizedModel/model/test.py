from tqdm import tqdm
from dataset.ciciot import load_data, read_dataset
import torch
from torchmetrics import Accuracy, F1Score, Precision, Recall,AUROC
import time
def test(model, device, criterion, num_epochs, epoch, files, type, file):
    # Validation loop
    model.eval()
    val_loss = 0.0
    correct = 0
    total = 0
    val_len = 0
    # Initialize lists to store true and predicted labels
    all_preds = []
    all_labels = []
    start_time = time.time()
    with torch.no_grad():
        for filename in tqdm(files):
            df = read_dataset(filename, type)
            val_loader = load_data(df)
            val_len += len(val_loader)
            for inputs, labels in val_loader:
                inputs = inputs.to(device)
                labels = labels.to(device)
                outputs = model(inputs)
                loss = criterion(outputs, labels)

                val_loss += loss.item()
                _, predicted = outputs.max(1)
                total += labels.size(0)
                correct += predicted.eq(labels).sum().item()
                # Store predictions and labels
                all_preds.append(outputs)
                all_labels.append(labels)
    # Concatenate all predictions and labels
    all_preds = torch.cat(all_preds)
    all_labels = torch.cat(all_labels)
    val_loss /= val_len
    val_acc = 100. * correct / total
    if type == "34":
        t = 34
        auroc = AUROC(task="multiclass", num_classes=t).to(device)
        auc = auroc(all_preds, all_labels)
        acc = Accuracy(task="multiclass", num_classes=t).to(device)
        accuracy = acc(all_preds, all_labels)
        f1c = F1Score(task="multiclass", num_classes=t).to(device)
        f1 = f1c(all_preds, all_labels)
        pre = Precision(task="multiclass", average='macro', num_classes=t).to(device)
        precision = pre(all_preds, all_labels)
        rec = Recall(task="multiclass", average='macro', num_classes=t).to(device)
        recall = rec(all_preds, all_labels)
        print(
        f'Epoch [{epoch}/{num_epochs}], Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}%, F1-Score: {f1.item():.4f}, Precision: {precision.item():.4f}, Recall: {recall.item():.4f}, AUC: {auc.item():.4f} ')
        file.write(
        f' Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}%, F1-Score: {f1.item():.4f}, Precision: {precision.item():.4f}, Recall: {recall.item():.4f}, AUC: {auc.item():.4f} \n')
    elif type == "8":
        t = 8
        acc = Accuracy(task="multiclass", num_classes=t).to(device)
        accuracy = acc(all_preds, all_labels)
        f1c = F1Score(task="multiclass", num_classes=t).to(device)
        f1 = f1c(all_preds, all_labels)
        pre = Precision(task="multiclass", average='macro', num_classes=t).to(device)
        precision = pre(all_preds, all_labels)
        rec = Recall(task="multiclass", average='macro', num_classes=t).to(device)
        recall = rec(all_preds, all_labels)
        print(
        f'Epoch [{epoch}/{num_epochs}], Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}%, F1-Score: {f1.item():.4f}, Precision: {precision.item():.4f}, Recall: {recall.item():.4f}, AUC: {auc.item():.4f} ')
        file.write(
        f' Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}%, F1-Score: {f1.item():.4f}, Precision: {precision.item():.4f}, Recall: {recall.item():.4f}, AUC: {auc.item():.4f} \n')
    else :
        print(
        f'Epoch [{epoch}/{num_epochs}], Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}%')
        file.write(
        f' Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.2f}% \n')
    end_time = time.time()
    epoch_time = end_time - start_time
    file.write(f"test {epoch} took {epoch_time:.2f} seconds. \n")
    # Compute metrics
    
    