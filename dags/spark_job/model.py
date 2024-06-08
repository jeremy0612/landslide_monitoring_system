from torch import nn
class landslide_identification(nn.Module):
    def __init__(self, input_dim, output_dim):
        super(landslide_identification, self).__init__()

        self.fc1 = nn.Linear(input_dim, 128)
        self.relu1 = nn.ReLU()
        self.bn1 = nn.BatchNorm1d(128)
        nn.init.kaiming_normal_(self.fc1.weight, a=0.1)
        self.fc1.bias.data.zero_()

        self.fc2 = nn.Linear(128, 64)
        self.relu2 = nn.ReLU()
        self.bn2 = nn.BatchNorm1d(64)
        nn.init.kaiming_normal_(self.fc2.weight, a=0.1)
        self.fc2.bias.data.zero_()

        self.fc3 = nn.Linear(64, 32)
        self.relu3 = nn.ReLU()
        self.bn3 = nn.BatchNorm1d(32)
        nn.init.kaiming_normal_(self.fc3.weight, a=0.1)
        self.fc3.bias.data.zero_()

        self.fc4 = nn.Linear(32, 16)
        self.relu4 = nn.ReLU()
        self.bn4 = nn.BatchNorm1d(16)
        nn.init.kaiming_normal_(self.fc4.weight, a=0.1)
        self.fc4.bias.data.zero_()
        
        self.fc5 = nn.Linear(16, 8)
        self.relu5 = nn.ReLU()
        self.bn5 = nn.BatchNorm1d(8)
        nn.init.kaiming_normal_(self.fc5.weight, a=0.1)
        self.fc5.bias.data.zero_()

        self.fc6 = nn.Linear(8, 4)
        self.relu6 = nn.ReLU()
        self.bn6 = nn.BatchNorm1d(4)
        nn.init.kaiming_normal_(self.fc6.weight, a=0.1)
        self.fc6.bias.data.zero_()

        self.fc7 = nn.Linear(4, 2)
        self.relu7 = nn.ReLU()
        self.bn7 = nn.BatchNorm1d(2)
        nn.init.kaiming_normal_(self.fc7.weight, a=0.1)
        self.fc7.bias.data.zero_()
        
        self.out = nn.Linear(2, output_dim)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.fc1(x)
        x = self.relu1(x)
        x = self.bn1(x)

        x = self.fc2(x)
        x = self.relu2(x)
        x = self.bn2(x)

        x = self.fc3(x)
        x = self.relu3(x)
        x = self.bn3(x)

        x = self.fc4(x)
        x = self.relu4(x)
        x = self.bn4(x)

        x = self.fc5(x)
        x = self.relu5(x)
        x = self.bn5(x)

        x = self.fc6(x)
        x = self.relu6(x)
        x = self.bn6(x)

        x = self.fc7(x)
        x = self.relu7(x)
        x = self.bn7(x)

        x = self.out(x)
        x = self.sigmoid(x)
        return x
    
'''    
import torch
# Create a sample model instance with example input dimensions
model = landslide_identification(100, 2)  # Assuming input dim of 100 and output dim of 2

# Create a dummy input for tracing (replace with your actual input)
dummy_input = torch.randn(1, 100)  # Replace 1 with batch size if needed

# Export the model to ONNX format
torch.onnx.export(
    model,  # The PyTorch model instance
    dummy_input,  # A dummy input for tracing
    "landslide_identification.onnx",  # Output filename
    opset_version=11,  # Optional: Specify the ONNX opset version
)

print("Model exported successfully to landslide_identification.onnx")
'''