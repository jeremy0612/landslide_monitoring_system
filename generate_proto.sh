mkdir -p build/protos
python -m grpc_tools.protoc -I./ --python_out=build --grpc_python_out=build ./protos/order.proto
sudo cp -r build/protos/ ./dags/
sudo touch dags/protos/__init__.py
sudo cp -r dags/protos ./tasks
rm -r build
