#!/bin/bash

# Script arguments
topic="landslide_event"
ca_file="/app/tasks/inference_script/certs/Amazon-root-CA-1.pem"
cert_file="/app/tasks/inference_script/certs/device.pem.crt"
key_file="/app/tasks/inference_script/certs/private.pem.key"
endpoint="a1qhqiuacj9opy-ats.iot.us-east-1.amazonaws.com"

# Nohup command to run the script in the background and redirect output
# AWS MQTT subsciber
nohup conda run -n aws_mqtt python "/app/tasks/inference_script/subscribe.py" --topic "$topic" \
  --ca_file "$ca_file" --cert "$cert_file" --key "$key_file" \
  --endpoint "$endpoint" >> "/app/logs/subscribe.log" 2>&1 &
echo "Started subscription script: subscribe.py (output redirected to 'subscribe.log')"
# gRPC server
nohup conda run -n grpc python /app/tasks/serve.py > "/app/logs/serve.log" 2>&1 &
echo "Started RPC server script: serve.py (output redirected to 'serve.log')"

# Wait for both background processes to finish
wait


