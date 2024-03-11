# Setup for AWS IoT Core connected to raspberry pi
Use awsiotsdk to deploy MQTT application to MQTT broker in cloud. This procedure could be use for all device running linux OS. 
## Requirements
In order to avoid unexpected traceback due to lack of packages, we should update the apt in raspi as it would be too old for. Run the update command:
```
sudo apt-get update
sudo apt-get upgrade
```
install the requirement packages for awsiotsdk.
```
sudo apt-get install cmake
sudo apt-get install libssl-dev
sudo apt-get install git
```
if your raspi has not installed the python3 yet, run the following command. By default, the raspi OS already has installed python that you should check the python version first.
```
sudo apt install python3
sudo apt install python3-pip
```
make sure your raspi already has git, python3 and pip3, to check using the command.
```
git     --version
python3 --version
pip3    --version
```
after that, go to your /home directory and start installing awsiotsdk with pip3. The reason for the cache parameter is that the HASH value error returned when I installed the packages in the second time (first time was fixed by ignoring the cache)
```
cd ~
pip3 install --no-cache-dir awsiotsdk
```

If you still got the error when trying download the awscrt package, you would rather to reinstall python or use the default python version of raspi and make sure your rapi has a good speed internet connection. Then clone the repo of aws for sample apps and dependencies to build the IoT project.
```
git clone https://github.com/aws/aws-iot-device-sdk-python-v2.git
```
One important thing that your end device need authorization method to connect with the MQTT broker or other service provided by AWS IoT Core. To get the certificates from AWS IoT Core, please follow this [AWS Tutorial](https://docs.aws.amazon.com/iot/latest/developerguide/create-iot-resources.html). The certificates should be named and allocated exactly the same as described below for smoothy process. (or you should hard code the in the sample apps 😂)

| File | File path |
| -------- | ------- |
| Root CA certificate | ~/certs/Amazon-root-CA-1.pem |
| Device certificate | ~/certs/device.pem.crt |
| Private key |	~/certs/private.pem.key |
## Run sample app
Finally, you need to take the endpoint in AWS IoT Console --> All device --> Things --> setting. This was used for edge device to identify the destination broker. Now redirect to the sample and run it.
```
cd ~/aws-iot-device-sdk-python-v2/samples

python3 pubsub.py --topic topic_1 --ca_file ~/certs/Amazon-root-CA-1.pem --cert ~/certs/device.pem.crt --key ~/certs/private.pem.key --endpoint your-iot-endpoint
```
if the sample app was broken, check the permission of the "thing" when you created. 