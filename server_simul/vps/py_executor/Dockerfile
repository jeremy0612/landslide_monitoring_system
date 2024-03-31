# Use a minimal base image for Python
FROM continuumio/miniconda3:latest

# Set working directory
WORKDIR /app

# Make RUN commands use `bash --login`:
SHELL ["/bin/bash", "--login", "-c"]

# Create ingestion environment using the YAML file
COPY ingestion.yml ./
RUN conda env create -f ingestion.yml
# Create grpc environment using the YAML file
COPY grpc.yml ./
RUN conda env create -f grpc.yml

# Activate conda environment
ENV PATH /opt/conda/envs/grpc/bin:$PATH
RUN /bin/bash -c "source activate grpc"

# Run python script
# CMD ["python", "/app/tasks/serve.py"]



# Initialize conda in bash config fiiles:
# RUN conda init bash

# RUN echo "conda activate grpc" >> ~/.bashrc

# Activate the environment, and make sure it's activated:
# Make RUN commands use the new environment:
# SHELL ["conda", "run", "-n", "landslide_ingestion", "/bin/bash", "-c"]
