# Use an official Python runtime as a parent image
FROM python:3.9

# Install Git
RUN apt-get update && apt-get install -y git

# Set the working directory in the container
WORKDIR /usr/src/app


RUN git clone https://github.com/Srinivas-Ravindranath/Distri_Model.git

WORKDIR /usr/src/app/Distri_Model

pip install --no-binary=h5py h5py
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

## Copy the rest of your application's source code from your host to your filesystem
#COPY . .

# Make port 9092 available to the world outside this container
EXPOSE 9092

# Define environment variable
ENV KAFKA_BROKER=localhost:9092
ENV KAFKA_TOPIC=mytopic

# Run app.py when the container launches
CMD ["python", "./main.py"]
