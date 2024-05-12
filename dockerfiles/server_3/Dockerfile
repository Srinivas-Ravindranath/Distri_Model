# Use an official Python runtime as a parent image
FROM python:3.9

# Install Git
RUN apt-get update && apt-get install -y \
    git \
    pkg-config \
    libhdf5-dev

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the Distry_Model directory from your host to the container's working directory
RUN git clone https://github.com/Srinivas-Ravindranath/Distri_Model.git

# Download the dataset
WORKDIR /usr/src/app/Distri_Model/Dataset

RUN wget -O spotify_data.csv "https://www.dropbox.com/scl/fi/i75lq9ys6vkekbbv3cokh/spotify_data.csv?rlkey=pwae21vg20se9huy7i57fyn3l&st=1ziyuhlw&dl=0"

# Change to the Distry_Model directory
WORKDIR /usr/src/app/Distri_Model

RUN pip install --no-binary=h5py h5py && \
    pip install --no-cache-dir -r requirements.txt

# Make port 9092 available to the world outside this container
EXPOSE 9092

# Run app.py when the container launches
CMD ["python", "./server/server_3.py"]