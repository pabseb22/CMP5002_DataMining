# Layer 1: Base Image
FROM python:3.10-slim

# / Means Base Dir. The following creates a dir. You can choose any names
# Configure workspace directory inside container
WORKDIR /appPablo

# using "." it copies and pastes eveything on my actual directory of my dockerfile
# copy local files to container
# COPY . /appPablo
COPY requirements.txt /appPablo
RUN pip install -r requirements.txt

COPY . /appPablo

# CMD it receives an array of string and receives a terminal language to execute python
# Command to execute python script
CMD ["python", "./notebooks/ingestData.py"]

# Build and run:

# docker build -t hello-world-docker .
# "-t" Allows to define name of image
# "." Menas that it is from base dir therefore we need to be in the same as the dockerfile

# To run: docker run hello-world-docker

# This image does not stay alive it just prints. Running from docker desktop goes to logs.
