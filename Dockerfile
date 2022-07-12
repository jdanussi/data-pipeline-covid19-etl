FROM python:3.10.4-slim

# Run updates and install pip upgrade
RUN apt-get update && apt-get upgrade -y
RUN python -m pip install --upgrade pip

# Set up of working directory
WORKDIR /etl

# Install python dependencies
COPY requirements.txt . 
RUN pip install -r requirements.txt

# Create directories a copy files
RUN mkdir -p /etl/data
COPY *.py ./

# Set up the container startup command 
CMD [ "python3", "etl.py" ]
