FROM python:3.10-slim

WORKDIR /app

RUN apt update -y \
    && apt install -y \
    libexpat1

# Copy only the requirements, this install step can take a while
# and requirements don't change as often as code, so doing it
# this way allows docker to cache the env
COPY ./requirements.txt /app/requirements.txt
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app/src
COPY ./src /app/src/

# Install the package to register the CLI entrypoint
# This should be relatively quick since the dependencies are already satisfied
COPY ./setup.py /app/setup.py
RUN pip install /app/
