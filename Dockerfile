# Use Debian 11 "Bullseye" as the stable base image
FROM debian:11-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.10.13

# STEP 1: Install system dependencies and compile Python
# This entire layer will be cached. It only re-runs if this command changes.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libsqlite3-dev \
    libreadline-dev \
    libffi-dev \
    curl \
    xz-utils \
    tk-dev \
    liblzma-dev \
    aria2 \
    megatools \
    p7zip-full \
    ffmpeg \
    libmagic-dev \
    qbittorrent-nox=4.2.5-0.1 \
 && rm -rf /var/lib/apt/lists/* \
 && wget "https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz" \
    && tar -xvf "Python-$PYTHON_VERSION.tar.xz" \
    && cd "Python-$PYTHON_VERSION" \
    && ./configure --enable-optimizations --with-ensurepip=install \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /Python-$PYTHON_VERSION*

# Set the working directory
WORKDIR /usr/src/app

# STEP 2: Install Python dependencies
# This layer is cached and only re-runs if requirements.txt changes
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# STEP 3: Copy the application code
# This is one of the last steps, so changes to your code won't trigger a full reinstall
COPY . .

# Define the command to start the application using your script
CMD ["/bin/bash", "start.sh"]
