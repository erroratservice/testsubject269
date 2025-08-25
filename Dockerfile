# Use Debian 11 "Bullseye" as the base image for a stable environment
FROM debian:11-slim

# Set environment variables to prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install essential dependencies: wget for downloading, and all runtime requirements for your bot
# This includes Python 3.9 (default in Debian 11), pip, and other necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    python3 \
    python3-pip \
    aria2 \
    megatools \
    p7zip-full \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Download and install the specific qbittorrent-nox v4.2.5 package
RUN wget http://ftp.hk.debian.org/debian/pool/main/q/qbittorrent/qbittorrent-nox_4.2.5-0.1_arm64.deb && \
    apt-get install -y ./qbittorrent-nox_4.2.5-0.1_arm64.deb && \
    rm qbittorrent-nox_4.2.5-0.1_arm64.deb

# Set up the application directory
WORKDIR /usr/src/app

# Set file permissions for the working directory.
RUN chmod 777 /usr/src/app

# Copy the requirements file first and install dependencies for build caching
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Now, copy the rest of your application code
COPY . .

# Define the command to start your application
CMD ["/bin/bash", "start.sh"]