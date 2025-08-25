# Use Debian 11 "Bullseye" as the stable base image
FROM debian:11-slim

# Set environment variables to prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.10.13

# Install all build dependencies and runtime dependencies in a single layer
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
    qbittorrent-nox=4.2.5-0.1 \
 && rm -rf /var/lib/apt/lists/*

# Compile and install Python 3.10 from source, making it the default
RUN wget "https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz" \
    && tar -xvf "Python-$PYTHON_VERSION.tar.xz" \
    && cd "Python-$PYTHON_VERSION" \
    && ./configure --enable-optimizations --with-ensurepip=install \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /Python-$PYTHON_VERSION*

# Set the working directory
WORKDIR /usr/src/app

# Copy and install requirements using the new default pip3
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Run the bot using the new default python3
CMD ["python3", "-m", "bot"]
