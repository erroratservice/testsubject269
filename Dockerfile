FROM debian:11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.10.13

# Step 0: Certificates + toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget curl xz-utils \
    build-essential libssl-dev zlib1g-dev libncurses5-dev libgdbm-dev \
    libnss3-dev libsqlite3-dev libreadline-dev libffi-dev tk-dev liblzma-dev \
 && update-ca-certificates && rm -rf /var/lib/apt/lists/*

# Step 1: Build Python
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
 && tar -xvf Python-${PYTHON_VERSION}.tar.xz \
 && cd Python-${PYTHON_VERSION} \
 && ./configure --enable-optimizations --with-ensurepip=install \
 && make -j"$(nproc)" && make install \
 && cd / && rm -rf Python-${PYTHON_VERSION}*

# Step 2: App/system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 megatools p7zip-full ffmpeg libmagic-dev procps git\
    qbittorrent-nox=4.2.5-0.1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
CMD ["/bin/bash", "start.sh"]
