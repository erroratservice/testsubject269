FROM debian:11-slim
ENV DEBIAN_FRONTEND=noninteractive
FROM debian:11-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget python3 python3-pip aria2 megatools p7zip-full ffmpeg \
    qbittorrent-nox=4.2.5-0.1 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/app
RUN chmod 777 /usr/src/app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .
CMD ["/bin/bash", "start.sh"]