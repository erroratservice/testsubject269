# Use Ubuntu 22.04 as the base image.
FROM ubuntu:22.04

# Set environment variables for non-interactive installs and timezone.
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Kolkata

# Install tzdata and all other dependencies in one RUN step for smaller image and faster build.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    tzdata \
    wget ca-certificates python3 python3-pip python3.10-venv aria2 curl git ffmpeg \
    libssl-dev libboost-system-dev libboost-chrono-dev libboost-log-dev libboost-thread-dev \
    libstdc++6 libqt5core5a libqt5network5 libqt5xml5 \
    build-essential python3-dev && \
    # Set timezone before installing other packages that use it.
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    # Manually download and install libtorrent-rasterbar10 according to architecture.
    ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
      amd64)  DEB_URL_LIBTORRENT="https://launchpad.net/ubuntu/+source/libtorrent-rasterbar/1.2.14-1.1/+build/22620136/+files/libtorrent-rasterbar10_1.2.14-1.1_amd64.deb" ;; \
      arm64)  DEB_URL_LIBTORRENT="https://launchpad.net/ubuntu/+source/libtorrent-rasterbar/1.2.14-1.1/+build/22620137/+files/libtorrent-rasterbar10_1.2.14-1.1_arm64.deb" ;; \
      *)      echo "Unsupported architecture: $ARCH" && exit 1 ;; \
    esac && \
    wget -O /tmp/libtorrent-rasterbar10.deb "$DEB_URL_LIBTORRENT" && \
    dpkg -i /tmp/libtorrent-rasterbar10.deb && \
    rm /tmp/libtorrent-rasterbar10.deb && \
    # Download and install qbittorrent-nox.
    ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
      amd64)  DEB_URL_QBIT="https://launchpadlibrarian.net/532828789/qbittorrent-nox_4.2.5-0.1ubuntu1_amd64.deb" ;; \
      arm64)  DEB_URL_QBIT="https://launchpadlibrarian.net/532828789/qbittorrent-nox_4.2.5-0.1ubuntu1_amd64.deb" ;; \
      *)      echo "Unsupported architecture: $ARCH" && exit 1 ;; \
    esac && \
    wget -O /tmp/qbittorrent-nox.deb "$DEB_URL_QBIT" && \
    dpkg -i /tmp/qbittorrent-nox.deb && \
    rm /tmp/qbittorrent-nox.deb && \
    # Clean up APT caches and temporary files to reduce image size.
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory for the application.
WORKDIR /usr/src/app

# Set file permissions for the working directory.
RUN chmod 777 /usr/src/app

# Copy the requirements file and install Python dependencies.
COPY requirements.txt .
RUN python3 -m venv mltbenv && \
    mltbenv/bin/pip install wheel && \
    mltbenv/bin/pip install -r requirements.txt

# Copy the rest of the application files.
COPY . .

# Set the default command to run when the container starts.
CMD ["bash", "start.sh"]