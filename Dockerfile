FROM ubuntu:22.04 as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    python3-dev python3-pip python3-numpy \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswscale-dev \
    libavdevice-dev \
    libv4l-dev \
    libjpeg-dev libpng-dev libtiff-dev \
    libtbb2 libtbb-dev \
    libdc1394-dev \
    libgtk2.0-dev \
    && apt-get clean

WORKDIR /opencv

RUN git clone https://github.com/opencv/opencv.git
RUN git clone https://github.com/opencv/opencv_contrib.git

RUN mkdir build
WORKDIR /opencv/build

RUN cmake -D CMAKE_BUILD_TYPE=Release \
    -D CMAKE_INSTALL_PREFIX=/usr/local \
    -D WITH_FFMPEG=ON \
    -D WITH_GSTREAMER=OFF \
    -D WITH_TBB=ON \
    -D WITH_V4L=ON \
    -D WITH_OPENCL=OFF \
    -D WITH_CUDA=OFF \
    -D BUILD_EXAMPLES=OFF \
    -D BUILD_DOCS=OFF \
    -D BUILD_TESTS=OFF \
    -D BUILD_PERF_TESTS=OFF \
    -D BUILD_opencv_python2=OFF \
    -D BUILD_opencv_python3=ON \
    -D OPENCV_EXTRA_MODULES_PATH=/opencv/opencv_contrib/modules \
    -D ENABLE_FAST_MATH=1 \
    ../opencv

RUN make -j$(nproc) && make install


FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 libxext6 libgl1 libglib2.0-0 \
    && apt-get clean

COPY --from=builder /usr/local /usr/local

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "handler.main"]
