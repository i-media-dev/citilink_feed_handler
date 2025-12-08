FROM jjanzic/docker-python3-opencv:latest
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-m", "handler.main"]
