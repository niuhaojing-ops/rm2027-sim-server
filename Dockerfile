FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE run.py config.example.json ./
COPY src ./src
COPY videos ./videos

RUN pip install --no-cache-dir .

EXPOSE 3333/tcp 18885/tcp 3334/udp

ENTRYPOINT ["rm2027-sim"]
CMD ["--host", "0.0.0.0", "--port", "3333", "--control-port", "18885", "--video-dir", "/app/videos"]
