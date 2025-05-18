FROM python:3.13-alpine AS builder

WORKDIR /build
COPY pyproject.toml .

RUN pip install --no-cache-dir --prefix=/install . && \
    find /install -name "*.py[co]" -delete && \
    find /install -type d -name "__pycache__" -exec rm -rf {} +

FROM python:3.13-alpine

RUN apk add --no-cache tzdata && \
    cp /usr/share/zoneinfo/Europe/Moscow /etc/localtime && \
    echo "Europe/Moscow" > /etc/timezone

ENV TZ=Europe/Moscow

COPY --from=builder /install /usr/local

COPY ./src ./src

EXPOSE 8000

CMD ["python", "src/client.py"]
