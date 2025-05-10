FROM python:3.13.0-alpine AS builder

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.13.0-alpine

COPY --from=builder /install /usr/local

RUN apk update && apk add --no-cache tzdata

COPY . .

CMD ["python", "client.py"]