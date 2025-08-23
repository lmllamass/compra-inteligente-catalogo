
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Opcionalmente: buffer a stdout/stderr y workers en 1
ENV PYTHONUNBUFFERED=1

# IMPORTANTE: usa el shell para expandir ${PORT}, con fallback a 8000
CMD ["/bin/sh","-lc","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]