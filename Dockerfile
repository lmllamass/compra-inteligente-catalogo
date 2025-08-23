
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# OJO: usamos sh -lc para que ${PORT} se expanda; por defecto 8000
CMD ["sh","-lc","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
