FROM python:3.11-slim

# Prevent Python from writing .pyc and ensure output is unbuffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps needed to build lxml (required by requirements.txt)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt1-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first to leverage layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source
COPY . .

# Railway injects PORT. Expose a conventional default for local runs.
EXPOSE 8080

# Launch via a shell so ${PORT} is expanded when Railway runs the container.
# If PORT is not set (local), fall back to 8080.
CMD ["/bin/sh","-lc","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
