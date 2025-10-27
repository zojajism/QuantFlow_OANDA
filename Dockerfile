# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Create and set work directory
WORKDIR /app

# Copy dependency list and install first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Set PYTHONPATH so "src" is importable
ENV PYTHONPATH=/app/src

# Default command
CMD ["python", "-m", "src.main"]
