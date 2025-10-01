FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt requirements-dev.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Install the package in development mode
RUN pip install -e .

# Create non-root user
RUN useradd -m -u 1000 benchmark && \
    chown -R benchmark:benchmark /app

USER benchmark

# Expose common ports
EXPOSE 8080 8081 8082

# Default command
CMD ["python", "scripts/start_worker.py", "--help"]