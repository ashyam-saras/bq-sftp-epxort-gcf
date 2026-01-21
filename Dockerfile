FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Add Flask and Gunicorn for HTTP server
RUN pip install --no-cache-dir flask gunicorn

# Copy application code and configs
COPY src/ ./src/
COPY configs/ ./configs/
COPY server.py ./

# Set environment variables
ENV PYTHONPATH=/app
ENV PORT=8080

# Expose port
EXPOSE 8080

# Use Gunicorn as the entrypoint
# Timeout should match or exceed Cloud Run timeout (2400s)
CMD ["sh", "-c", "exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 2400 server:app"]