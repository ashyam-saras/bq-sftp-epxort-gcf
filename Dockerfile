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
# Without this, print() to stdout is block-buffered (stdout is not a TTY under
# gunicorn), so cprint output never reaches Cloud Logging and failures look
# silent — only gunicorn's own stderr lines show up. Cost us a day of debugging
# a DNS outage that was logging correctly the whole time. Do not remove.
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Use Gunicorn as the entrypoint
# Timeout should match or exceed Cloud Run timeout (2400s)
CMD ["sh", "-c", "exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 2400 server:app"]