# Dockerfile

# Use an official Python runtime as the base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

RUN pip install --no-cache-dir azure-storage-blob==12.14.1

# Copy the Python script into the container
COPY manage_blob_containers.py .
COPY init-gcs.sh .

# Command to run the script
CMD ["python", "manage_blob_containers.py"]
