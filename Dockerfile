FROM python:3.10-slim

# Set environment variables to prevent Python from writing .pyc files
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory in the container
WORKDIR /app


# Install build dependencies (termasuk libisl-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
  gcc \
  g++ \
  make \
  libopenblas-dev \
  liblapack-dev \
  gfortran \
  && rm -rf /var/lib/apt/lists/*
  
# Copy the requirements file into the container
COPY requirements.txt .
# Upgrade pip, setuptools, and wheel to the latest versions
RUN pip install --upgrade pip setuptools wheel
# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the Flask application will run on
EXPOSE 8000
EXPOSE 8001

# Command to run the application
CMD ["python", "src/supervisor.py"]
