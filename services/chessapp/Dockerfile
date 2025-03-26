# Use the official Python runtime image
# FROM python:3.10.16-slim-bullseye
FROM python:3.14-rc-bullseye
 
# Set the working directory inside the container
WORKDIR /app
 
# Set environment variables 
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
#Prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1 
 
# Install Packages for mysql, postgresql
#RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
#    --mount=target=/var/cache/apt,type=cache,sharing=locked \
#    rm -f /etc/apt/apt.conf.d/docker-clean \
#    && apt-get update \
#    && apt-get install -y \
#    default-libmysqlclient-dev \
#    pkg-config \
#    gcc \
#    && rm -rf /var/lib/apt/lists/*

#    default-libmysqlclient-dev \
#    wget gcc libpq-dev pkg-config \
#
## Manually Install mariadb for compatibility
#RUN wget https://dlm.mariadb.com/4047908/Connectors/c/connector-c-3.4.4/mariadb-connector-c-3.4.4-debian-bullseye-amd64.tar.gz -O - | tar -zxf - --strip-components=1 -C /usr

# Copy the Django project and install dependencies
COPY requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy the Django project to the container
COPY . .
 
# Expose the Django port
EXPOSE 8000
 
# Run Django’s development server
CMD ["sh", "-c", "sleep 10 && python manage.py migrate && python manage.py runserver 0.0.0.0:8000 --insecure"]
