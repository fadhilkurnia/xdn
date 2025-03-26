# Use the official Python runtime image
FROM python:3.10.16-slim-bullseye
 
# Set the working directory inside the container
WORKDIR /app
 
# Set environment variables 
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
#Prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1 
 
# Copy the Django project  and install dependencies
COPY requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy the Django project to the container
COPY . .

# Migrate Data
RUN touch /app/data/db.sqlite3
#RUN python manage.py makemigrations
#RUN python manage.py migrate
 
# Expose the Django port
EXPOSE 8000
 
# Run Django’s development server
CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8000 --insecure"]
