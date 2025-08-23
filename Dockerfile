FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port your application listens on (if applicable)
# For example, if your Flask app runs on port 5000:
# EXPOSE 5000

# Define the command to run your application
CMD ["python", "app.py"]