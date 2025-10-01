FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install flask boto3
COPY app_athena_simple.py .
EXPOSE 5000
CMD ["python", "app_athena_simple.py"]