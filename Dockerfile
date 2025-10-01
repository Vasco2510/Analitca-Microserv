FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app_athena_simple.py .

EXPOSE 5000

# Comando para ejecutar
CMD ["python", "app_athena_simple.py"]