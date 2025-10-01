FROM python:3.9-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia requirements primero (para cachear las dependencias)
COPY requirements.txt .

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código fuente
COPY . .

# Variables de entorno por defecto (opcionales)
ENV AWS_REGION=us-east-1
ENV GLUE_DATABASE_NAME=ecommerce_analytics_db
ENV FLASK_ENV=production

# Expone el puerto por defecto de Flask
EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]