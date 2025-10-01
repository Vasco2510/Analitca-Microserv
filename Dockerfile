FROM python:3.9-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
## Variables de entorno añadir . 

# Copia el código fuente
COPY . .

# Expone el puerto por defecto de Flask
EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]
