
from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def hola_mundo():
    return "¡Hola! El microservicio funciona básicamente"

@app.route('/health')
def health():
    return "✅ Servicio activo"

if __name__ == '__main__':
    print("🔧 Iniciando servidor básico...")
    app.run(host='0.0.0.0', port=5000, debug=True)
