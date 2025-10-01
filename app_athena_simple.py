from flask import Flask, jsonify
import boto3
import time
import os

app = Flask(__name__)

# Configuración básica
REGION = 'us-east-1'
S3_OUTPUT_BUCKET_NAME = 'analytics-proy-parcial'  # Tu bucket real
DATABASE_NAME = 'ecommerce_analytics_db'
S3_OUTPUT_LOCATION = f's3://{S3_OUTPUT_BUCKET_NAME}/results/'

print("🚀 Iniciando microservicio Athena SIMPLIFICADO...")

def ejecutar_consulta_athena(query):
    """Ejecuta UNA consulta en Athena y devuelve resultados"""
    try:
        # 1. Conectar con Athena
        athena = boto3.client('athena', region_name=REGION)
        print("✅ Cliente Athena conectado")
        
        # 2. Ejecutar consulta
        print(f"📊 Ejecutando consulta: {query}")
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE_NAME},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
        )
        
        query_id = response['QueryExecutionId']
        print(f"📝 ID de consulta: {query_id}")
        
        # 3. Esperar a que termine
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            estado = status['QueryExecution']['Status']['State']
            
            if estado == 'SUCCEEDED':
                print("✅ Consulta completada")
                break
            elif estado in ['FAILED', 'CANCELLED']:
                error = status['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')
                print(f"❌ Consulta falló: {error}")
                return None, error
                
            print("⏳ Esperando...")
            time.sleep(2)
        
        # 4. Obtener resultados
        resultados = athena.get_query_results(QueryExecutionId=query_id)
        
        # 5. Procesar resultados simples
        filas = resultados['ResultSet']['Rows']
        if len(filas) > 1:
            # Primera fila son los nombres de columnas
            columnas = [col['VarCharValue'] for col in filas[0]['Data']]
            # Demás filas son datos
            datos = []
            for fila in filas[1:]:
                dato_fila = {}
                for i, columna in enumerate(columnas):
                    valor = fila['Data'][i].get('VarCharValue', '') if i < len(fila['Data']) else ''
                    dato_fila[columna] = valor
                datos.append(dato_fila)
            
            return datos, None
        else:
            return [], "No hay datos"
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        return None, str(e)

# SOLO 2 ENDPOINTS - NADA MÁS

@app.route('/health')
def health():
    return jsonify({"status": "active", "service": "Athena Simple"})

@app.route('/api/consulta-simple')
def consulta_simple():
    """ENDPOINT PRINCIPAL - Solo una consulta específica"""
    
    # CONSULTA FIJA - No cambia
    consulta = """
    SELECT 
        p.nombre as producto,
        p.precio,
        a.nombre as almacen,
        i.stock_disponible
    FROM productos p
    INNER JOIN inventarios i ON p.id_producto = i.id_producto
    INNER JOIN almacenes a ON i.id_almacen = a.id_almacen
    LIMIT 5
    """
    
    resultados, error = ejecutar_consulta_athena(consulta)
    
    if error:
        return jsonify({
            "status": "error",
            "message": f"Error en Athena: {error}",
            "consulta_usada": consulta
        }), 500
    else:
        return jsonify({
            "status": "success",
            "message": "Consulta ejecutada correctamente",
            "total_resultados": len(resultados),
            "data": resultados,
            "consulta_usada": consulta
        })

if __name__ == '__main__':
    print("=" * 50)
    print("🎯 MICROSERVICIO ATHENA - VERSIÓN SIMPLIFICADA")
    print("📍 Endpoints:")
    print("   GET /health")
    print("   GET /api/consulta-simple")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5000, debug=True)