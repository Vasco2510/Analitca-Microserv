import boto3
from flask import Flask, jsonify, request
import time
import os

# ============================================
# CONFIGURACIÓN DE AWS
# ============================================
# Usamos el valor por defecto si no se encuentra en el entorno
REGION = os.environ.get('AWS_REGION', 'us-east-1')

S3_OUTPUT_BUCKET_NAME = os.environ.get('S3_OUTPUT_BUCKET_NAME', 'analytics-proy-parcial') 
S3_OUTPUT_LOCATION = f's3://{S3_OUTPUT_BUCKET_NAME}/results/'
DATABASE_NAME = os.environ.get('GLUE_DATABASE_NAME', 'ecommerce_analytics_db')

# Variables para diagnóstico
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN')

print("=" * 60)
print("🔍 INICIANDO CONFIGURACIÓN AWS ATHENA")
print("=" * 60)
print(f"📍 Región AWS: {REGION}")
print(f"📦 Bucket S3: {S3_OUTPUT_BUCKET_NAME}")
print(f"🗄️ Base de datos: {DATABASE_NAME}")
print(f"📁 Ubicación resultados: {S3_OUTPUT_LOCATION}")
print("=" * 60)

ATHENA_CLIENT = None

def diagnosticar_conexion():
    """Función para diagnosticar problemas de conexión"""
    problemas = []
    
    print("🔍 Ejecutando diagnóstico de conexión AWS...")
    
    # 1. Verificar credenciales AWS
    try:
        sts = boto3.client('sts', region_name=REGION)
        identity = sts.get_caller_identity()
        print(f"✅ Credenciales AWS válidas - Usuario: {identity['UserId']}")
        print(f"✅ ARN: {identity['Arn']}")
    except Exception as e:
        error_msg = f"Credenciales AWS: {e}"
        problemas.append(error_msg)
        print(f"❌ {error_msg}")
    
    # 2. Verificar bucket S3
    try:
        s3 = boto3.client('s3', region_name=REGION)
        s3.list_objects_v2(Bucket=S3_OUTPUT_BUCKET_NAME, MaxKeys=1)
        print(f"✅ Bucket S3 accesible: {S3_OUTPUT_BUCKET_NAME}")
    except Exception as e:
        error_msg = f"Bucket S3 '{S3_OUTPUT_BUCKET_NAME}': {e}"
        problemas.append(error_msg)
        print(f"❌ {error_msg}")
    
    # 3. Verificar base de datos Glue
    try:
        glue = boto3.client('glue', region_name=REGION)
        db_info = glue.get_database(Name=DATABASE_NAME)
        print(f"✅ Base de datos Glue accesible: {DATABASE_NAME}")
        
        # Listar tablas disponibles
        tables_response = glue.get_tables(DatabaseName=DATABASE_NAME)
        tables = [table['Name'] for table in tables_response['TableList']]
        print(f"✅ Tablas encontradas: {tables}")
        
    except Exception as e:
        error_msg = f"Base de datos Glue '{DATABASE_NAME}': {e}"
        problemas.append(error_msg)
        print(f"❌ {error_msg}")
    
    # 4. Verificar Athena
    try:
        athena = boto3.client('athena', region_name=REGION)
        athena.list_data_catalogs(MaxResults=1)
        print("✅ Cliente Athena funcionando correctamente")
    except Exception as e:
        error_msg = f"Athena: {e}"
        problemas.append(error_msg)
        print(f"❌ {error_msg}")
    
    if problemas:
        print("❌ Problemas encontrados en la configuración:")
        for problema in problemas:
            print(f"   - {problema}")
    else:
        print("🎉 Todas las verificaciones pasaron correctamente!")
    
    return problemas

try:
    # Ejecutar diagnóstico primero
    problemas = diagnosticar_conexion()
    
    if problemas:
        print("⚠️  Hay problemas de configuración, pero intentando inicializar Athena...")
    
    ATHENA_CLIENT = boto3.client(
        'athena', 
        region_name=REGION
    )
    
    # Test simple de Athena
    ATHENA_CLIENT.list_data_catalogs(MaxResults=1)
    print("✅ Cliente de Boto3/Athena inicializado y autenticado correctamente.")

except Exception as e:
    print("=========================================================================")
    print(f"❌ ERROR CRÍTICO: Fallo en la inicialización/autenticación de Boto3/Athena.")
    print(f"   Razón: {e}")
    print("   Acción: Revise la validez de AWS_SESSION_TOKEN y los permisos IAM.")
    print("=========================================================================")

app = Flask(__name__)

def run_athena_query(query):
    """Ejecuta una consulta SQL en Athena y espera por el resultado."""
    if not ATHENA_CLIENT:
        return {"error": "El cliente de Athena no está inicializado. Error de credenciales/conexión."}, 500

    print(f"🔍 Ejecutando consulta en Athena: {query[:100]}...")
    try:
        # 1. Iniciar la ejecución de la consulta
        response = ATHENA_CLIENT.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE_NAME
            },
            ResultConfiguration={
                'OutputLocation': S3_OUTPUT_LOCATION
            }
        )
        query_execution_id = response['QueryExecutionId']
        print(f"📝 Query Execution ID: {query_execution_id}")

        # 2. Esperar a que la consulta termine (polling)
        max_attempts = 30  # Máximo 60 segundos (30 * 2 segundos)
        attempts = 0
        
        while attempts < max_attempts:
            status_response = ATHENA_CLIENT.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                print("✅ Consulta ejecutada exitosamente")
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Razón no especificada')
                print(f"❌ Consulta de Athena falló. Razón: {reason}")
                return {"error": f"Consulta fallida en Athena: {reason}"}, 500
            elif status == 'RUNNING':
                print(f"⏳ Consulta en progreso... ({attempts * 2}s)")
            
            attempts += 1
            time.sleep(2)
        else:
            return {"error": "Tiempo de espera agotado para la consulta de Athena"}, 500
        
        # 3. Obtener los resultados
        results_response = ATHENA_CLIENT.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=1000  # Límite de resultados
        )
        
        rows = results_response['ResultSet']['Rows']
        if not rows:
            return [], 200
            
        # Extraer nombres de columnas
        column_info = rows[0]
        column_names = [col['VarCharValue'] for col in column_info['Data']]
        
        # Procesar datos
        data = []
        for row in rows[1:]:
            row_data = {}
            for i, name in enumerate(column_names):
                value = row['Data'][i].get('VarCharValue') if i < len(row['Data']) else None
                row_data[name] = value
            data.append(row_data)

        print(f"✅ Consulta exitosa. Filas encontradas: {len(data)}")
        return data, 200

    except Exception as e:
        print(f"❌ ERROR general al ejecutar la consulta de Athena: {e}")
        return {"error": f"Error interno del servidor al consultar Athena: {e}"}, 500

# ============================================
# ENDPOINTS DE LA API REST
# ============================================

@app.route('/api/analytics/check', methods=['GET'])
def check_athena_connection():
    """Endpoint simple para probar la conexión y permisos de Athena"""
    
    # Consulta de prueba que une las 3 tablas
    query = """
    SELECT 
        p.nombre as producto,
        a.nombre as almacen,
        a.ubicacion,
        i.stock_disponible,
        i.stock_reservado,
        p.precio
    FROM productos p
    INNER JOIN inventarios i ON p.id_producto = i.id_producto
    INNER JOIN almacenes a ON i.id_almacen = a.id_almacen
    LIMIT 10
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "message": "Conexión a Athena exitosa y se obtuvieron datos de las 3 tablas.",
            "tablas_involucradas": ["productos", "inventarios", "almacenes"],
            "data": results,
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/tablas', methods=['GET'])
def list_tables():
    """Endpoint para listar todas las tablas disponibles en la base de datos"""
    try:
        glue_client = boto3.client('glue', region_name=REGION)
        response = glue_client.get_tables(DatabaseName=DATABASE_NAME)
        tables = []
        
        for table in response['TableList']:
            table_info = {
                'name': table['Name'],
                'columns': [
                    {'name': col['Name'], 'type': col['Type']} 
                    for col in table['StorageDescriptor']['Columns']
                ],
                'location': table['StorageDescriptor']['Location']
            }
            tables.append(table_info)
            
        return jsonify({
            "status": "success", 
            "database": DATABASE_NAME,
            "tablas": tables
        })
    except Exception as e:
        return jsonify({"error": f"No se pudieron listar las tablas: {e}"}), 500

@app.route('/api/analytics/test-consulta-simple', methods=['GET'])
def test_simple_query():
    """Endpoint para probar una consulta SQL simple"""
    
    # Consulta simple de prueba
    query = """
    SELECT 
        COUNT(*) as total_productos,
        AVG(p.precio) as precio_promedio,
        MAX(p.precio) as precio_maximo
    FROM productos p
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "consulta": "Estadísticas básicas de productos",
            "data": results
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/inventario-total', methods=['GET'])
def get_total_inventory():
    """Consulta: Inventario total por almacén"""
    
    query = """
    SELECT 
        a.nombre as almacen,
        a.ubicacion,
        a.tipo,
        COUNT(i.id_producto) as total_productos,
        SUM(i.stock_disponible) as stock_total,
        SUM(i.stock_reservado) as reservado_total
    FROM almacenes a
    LEFT JOIN inventarios i ON a.id_almacen = i.id_almacen
    GROUP BY a.nombre, a.ubicacion, a.tipo
    ORDER BY stock_total DESC
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "consulta": "Inventario total por almacén",
            "data": results
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/productos-precio', methods=['GET'])
def get_products_by_price():
    """Consulta: Productos ordenados por precio"""
    
    query = """
    SELECT 
        nombre,
        precio,
        peso,
        volumen,
        sku
    FROM productos
    ORDER BY precio DESC
    LIMIT 20
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "consulta": "Productos ordenados por precio (TOP 20)",
            "data": results
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/stock-bajo', methods=['GET'])
def get_low_stock():
    """Consulta: Productos con stock bajo"""
    
    query = """
    SELECT 
        p.nombre as producto,
        a.nombre as almacen,
        i.stock_disponible,
        i.stock_reservado,
        p.precio
    FROM productos p
    INNER JOIN inventarios i ON p.id_producto = i.id_producto
    INNER JOIN almacenes a ON i.id_almacen = a.id_almacen
    WHERE i.stock_disponible < 50
    ORDER BY i.stock_disponible ASC
    LIMIT 15
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "consulta": "Productos con stock bajo (< 50 unidades)",
            "data": results
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/consulta-personalizada', methods=['POST'])
def custom_query():
    """Endpoint para ejecutar consultas SQL personalizadas"""
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Se requiere un campo 'query' en el JSON"}), 400
        
        query = data['query']
        results, status_code = run_athena_query(query)
        
        if status_code != 500:
            return jsonify({
                "status": "success", 
                "data": results
            }), 200
        else:
            return jsonify(results), status_code
            
    except Exception as e:
        return jsonify({"error": f"Error procesando la solicitud: {e}"}), 500

@app.route('/api/analytics/estadisticas-generales', methods=['GET'])
def general_stats():
    """Estadísticas generales del ecommerce"""
    
    query = """
    SELECT 
        (SELECT COUNT(*) FROM productos) as total_productos,
        (SELECT COUNT(*) FROM almacenes) as total_almacenes,
        (SELECT COUNT(*) FROM inventarios) as total_registros_inventario,
        (SELECT AVG(precio) FROM productos) as precio_promedio,
        (SELECT SUM(stock_disponible) FROM inventarios) as stock_total_global
    """
    
    results, status_code = run_athena_query(query)
    
    if status_code != 500:
        return jsonify({
            "status": "success", 
            "consulta": "Estadísticas generales del ecommerce",
            "data": results
        }), 200
    else:
        return jsonify(results), status_code

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint básico de salud"""
    return jsonify({
        "status": "healthy",
        "service": "Microservicio Analytics Athena",
        "timestamp": time.time()
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 INICIANDO MICROSERVICIO ANALYTICS ATHENA")
    print("=" * 60)
    
    # Verificar configuración final
    problemas_finales = diagnosticar_conexion()
    
    if problemas_finales:
        print("⚠️  ADVERTENCIA: Hay problemas de configuración, pero iniciando servidor...")
        print("   Los endpoints pueden no funcionar correctamente.")
    else:
        print("🎉 Configuración OK - Servidor listo para recibir requests")
    
    print("📊 Endpoints disponibles:")
    print("   GET  /health                          - Salud del servicio")
    print("   GET  /api/analytics/check             - Verificar conexión Athena")
    print("   GET  /api/analytics/tablas            - Listar tablas disponibles")
    print("   GET  /api/analytics/test-consulta-simple - Consulta simple de prueba")
    print("   GET  /api/analytics/inventario-total  - Inventario por almacén")
    print("   GET  /api/analytics/productos-precio  - Productos por precio")
    print("   GET  /api/analytics/stock-bajo        - Productos con stock bajo")
    print("   GET  /api/analytics/estadisticas-generales - Estadísticas generales")
    print("   POST /api/analytics/consulta-personalizada - Consulta SQL personalizada")
    print("=" * 60)
    
    # Flask corriendo en el puerto 5000
    app.run(host='0.0.0.0', port=5000, debug=True)