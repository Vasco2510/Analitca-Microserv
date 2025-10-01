
import boto3
from flask import Flask, jsonify, request
import time
import os
from dotenv import load_dotenv

# ===========================================
# Cargar Variables de Entorno (Solo para desarrollo local)
# En AWS, boto3 usa automáticamente el IAM Role del EC2/ECS/Fargate.
# ===========================================
load_dotenv()
# ============================================
# CONFIGURACIÓN DE AWS
# ============================================
# Usamos el valor por defecto si no se encuentra en el entorno
REGION = 'us-east-1'
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# CRÍTICO: REEMPLAZA 'analytics-proy-parcial' CON EL NOMBRE REAL DE TU BUCKET S3
S3_OUTPUT_BUCKET = f's3://analytics-proy-parcial/results/' 

DATABASE_NAME = 'ecommerce_analytics_db' 

try:
    ATHENA_CLIENT = boto3.client('athena', region_name=REGION)
except Exception as e:
    # Esto ocurre si las credenciales no están configuradas correctamente al inicio
    print(f"ERROR: No se pudo inicializar el cliente de Boto3/Athena: {e}")


ATHENA_CLIENT = None
try:
    # 1. Intentar establecer la conexión
    ATHENA_CLIENT = boto3.client(
        'athena', 
        region_name=REGION
        # Boto3 lee las credenciales del entorno automáticamente
    )
    
    ATHENA_CLIENT.list_data_catalogs(MaxResults=1)
    
    print("✅ Cliente de Boto3/Athena inicializado y autenticado correctamente.")

except Exception as e:
    # Si falla, imprime el error de forma clara
    print("=========================================================================")
    print(f"❌ ERROR CRÍTICO: Fallo en la inicialización/autenticación de Boto3/Athena.")
    print(f"   Razón: {e}")
    print("   Acción: Revise la validez de AWS_SESSION_TOKEN y los permisos IAM.")
    print("=========================================================================")

app = Flask(__name__)

# Función auxiliar para ejecutar la consulta y obtener los resultados
def run_athena_query(query):
    """Ejecuta una consulta SQL en Athena y espera por el resultado."""
    print(f"Ejecutando consulta en Athena: {query[:50]}...")
    try:
        # 1. Ejecutar la consulta en Athena
        response = ATHENA_CLIENT.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE_NAME
            },
            ResultConfiguration={
                'OutputLocation': S3_OUTPUT_BUCKET
            }
        )
        query_execution_id = response['QueryExecutionId']

        # 2. Esperar a que la consulta termine (polling)
        while True:
            status_response = ATHENA_CLIENT.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1) 

        if status == 'SUCCEEDED':
            # 3. Obtener y procesar los resultados
            result = ATHENA_CLIENT.get_query_results(QueryExecutionId=query_execution_id)
            
            # Manejo de consultas que no retornan data (ej. CREATE VIEW)
            if not result['ResultSet']['Rows'] or len(result['ResultSet']['Rows']) <= 1:
                return [], 200

            # Procesar las filas de resultados (la primera fila es la cabecera)
            columns = [col['VarCharValue'] for col in result['ResultSet']['Rows'][0]['Data']]
            data = []
            for row in result['ResultSet']['Rows'][1:]:
                # Crea un diccionario para cada fila
                item = {columns[i]: row['Data'][i]['VarCharValue'] for i in range(len(columns))}
                data.append(item)
            
            return data, 200
        else:
            error_message = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')
            return {"error": f"Consulta fallida: {error_message}"}, 500

    except Exception as e:
        print(f"Excepción en run_athena_query: {e}")
        return {"error": str(e)}, 500

# ============================================
# ENDPOINTS DE LA API REST (4 CONSULTAS + 1 VISTA)
# ============================================

@app.route('/api/analytics/vistas/crear', methods=['POST'])
def create_analytics_views():
    """ 
    Endpoint para crear o reemplazar las 2 vistas requeridas en Athena.
    """
    
    # Vista 1: Inventario Detallado (Unión de 3 Tablas)
    view_query_1 = """
    CREATE OR REPLACE VIEW inventario_detalle AS
    SELECT
        t1.id_inventario, t1.stock_disponible, t1.stock_reservado, t1.ultima_actualizacion,
        t2.nombre AS nombre_almacen, t2.ubicacion,
        t3.nombre AS nombre_producto, t3.precio, t3.sku
    FROM
        inventarios t1
    INNER JOIN
        almacenes t2 ON t1.id_almacen = t2.id_almacen
    INNER JOIN
        productos t3 ON t1.id_producto = t3.id_producto;
    """
    
    # Vista 2: Inventario Bajo Riesgo (Para Monitoreo del Stock)
    view_query_2 = """
    CREATE OR REPLACE VIEW inventario_bajo_riesgo AS
    SELECT
        nombre_producto, nombre_almacen, stock_disponible, stock_reservado, ultima_actualizacion
    FROM
        inventario_detalle
    WHERE
        stock_disponible < 150 OR stock_reservado > 100
    ORDER BY
        stock_disponible ASC;
    """

    # Ejecutar la creación de ambas vistas
    results1, code1 = run_athena_query(view_query_1)
    results2, code2 = run_athena_query(view_query_2)
    
    if code1 == 200 and code2 == 200:
        return jsonify({
            "status": "success", 
            "message": "Vistas 'inventario_detalle' y 'inventario_bajo_riesgo' creadas/actualizadas en Athena."
        }), 200
    else:
        # Devolver el primer error encontrado
        return jsonify({"status": "error", "message": "Fallo al crear una o más vistas.", "details": f"Error 1: {results1}, Error 2: {results2}"}), 500

@app.route('/api/analytics/top-productos-valor', methods=['GET'])
def get_top_products_by_value():
    """ 
    Consulta 1: Top 5 Productos con Mayor Valor de Inventario Total (inventarios, productos).
    """
    query = f"""
    SELECT
        t2.nombre AS nombre_producto,
        SUM(CAST(t1.stock_disponible AS DOUBLE) * t2.precio) AS valor_inventario_total
    FROM
        inventarios t1
    INNER JOIN
        productos t2 ON t1.id_producto = t2.id_producto
    GROUP BY
        t2.nombre
    ORDER BY
        valor_inventario_total DESC
    LIMIT 5
    """
    results, status_code = run_athena_query(query)
    if status_code != 500:
        return jsonify({"status": "success", "data": results, "metadata": "Top 5 Productos por Valor Monetario de Inventario (Consulta 1)"})
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/almacen-eficiencia', methods=['GET'])
def get_warehouse_efficiency():
    """ 
    Consulta 2: Stock Promedio por Tipo de Almacén (inventarios, almacenes).
    """
    query = f"""
    SELECT
        t2.tipo AS tipo_almacen,
        COUNT(DISTINCT t1.id_producto) AS productos_diferentes_almacenados,
        AVG(CAST(t1.stock_disponible AS DOUBLE)) AS stock_promedio_por_producto
    FROM
        inventarios t1
    INNER JOIN
        almacenes t2 ON t1.id_almacen = t2.id_almacen
    GROUP BY
        t2.tipo
    ORDER BY
        productos_diferentes_almacenados DESC
    """
    results, status_code = run_athena_query(query)
    if status_code != 500:
        return jsonify({"status": "success", "data": results, "metadata": "Eficiencia de Inventario por Tipo de Almacén (Consulta 2)"})
    else:
        return jsonify(results), status_code


@app.route('/api/analytics/inventario-bajo-riesgo', methods=['GET'])
def get_low_risk_inventory():
    """ 
    Consulta 3: Productos con Bajo Stock (Utiliza la Vista 2: inventario_bajo_riesgo).
    """
    # Esta consulta usa la vista pre-calculada
    query = f"""
    SELECT
        nombre_producto,
        nombre_almacen,
        stock_disponible,
        stock_reservado
    FROM
        inventario_bajo_riesgo
    LIMIT 20
    """
    results, status_code = run_athena_query(query)
    if status_code != 500:
        return jsonify({"status": "success", "data": results, "metadata": "Inventario de Bajo Riesgo de Quiebre (Consulta 3 usando Vista 2)"})
    else:
        return jsonify(results), status_code

@app.route('/api/analytics/datos-generales-producto', methods=['GET'])
def get_product_general_metrics():
    """ 
    Consulta 4: Análisis de Volumen y Peso Promedio (productos).
    """
    query = f"""
    SELECT
        COUNT(id_producto) AS total_productos,
        AVG(peso) AS peso_promedio_kg,
        AVG(volumen) AS volumen_promedio_m3,
        MAX(precio) AS precio_maximo
    FROM
        productos
    """
    results, status_code = run_athena_query(query)
    if status_code != 500:
        return jsonify({"status": "success", "data": results, "metadata": "Datos Generales del Catálogo de Productos (Consulta 4)"})
    else:
        return jsonify(results), status_code


if __name__ == '__main__':
    # Flask corriendo en el puerto 5000 (el puerto que expondremos en Docker)
    app.run(host='0.0.0.0', port=5000)
