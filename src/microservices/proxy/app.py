from flask import Flask, jsonify, request
import requests
import random
import os
import logging
from functools import wraps
import time

app = Flask(__name__)

MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', 0))
MONOLITH_URL = os.getenv('MONOLITH_URL', 'http://monolith:8080')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL', 'http://movies-service:8081')
EVENTS_SERVICE_URL = os.getenv('EVENTS_SERVICE_URL', 'http://events-service:8082')
PORT = os.getenv('PORT', '8000')
GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def should_use_new_movies_service():
    """Определяет, использовать ли новый сервис фильмов на основе процента миграции"""
    if not GRADUAL_MIGRATION:
        return False
    return random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT

def handle_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            return jsonify({'error': 'Service unavailable', 'details': str(e)}), 503
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return jsonify({'error': 'Internal server error', 'details': str(e)}), 500
    return decorated_function

def get_request_data():
    """Безопасно получает данные запроса"""
    if request.is_json:
        return request.get_json()
    elif request.content_type and 'application/json' in request.content_type:
        return request.get_json(force=True)
    else:
        return None

def proxy_request(target_url, method='GET', data=None):
    """Проксирует запрос к целевому сервису"""
    headers = {}
    for key, value in request.headers:
        if key.lower() != 'host':
            headers[key] = value
    
    logger.info(f"Proxying {method} request to: {target_url}")
    
    try:
        if method == 'GET':
            response = requests.get(
                target_url, 
                params=request.args, 
                headers=headers, 
                timeout=10
            )
        elif method == 'POST':
            request_data = data if data is not None else get_request_data()
            response = requests.post(
                target_url, 
                json=request_data,
                headers=headers, 
                timeout=10
            )
        else:
            return None, {'error': 'Method not supported'}, 405
        
        return response, None, response.status_code
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout connecting to {target_url}")
        return None, {'error': 'Service timeout'}, 504
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error to {target_url}")
        return None, {'error': 'Service connection failed'}, 503
    except Exception as e:
        logger.error(f"Error proxying to {target_url}: {str(e)}")
        return None, {'error': f'Proxy error: {str(e)}'}, 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'proxy-service'})

@app.route('/api/movies', methods=['GET', 'POST'])
@handle_errors
def movies_proxy():
    """Прокси для эндпоинтов movies с feature flag"""
    use_new = should_use_new_movies_service()
    
    if use_new:
        target_url = f"{MOVIES_SERVICE_URL}/api/movies"
        service_name = "movies-service"
    else:
        target_url = f"{MONOLITH_URL}/api/movies" 
        service_name = "monolith"
    
    logger.info(f"Routing movies request to {service_name}: {target_url}")
    
    request_data = None
    if request.method == 'POST':
        request_data = get_request_data()
    
    response, error, status_code = proxy_request(target_url, request.method, request_data)
    
    if error:
        return jsonify(error), status_code
    
    try:
        response_data = response.json()
        
        if request.method == 'GET':
            if isinstance(response_data, list):
                return jsonify(response_data), response.status_code
            elif isinstance(response_data, dict) and 'data' in response_data:
                return jsonify(response_data['data']), response.status_code
            else:
                return jsonify(response_data), response.status_code
        else:
            if isinstance(response_data, dict):
                response_data['_debug'] = {
                    'source_service': service_name,
                    'gradual_migration': GRADUAL_MIGRATION,
                    'migration_percent': MOVIES_MIGRATION_PERCENT
                }
            return jsonify(response_data), response.status_code
        
    except Exception as e:
        logger.error(f"Error parsing response from {service_name}: {str(e)}")
        return response.text, response.status_code

@app.route('/api/users', methods=['GET', 'POST'])
@handle_errors
def users_proxy():
    """Прокси для эндпоинтов users"""
    target_url = f"{MONOLITH_URL}/api/users"
    logger.info(f"Routing users request to monolith: {target_url}")
    
    request_data = None
    if request.method == 'POST':
        request_data = get_request_data()
    
    response, error, status_code = proxy_request(target_url, request.method, request_data)
    
    if error:
        return jsonify(error), status_code
    
    try:
        return jsonify(response.json()), response.status_code
    except:
        return response.text, response.status_code

@app.route('/api/payments', methods=['GET', 'POST'])
@handle_errors
def payments_proxy():
    """Прокси для эндпоинтов payments"""
    target_url = f"{MONOLITH_URL}/api/payments"
    logger.info(f"Routing payments request to monolith: {target_url}")
    
    request_data = None
    if request.method == 'POST':
        request_data = get_request_data()
    
    response, error, status_code = proxy_request(target_url, request.method, request_data)
    
    if error:
        return jsonify(error), status_code
    
    try:
        return jsonify(response.json()), response.status_code
    except:
        return response.text, response.status_code

@app.route('/api/subscriptions', methods=['GET', 'POST'])
@handle_errors
def subscriptions_proxy():
    """Прокси для эндпоинтов subscriptions"""
    target_url = f"{MONOLITH_URL}/api/subscriptions"
    logger.info(f"Routing subscriptions request to monolith: {target_url}")
    
    request_data = None
    if request.method == 'POST':
        request_data = get_request_data()
    
    response, error, status_code = proxy_request(target_url, request.method, request_data)
    
    if error:
        return jsonify(error), status_code
    
    try:
        return jsonify(response.json()), response.status_code
    except:
        return response.text, response.status_code

@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@handle_errors
def catch_all_proxy(path):
    """Прокси для всех остальных API эндпоинтов"""
    target_url = f"{MONOLITH_URL}/api/{path}"
    logger.info(f"Routing request to monolith: {target_url}")
    
    request_data = None
    if request.method in ['POST', 'PUT']:
        request_data = get_request_data()
    
    response, error, status_code = proxy_request(target_url, request.method, request_data)
    
    if error:
        return jsonify(error), status_code
    
    try:
        return jsonify(response.json()), response.status_code
    except:
        return response.text, response.status_code

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'CinemaAbyss Proxy Service',
        'endpoints': {
            '/health': 'Health check',
            '/api/movies': 'Movies API (with gradual migration)',
            '/api/users': 'Users API',
            '/api/payments': 'Payments API', 
            '/api/subscriptions': 'Subscriptions API'
        }
    })

if __name__ == '__main__':
    logger.info(f"Starting Proxy Service on port {PORT}")
    logger.info(f"Gradual migration: {GRADUAL_MIGRATION}")
    logger.info(f"Movies migration percent: {MOVIES_MIGRATION_PERCENT}%")
    app.run(host='0.0.0.0', port=PORT, debug=False)