from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json
import logging
import os
import time
from datetime import datetime

app = Flask(__name__)


KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
PORT = os.getenv('PORT', '8082')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


producer = None

def init_kafka():
    """Инициализация Kafka producer"""
    global producer
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BROKER}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            request_timeout_ms=10000
        )
        logger.info("Successfully connected to Kafka")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {str(e)}")
        return False


time.sleep(15)  
kafka_ready = init_kafka()

@app.route('/api/events/health', methods=['GET'])
def health_check():
    kafka_status = "connected" if kafka_ready and producer else "disconnected"
    return jsonify({
        'status': True, 
        'service': 'events-microservice',
        'kafka': kafka_status,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    """Создание события пользователя"""
    if not kafka_ready or not producer:
        return jsonify({'error': 'Kafka not available'}), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        event = {
            'type': 'user',
            'event': data.get('action', 'user_created'),
            'user_id': data.get('user_id'),
            'username': data.get('username'),
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        producer.send('user-events', event)
        producer.flush(timeout=5)
        
        logger.info(f"User event sent: user_id={event['user_id']}, action={event['event']}")
        
        return jsonify({
            'status': 'success',
            'message': 'User event created successfully',
            'event_id': f"user_{event['user_id']}_{int(datetime.now().timestamp())}",
            'event': event
        }), 201
        
    except Exception as e:
        logger.error(f"Failed to process user event: {str(e)}")
        return jsonify({'error': f'Failed to process event: {str(e)}'}), 500

@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    """Создание платежного события"""
    if not kafka_ready or not producer:
        return jsonify({'error': 'Kafka not available'}), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        event = {
            'type': 'payment',
            'event': 'payment_processed',
            'payment_id': data.get('payment_id'),
            'user_id': data.get('user_id'),
            'amount': data.get('amount'),
            'status': data.get('status', 'completed'),
            'method_type': data.get('method_type', 'credit_card'),
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        producer.send('payment-events', event)
        producer.flush(timeout=5)
        
        logger.info(f"Payment event sent: payment_id={event['payment_id']}, amount={event['amount']}")
        
        return jsonify({
            'status': 'success',
            'message': 'Payment event created successfully',
            'event_id': f"payment_{event['payment_id']}_{int(datetime.now().timestamp())}",
            'event': event
        }), 201
        
    except Exception as e:
        logger.error(f"Failed to process payment event: {str(e)}")
        return jsonify({'error': f'Failed to process event: {str(e)}'}), 500

@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    """Создание события фильма"""
    if not kafka_ready or not producer:
        return jsonify({'error': 'Kafka not available'}), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        if not data.get('movie_id') or not data.get('user_id'):
            return jsonify({'error': 'Missing required fields: movie_id and user_id are required'}), 400
        
        event = {
            'type': 'movie',
            'event': data.get('action', 'viewed'),
            'user_id': data.get('user_id'),
            'movie_id': data.get('movie_id'),
            'title': data.get('title', 'Unknown Movie'),
            'duration': data.get('duration'),
            'progress': data.get('progress'),
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        producer.send('movie-events', event)
        producer.flush(timeout=5)
        
        logger.info(f"Movie event sent: movie_id={event['movie_id']}, user_id={event['user_id']}, action={event['event']}")
        
        return jsonify({
            'status': 'success',
            'message': 'Movie event created successfully',
            'event_id': f"movie_{event['movie_id']}_{event['user_id']}_{int(datetime.now().timestamp())}",
            'event': event
        }), 201
        
    except Exception as e:
        logger.error(f"Failed to process movie event: {str(e)}")
        return jsonify({'error': f'Failed to process event: {str(e)}'}), 500

@app.route('/api/events/test', methods=['GET', 'POST'])
def test_events():
    """Тестовый эндпоинт для проверки работы events"""
    test_data = {
        'user_event': {
            'user_id': 123,
            'username': 'test_user',
            'action': 'registered'
        },
        'movie_event': {
            'movie_id': 456,
            'user_id': 123,
            'title': 'Test Movie',
            'action': 'viewed'
        },
        'payment_event': {
            'payment_id': 789,
            'user_id': 123,
            'amount': 9.99,
            'status': 'completed'
        }
    }
    
    if request.method == 'POST':
        results = {}
        
        try:
            user_response = create_user_event()
            results['user_event'] = {
                'status_code': user_response[1],
                'response': user_response[0].get_json() if user_response[0] else None
            }
        except Exception as e:
            results['user_event'] = {'error': str(e)}
        
        try:
            movie_response = create_movie_event()
            results['movie_event'] = {
                'status_code': movie_response[1],
                'response': movie_response[0].get_json() if movie_response[0] else None
            }
        except Exception as e:
            results['movie_event'] = {'error': str(e)}
        
        try:
            payment_response = create_payment_event()
            results['payment_event'] = {
                'status_code': payment_response[1],
                'response': payment_response[0].get_json() if payment_response[0] else None
            }
        except Exception as e:
            results['payment_event'] = {'error': str(e)}
        
        return jsonify({
            'test_results': results,
            'kafka_status': 'connected' if kafka_ready else 'disconnected'
        })
    
    return jsonify({
        'message': 'Events service test endpoint',
        'kafka_status': 'connected' if kafka_ready else 'disconnected',
        'test_data': test_data
    })

@app.errorhandler(400)
def bad_request(error):
    return jsonify({
        'error': 'Bad Request',
        'message': 'The server could not understand the request. Please check your JSON format and headers.',
        'details': str(error)
    }), 400

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal Server Error',
        'message': 'An internal server error occurred.'
    }), 500

if __name__ == '__main__':
    logger.info(f"Starting Events Microservice on port {PORT}")
    logger.info(f"Kafka broker: {KAFKA_BROKER}")
    logger.info(f"Kafka status: {'connected' if kafka_ready else 'disconnected'}")
    app.run(host='0.0.0.0', port=PORT, debug=False)