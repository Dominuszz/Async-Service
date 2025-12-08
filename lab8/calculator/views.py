from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

import time
import random
import requests
import math
import logging
from concurrent import futures

# Настройка логирования
logger = logging.getLogger(__name__)

# URL основного сервиса Go
MAIN_SERVICE_URL = "http://localhost:8080/api/v1/bigorequest/"
# Секретный ключ для авторизации
SECRET_KEY = "SECRET_KEY_12345"

# Пул потоков для асинхронных задач
executor = futures.ThreadPoolExecutor(max_workers=3)

def calculate_complexity_task(request_id, compclasses_data):
    """
    Долгий расчет времени выполнения алгоритмов для заявки
    Имитирует реальный расчет с задержкой 5-10 секунд
    """
    logger.info(f"Начало расчета для заявки {request_id}")

    delay = random.randint(2, 5)
    time.sleep(delay)
    
    total_time = 0
    max_time = 0
    max_complexity = "O(1)"
    
    # Расчет времени для каждого класса сложности
    for compclass in compclasses_data:
        try:
            degree = float(compclass.get('degree', 1.0))
            array_size = int(compclass.get('array_size', 1000))
            complexity = str(compclass.get('complexity', '1'))
            
            # Формула: время = array_size^degree
            compclass_time = math.pow(array_size, degree)
            total_time += compclass_time
            
            if compclass_time > max_time:
                max_time = compclass_time
                max_complexity = f"O({complexity})"
                
            logger.debug(f"Класс {complexity}: размер={array_size}, степень={degree}, время={compclass_time:.2f}")
        except Exception as e:
            logger.error(f"Ошибка расчета для класса {compclass}: {e}")
            continue
    
    # Случайный успех/неудача (имитация реального расчета)
    success = random.choice([True, False, True, True])  # 75% успеха
    
    result = {
        "request_id": request_id,
        "calculated_time": round(total_time, 2),
        "calculated_complexity": max_complexity,
        "success": success,
        "message": "Расчет успешно завершен" if success else "Ошибка в расчетах"
    }
    
    logger.info(f"Расчет для заявки {request_id} завершен: время={total_time:.2f}, сложность={max_complexity}, успех={success}")
    return result

def send_result_to_main_service(task):
    """
    Колбэк для отправки результатов расчета в основной сервис
    """
    try:
        result = task.result()
        logger.info(f"Отправка результатов для заявки {result['request_id']}")
    except futures.CancelledError:
        logger.warning("Задача расчета была отменена")
        return
    except Exception as e:
        logger.error(f"Ошибка в задаче расчета: {e}")
        return
    
    # URL для обновления заявки в основном сервисе
    update_url = f"{MAIN_SERVICE_URL}{result['request_id']}/update-calculation/"
    
    try:
        # Отправка PUT запроса с результатами
        # ВАЖНО: Заголовок должен быть "Bearer SECRET_KEY_12345"
        response = requests.put(
            update_url,
            json={
                "calculated_time": result['calculated_time'],
                "calculated_complexity": result['calculated_complexity'],
                "success": result['success'],
                "message": result['message']
            },
            headers={
                "Authorization": "Bearer SECRET_KEY_12345",  # ТОЧНО ТАК!
                "Content-Type": "application/json"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"Результаты для заявки {result['request_id']} успешно отправлены")
        else:
            logger.error(f"Ошибка отправки результатов: {response.status_code} - {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка соединения с основным сервисом: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        
@api_view(['POST'])
def start_calculation(request):
    """
    Запуск асинхронного расчета для заявки
    Принимает: {"request_id": 123, "compclasses": [{"complexity": "n", "degree": 1.0, "array_size": 1000}]}
    Возвращает: 202 Accepted - расчет запущен
    """
    logger.info(f"Получен запрос на расчет: {request.data}")
    
    # Валидация входных данных
    required_fields = ["request_id", "compclasses"]
    for field in required_fields:
        if field not in request.data:
            logger.error(f"Отсутствует обязательное поле: {field}")
            return Response(
                {"error": f"Missing required field: {field}"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
    
    request_id = request.data["request_id"]
    compclasses_data = request.data["compclasses"]
    
    # Проверка типа данных
    if not isinstance(request_id, int) or request_id <= 0:
        return Response(
            {"error": "request_id must be a positive integer"}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    
    if not isinstance(compclasses_data, list):
        return Response(
            {"error": "compclasses must be an array"}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    
    # Запуск расчета в фоновом режиме
    try:
        task = executor.submit(calculate_complexity_task, request_id, compclasses_data)
        task.add_done_callback(send_result_to_main_service)
        
        logger.info(f"Расчет для заявки {request_id} запущен в фоновом режиме")
        
        return Response(
            {
                "status": "calculation_started",
                "request_id": request_id,
                "message": "Расчет запущен в асинхронном режиме. Результаты будут отправлены автоматически.",
                "estimated_time": "5-10 секунд"
            },
            status=status.HTTP_202_ACCEPTED
        )
        
    except Exception as e:
        logger.error(f"Ошибка запуска расчета: {e}")
        return Response(
            {"error": f"Failed to start calculation: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@api_view(['GET'])
def health_check(request):
    """
    Проверка состояния сервиса
    """
    return Response({
        "status": "healthy",
        "service": "BigOCalc Async Calculator",
        "active_tasks": executor._work_queue.qsize(),
        "max_workers": executor._max_workers
    })