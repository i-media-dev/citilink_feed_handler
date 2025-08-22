import logging
import functools
import time
from http.client import IncompleteRead
import requests

import mysql.connector

from handler.db_config import config
from handler.logging_config import setup_logging

setup_logging()


def time_of_function(func):
    """
    Декоратор для измерения времени выполнения функции.

    Замеряет время выполнения декорируемой функции и логирует результат
    в секундах и минутах. Время округляется до 3 знаков после запятой
    для секунд и до 2 знаков для минут.

    Args:
        func (callable): Декорируемая функция, время выполнения которой
        нужно измерить.

    Returns:
        callable: Обёрнутая функция с добавленной функциональностью
        замера времени.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = round(time.time() - start_time, 3)
        logging.info(
            f'Функция {func.__name__} завершила работу. '
            f'Время выполнения - {execution_time} сек. '
            f'или {round(execution_time / 60, 2)} мин.'
        )
        return result
    return wrapper


def connection_db(func):
    """
    Декоратор для подключения к базе данных.

    Подключается к базе данных, обрабатывает ошибки в процессе подключения,
    логирует все успешные/неуспешные действия, вызывает функцию, выполняющую
    действия в базе данных и закрывает подключение.

    Args:
        func (callable): Декорируемая функция, которая выполняет
        действия с базой данных.

    Returns:
        callable: Обёрнутая функция с добавленной функциональностью
        подключения к базе данных и логирования.
    """
    def wrapper(*args, **kwargs):
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        try:
            kwargs['cursor'] = cursor
            result = func(*args, **kwargs)
            connection.commit()
            return result
        except Exception as e:
            connection.rollback()
            logging.error(f'Ошибка в {func.__name__}: {str(e)}', exc_info=True)
            raise
        finally:
            cursor.close()
            connection.close()
    return wrapper


def retry_on_network_error(max_attempts=3, delays=(2, 5, 10)):
    """Декоратор для повторных попыток при сетевых ошибках"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            last_exception = None

            while attempt < max_attempts:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except (IncompleteRead, requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError) as e:
                    last_exception = e
                    if attempt < max_attempts:
                        delay = delays[attempt - 1] if attempt - \
                            1 < len(delays) else delays[-1]
                        logging.warning(
                            f'Попытка {attempt}/{max_attempts} неудачна, '
                            f'повтор через {delay}сек: {e}')
                        time.sleep(delay)
                    else:
                        logging.error(f'Все {max_attempts} попыток неудачны')
                        raise last_exception
            return None
        return wrapper
    return decorator
