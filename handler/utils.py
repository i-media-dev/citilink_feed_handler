from handler.feeds_handler import FeedHandler
from handler.feeds_save import FeedSaver
from handler.reports_db import ReportDataBase


def initialize_components() -> tuple:
    """
    Инициализирует и возвращает все необходимые
    компоненты для работы приложения.

    Выполняет следующие действия:
    1. Создает объект класса XMLSaver.
    2. Создает объект класса XMLHandler.
    3. Создает объект класса XMLDataBase.
    4 Создает объект класса XMLImage

    Returns:
        tuple: Кортеж с инициализированными компонентами.
    """
    saver = FeedSaver()
    handler = FeedHandler()
    db_client = ReportDataBase()
    return saver, handler, db_client


def save_to_database(
    db_client: ReportDataBase,
    data: list
) -> None:
    """
    Сохраняет данные в базу данных.
    Args:
        - db_client (XMLDataBase): Клиент для работы с базой данных.
        - data: Данные для сохранения
    """
    queries = [
        db_client.insert_reports(data),
        db_client.insert_catalog(data)
    ]
    for query in queries:
        db_client.save_to_database(query)
