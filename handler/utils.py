import logging
from pathlib import Path

from handler.exceptions import DirectoryCreationError, EmptyFeedsListError
from handler.logging_config import setup_logging
# from handler.feeds_handler import FeedHandler
# from handler.feeds_save import FeedSaver
from handler.reports_db import ReportDataBase

setup_logging()


# def initialize_components() -> tuple:
#     """
#     Инициализирует и возвращает все необходимые
#     компоненты для работы приложения.

#     Выполняет следующие действия:
#     1. Создает объект класса XMLSaver.
#     2. Создает объект класса XMLHandler.
#     3. Создает объект класса XMLDataBase.
#     4 Создает объект класса XMLImage

#     Returns:
#         tuple: Кортеж с инициализированными компонентами.
#     """
#     saver = FeedSaver()
#     handler = FeedHandler()
#     db_client = ReportDataBase()
#     return saver, handler, db_client


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


def get_filenames_list(folder_name: str) -> list[str]:
    """Функция, возвращает список названий фидов."""
    folder_path = Path(__file__).parent.parent / folder_name
    if not folder_path.exists():
        logging.error('Папка %s не существует', folder_name)
        raise DirectoryCreationError('Папка %s не найдена', folder_name)
    files_names = [
        file.name for file in folder_path.iterdir() if file.is_file()
    ]
    if not files_names:
        logging.error('В папке нет файлов')
        raise EmptyFeedsListError('Нет скачанных файлов')
    logging.debug('Найдены файлы: %s', files_names)
    return files_names
