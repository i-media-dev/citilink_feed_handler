import logging
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image

from handler.constants import (FEEDS_FOLDER, FRAME_FOLDER, IMAGE_FOLDER,
                               NAME_OF_FRAME, NEW_IMAGE_FOLDER,
                               RGB_COLOR_SETTINGS)
from handler.decorators import time_of_function
from handler.exceptions import DirectoryCreationError, EmptyFeedsListError
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()
logger = logging.getLogger(__name__)


class FeedImage(FileMixin):
    """
    Класс, предоставляющий интерфейс
    для работы с изображениями.
    """

    def __init__(
        self,
        filename: str,
        feeds_folder: str = FEEDS_FOLDER,
        image_folder: str = IMAGE_FOLDER,
        frame_folder: str = FRAME_FOLDER,
        new_image_folder: str = NEW_IMAGE_FOLDER
    ) -> None:
        self.filename = filename
        self.feeds_folder = feeds_folder
        self.image_folder = image_folder
        self.frame_folder = frame_folder
        self.new_image_folder = new_image_folder
        self._root = None
        self._existing_image_offers: set[str] = set()
        self._existing_framed_offers: set[str] = set()

    @property
    def root(self):
        """Ленивая загрузка корневого элемента."""
        if self._root is None:
            self._root = self._get_root(self.filename, self.feeds_folder)
        return self._root

    def _get_images_list(self, folder_name: str) -> list:
        """Защищенный метод, возвращает список названий фидов."""
        folder_path = Path(__file__).parent.parent / folder_name
        if not folder_path.exists():
            logging.error(f'Папка {folder_name} не существует')
            raise DirectoryCreationError(f'Папка {folder_name} не найдена')
        images_list = [
            file.name for file in folder_path.iterdir() if file.is_file()
        ]
        if not images_list:
            logging.error('В папке нет файлов')
            raise EmptyFeedsListError('Нет скачанных файлов')
        logging.debug(f'Найдены файлы: {images_list}')
        return images_list

    def _get_image_data(self, url: str) -> tuple:
        """
        Защищенный метод, загружает данные изображения
        и возвращает (image_data, image_format).
        """
        response_content = None
        try:
            response = requests.get(url)
            response.raise_for_status()
            response_content = response.content
            image = Image.open(BytesIO(response_content))
            image_format = image.format.lower() if image.format else None
            return response_content, image_format
        except requests.exceptions.RequestException as error:
            logging.error('Ошибка сети при загрузке URL %s: %s', url, error)
            return None, None
        except IOError as error:
            logging.error(
                'Pillow не смог распознать изображение из URL %s: %s',
                url,
                error
            )
            return None, None
        except Exception as error:
            logging.error(
                'Непредвиденная ошибка при обработке изображения %s: %s',
                url,
                error
            )
            return None, None

    def _get_image_filename(
        self,
        offer_id: str,
        image_data: bytes,
        image_format: str
    ) -> str:
        """Защищенный метод, создает имя файла с изображением."""
        if not image_data or not image_format:
            return ''
        return f'{offer_id}.{image_format}'

    def _save_image(
        self,
        image_data: bytes,
        folder_path: Path,
        image_filename: str
    ):
        """Защищенный метод, сохраняет изображение по указанному пути."""
        if not image_data:
            return
        try:
            file_path = folder_path / image_filename
            with open(file_path, 'wb') as f:
                f.write(image_data)
            logging.debug('Изображение сохранено: %s', file_path)
        except Exception as error:
            logging.error(
                'Ошибка при сохранении %s: %s',
                image_filename,
                error
            )

    @time_of_function
    def get_images(self):
        """Метод получения и сохранения изображений из xml-файла."""
        total_offers_processed = 0
        offers_with_images = 0
        images_downloaded = 0
        offers_skipped_existing = 0

        try:
            self._build_set(
                self.image_folder,
                self._existing_image_offers
            )
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.warning(
                'Директория с изображениями отсутствует. Первый запуск'
            )
        try:
            offers = self.root.findall('.//offer')

            if not offers:
                logging.debug('В файле %s не найдено offers', self.filename)
                return

            for offer in offers:
                offer_id = offer.get('id')
                total_offers_processed += 1

                picture = offer.find('picture')
                if picture is None:
                    continue

                offer_image = picture.text
                if not offer_image:
                    continue

                offers_with_images += 1

                if str(offer_id) in self._existing_image_offers:
                    offers_skipped_existing += 1
                    continue

                image_data, image_format = self._get_image_data(
                    offer_image
                )
                image_filename = self._get_image_filename(
                    offer_id,
                    image_data,
                    image_format
                )
                folder_path = self._make_dir(self.image_folder)
                self._save_image(
                    image_data,
                    folder_path,
                    image_filename
                )
                images_downloaded += 1
            logging.info(
                '\nВсего обработано офферов - %s'
                '\nВсего офферов с подходящими изображениями - %s'
                '\nВсего изображений скачано - %s'
                '\nПропущено офферов с уже скачанными изображениями - %s',
                total_offers_processed,
                offers_with_images,
                images_downloaded,
                offers_skipped_existing
            )
        except Exception as error:
            logging.error(
                'Неожиданная ошибка при получении изображений: %s',
                error
            )

    @time_of_function
    def add_frame(self):
        """Метод форматирует изображения и добавляет рамку."""
        file_path = self._make_dir(self.image_folder)
        frame_path = self._make_dir(self.frame_folder)
        new_file_path = self._make_dir(self.new_image_folder)
        images_names_list = self._get_files_list(self.image_folder)
        total_framed_images = 0
        total_failed_images = 0
        skipped_images = 0

        try:
            self._build_set(
                self.new_image_folder,
                self._existing_framed_offers
            )
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.warning(
                'Директория с форматированными изображениями отсутствует. '
                'Первый запуск'
            )
        try:
            frame = Image.open(frame_path / NAME_OF_FRAME)
        except Exception as error:
            logging.error('Не удалось загрузить рамку: %s', error)
            return
        try:
            for image_name in images_names_list:
                if image_name.split('.')[0] in self._existing_framed_offers:
                    skipped_images += 1
                    continue
                try:
                    with Image.open(file_path / image_name) as image:
                        image = image.convert('RGBA')
                        image.load()
                        image_width, image_height = image.size
                except Exception as error:
                    total_failed_images += 1
                    logging.error(
                        'Ошибка загрузки изображения %s: %s',
                        image_name,
                        error
                    )
                    continue

                canvas_width = image_width + 200
                canvas_height = image_height + 200

                final_image = Image.new(
                    'RGB',
                    (canvas_width, canvas_height),
                    RGB_COLOR_SETTINGS
                )
                final_image.paste(image, (100, 100))
                final_image.paste(frame, (350, 630), frame)
                final_image = final_image.convert('RGB')
                final_image.save(
                    new_file_path / f"{image_name.split('.')[0]}.png",
                    'PNG'
                )

                total_framed_images += 1
            logging.info(
                '\nКоличество изображений, к которым добавлена рамка - %s'
                '\nКоличество уже обрамленных изображений - %s'
                '\nКоличество изображений обрамленных неудачно - %s',
                total_framed_images,
                skipped_images,
                total_failed_images
            )
        except Exception as error:
            logging.error(
                'Критическая ошибка в процессе обрамления: %s',
                error
            )
            raise
