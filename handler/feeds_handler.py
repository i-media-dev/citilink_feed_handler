import logging
import xml.etree.ElementTree as ET
from collections import defaultdict

from handler.constants import (FEEDS_FOLDER, IMAGE_FTP_ADDRESS,
                               NEW_FEEDS_FOLDER, NEW_IMAGE_FOLDER,
                               VIDEO_FTP_ADDRESS, VIDEOS_FOLDER)
from handler.decorators import time_of_function
from handler.feeds import FEEDS
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()
logger = logging.getLogger(__name__)


class FeedHandler(FileMixin):
    """
    Класс, предоставляющий интерфейс
    для обработки xml-файлов.
    """

    def __init__(
        self,
        filename: str,
        feeds_folder: str = FEEDS_FOLDER,
        new_feeds_folder: str = NEW_FEEDS_FOLDER,
        new_images_folder: str = NEW_IMAGE_FOLDER,
        videos_folder: str = VIDEOS_FOLDER,
        feeds_list: tuple[str, ...] = FEEDS
    ):
        self.filename = filename
        self.feeds_folder = feeds_folder
        self.new_feeds_folder = new_feeds_folder
        self.new_images_folder = new_images_folder
        self.videos_folder = videos_folder
        self.feeds_list = feeds_list
        self._root = None
        self._is_modified = False

    def __repr__(self):
        return (
            f"FeedHandler(filename = '{self.filename}'"
        )

    @property
    def root(self):
        """Ленивая загрузка корневого элемента."""
        if self._root is None:
            self._root = self._get_root(self.filename, self.feeds_folder)
        return self._root

    def delete_tags(self, tags: tuple):
        """Метод удаляет переданные теги из офферов."""
        tags_dict_count: dict = defaultdict(int)
        try:
            offers = self.root.findall('.//offer')
            for offer in offers:
                for tag in tags:
                    target_tag = offer.find(tag)
                    if target_tag is not None:
                        offer.remove(target_tag)
                        tags_dict_count[tag] += 1
                        self._is_modified = True

            logging.info('Удаление тегов в фиде %s:', self.filename)
            logging.info('Всего обработано офферов - %s', len(offers))
            logging.info(
                'Всего удалено тегов в фиде %s - %s',
                self.filename,
                tags_dict_count
            )
            return self

        except Exception as error:
            logging.error(
                'Неизвестная ошибка в delete_tags в файле %s: %s',
                self.filename,
                error
            )
            raise

    def delete_param(self, param: str):
        """Метод удаляет переданные параметры из офферов."""
        deleted_params = 0
        try:
            target_param = self.root.findall(f'.//*[@{param}]')

            if not target_param:
                logging.debug('В файле %s не найдено %s', self.filename, param)
                return self

            for element in target_param:
                element.attrib.pop(param, None)
                deleted_params += 1
                self._is_modified = True

            logging.info(
                'Удаление параметра %s в фиде %s:',
                param,
                self.filename
            )
            logging.info(
                'Всего найдено параметров в фиде - %s',
                len(target_param)
            )
            logging.info(
                'Всего удалено параметров в фиде %s - %s',
                self.filename,
                deleted_params
            )
            return self

        except Exception as error:
            logging.error(
                'Неожиданная ошибка в delete_param в файле %s: %s',
                self.filename,
                error
            )
            raise

    @time_of_function
    def remove_non_matching_offers(self, vendor_category: dict):
        """
        Метод фильтрует офферы по брендам и
        категориям и удаляет неподходящие.
        """
        try:
            categories = self.root.findall('.//category')
            all_categories = {
                category.get('id'): category.get('parentId')
                for category in categories
            }

            children_map = defaultdict(list)
            for category_id, parent_id in all_categories.items():
                if parent_id:
                    children_map[parent_id].append(category_id)

            def collect(category_id):
                result = set()
                stack = [str(category_id)]

                while stack:
                    cur = stack.pop()
                    result.add(cur)
                    for child in children_map.get(cur, []):
                        if child not in result:
                            stack.append(child)
                return result

            target: set = set()

            for vendor_cats in vendor_category.values():
                for category_id in vendor_cats:
                    if category_id == 'all':
                        target.update(all_categories.keys())
                        target.add(None)
                    elif str(category_id).isdigit():
                        target.update(collect(category_id))

            offers = self.root.findall('.//offer')
            parent = self.root.find('.//offers') or self.root
            removed = 0

            for offer in offers[:]:
                vendor = (offer.findtext('vendor') or '').strip().lower()
                category_id = offer.findtext('categoryId')

                if vendor not in vendor_category:
                    parent.remove(offer)
                    removed += 1
                    continue

                vendor_cats = vendor_category[vendor]

                if not vendor_cats:
                    parent.remove(offer)
                    removed += 1

                if 'all' in vendor_cats:
                    continue

                if category_id not in target:
                    parent.remove(offer)
                    removed += 1

            if removed:
                self._is_modified = True

            logger.info(
                'Удалено офферов по фильтрам: %s (%s)',
                removed, self.filename
            )
            return self

        except Exception as error:
            logger.error(
                'Ошибка в remove_non_matching_offers в файле %s: %s',
                self.filename,
                error
            )
            raise

    @time_of_function
    def replace_images(self):
        """Метод, заменяющий в фидах изображения на обрамленные."""
        deleted_images = 0
        input_images = 0
        try:
            image_dict = self._get_files_dict(self.new_images_folder)
            offers = self.root.findall('.//offer')

            for offer in offers:
                offer_id = offer.get('id')

                if offer_id and offer_id in image_dict:
                    pictures = offer.findall('picture')
                    for picture in pictures:
                        offer.remove(picture)
                    deleted_images += len(pictures)
                    picture_tag = ET.SubElement(offer, 'picture')
                    picture_tag.text = (
                        f'{IMAGE_FTP_ADDRESS}/{image_dict[offer_id]}'
                    )
                    input_images += 1
                    self._is_modified = True

            logging.info(
                '\nКоличество удаленных изображений - %s'
                '\nКоличество добавленных изображений - %s',
                deleted_images,
                input_images
            )
            return self
        except Exception as error:
            logging.error(
                'Неожиданная ошибка в replace_images в файле %s: %s',
                self.filename,
                error
            )
            raise

    def add_video(self):
        """Метод, добавляющий в оффер ссылку на видео в тег <video>."""
        input_videos = 0
        try:
            videos_dict = self._get_files_dict(self.videos_folder)
            offers = self.root.findall('.//offer')
            for offer in offers:
                offer_id = offer.get('id')

                if offer_id and offer_id in videos_dict:
                    video_tag = ET.SubElement(offer, 'video')
                    video_tag.text = (
                        f'{VIDEO_FTP_ADDRESS}/{videos_dict[offer_id]}'
                    )
                    input_videos += 1
                    self._is_modified = True
            logging.info('Количество добавленных видео - %s', input_videos)
            return self
        except Exception as error:
            logging.error(
                'Неожиданная ошибка в add_video в файле %s: %s',
                self.filename,
                error
            )
            raise

    def save(self, prefix: str):
        """Метод сохраняет файл, если были изменения."""
        try:
            if not self._is_modified:
                logger.info('Изменений нет — файл %s не сохранён',
                            self.filename)
                return self

            old_prefix = self.filename.split('_')[0]
            new_filename = self.filename.replace(old_prefix, prefix)

            self._save_xml(self.root, self.new_feeds_folder, new_filename)
            logger.info('Файл сохранён как %s', new_filename)

            self._is_modified = False
            return self
        except Exception as error:
            logging.error(
                'Неожиданная ошибка при сохранении файла %s: %s',
                self.filename,
                error
            )
            raise
