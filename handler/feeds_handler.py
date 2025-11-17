import logging
from collections import defaultdict

from handler.constants import FEEDS_FOLDER, NEW_FEEDS_FOLDER, NEW_PREFIX
from handler.decorators import time_of_function
from handler.feeds import FEEDS
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()


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
        feeds_list: tuple[str, ...] = FEEDS
    ):
        self.filename = filename
        self.feeds_folder = feeds_folder
        self.new_feeds_folder = new_feeds_folder
        self.feeds_list = feeds_list
        self._root = None

    def __repr__(self):
        return (
            f"FeedHandler(filename = '{self.filename}', "
            f"feeds_folder='{self.feeds_folder}', "
            f"new_feeds_folder='{self.new_feeds_folder}'), "
            f"feeds_list='{self.feeds_list}', "
            f"root='{self._root}'."
        )

    @property
    def root(self):
        """Ленивая загрузка корневого элемента."""
        if self._root is None:
            self._root = self._get_root(self.filename, self.feeds_folder)
        return self._root

    @property
    def offers(self) -> list:
        """Доступ к офферам (для анализа, поиска)."""
        return self.root.findall('.//offer')

    @time_of_function
    def delete_tags(self, tags):
        """Метод удаляет переданные теги из офферов."""
        tags_dict_count = defaultdict(int)
        tags_non_dict_count = defaultdict(int)
        try:
            offers = self.root.findall('.//offer')
            for offer in offers:
                for tag in tags:
                    target_tag = offer.find(tag)

                    if target_tag is None:
                        tags_non_dict_count[tag] += 1
                        continue
                    offer.remove(target_tag)
                    tags_dict_count[tag] += 1
            prefix = self.filename.split('_')[0]
            new_filename = self.filename.replace(prefix, NEW_PREFIX)
            self._save_xml(self.root, self.new_feeds_folder, new_filename)
            logging.info(
                '\n%s переименован в  %s'
                '\nВсего обработано офферов - %s'
                '\nВсего удалено тегов - %s'
                '\nВсего отсутствовавших тегов - %s',
                self.filename,
                new_filename,
                len(self.offers),
                tags_dict_count,
                tags_non_dict_count
            )
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise

    @time_of_function
    def delete_param(self, param):
        """Метод удаляет переданные параметры из офферов."""
        deleted_params = 0
        try:
            parent_physicals = self.root.findall(f'.//*[@{param}]')

            if parent_physicals is None:
                logging.debug('В файле %s не найдено %s', self.filename, param)
                return

            for element in parent_physicals:
                element.attrib.pop(param, None)
                deleted_params += 1

            self._save_xml(self.root, self.new_feeds_folder, self.filename)
            logging.info(
                '\nПараметр - %s'
                '\nВсего найдено параметров в фиде %s - %s'
                '\nВсего удалено параметров - %s',
                param,
                self.filename,
                len(parent_physicals),
                deleted_params,
            )
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise
