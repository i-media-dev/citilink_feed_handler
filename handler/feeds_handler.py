import logging
from collections import defaultdict

from handler.constants import FEEDS_FOLDER, NEW_FEEDS_FOLDER
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
        self._is_modified = False

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
    def processing_and_safe(
        self,
        new_prefix,
        tags_to_delete=None,
        params_to_delete=None
    ):
        if tags_to_delete:
            self._delete_tags(tags_to_delete)

        if params_to_delete:
            for param in params_to_delete:
                self._delete_param(param)

        if self._is_modified:
            prefix = self.filename.split('_')[0]
            new_filename = self.filename.replace(prefix, new_prefix)
            self._save_xml(self.root, self.new_feeds_folder, new_filename)
            self._is_modified = False
            logging.info('Файл сохранен как %s', new_filename)
        else:
            logging.info('Изменений нет, файл не сохранен')

    def _delete_tags(self, tags):
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
                    self._is_modified = True

            logging.info(
                '\nУдаление тегов в файле %s:'
                '\nВсего обработано офферов - %s'
                '\nВсего удалено тегов - %s'
                '\nВсего отсутствовавших тегов - %s',
                self.filename,
                len(offers),
                tags_dict_count,
                tags_non_dict_count
            )
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise

    def _delete_param(self, param):
        """Метод удаляет переданные параметры из офферов."""
        deleted_params = 0
        try:
            parent_physicals = self.root.findall(f'.//*[@{param}]')

            if not parent_physicals:
                logging.debug('В файле %s не найдено %s', self.filename, param)
                return

            for element in parent_physicals:
                element.attrib.pop(param, None)
                deleted_params += 1
                self._is_modified = True

            logging.info(
                '\nУдаление параметра в файле %s:'
                '\nПараметр - %s'
                '\nВсего найдено параметров - %s'
                '\nВсего удалено параметров - %s',
                self.filename,
                param,
                len(parent_physicals),
                deleted_params,
            )
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise

    def remove_non_matching_offers(self, brands_dict: dict):
        """
        Удаляет из фида офферы, которые не
        соответствуют критериям брендов и категорий
        """
        try:
            all_categories = {}
            categories = self.root.findall('.//category')
            for category in categories:
                cat_id = category.get('id')
                parent_id = category.get('parentId')
                all_categories[cat_id] = parent_id

            category_children = defaultdict(list)
            for cat_id, parent_id in all_categories.items():
                if parent_id:
                    category_children[parent_id].append(cat_id)

            def get_all_child_categories(parent_id):
                children = set()
                stack = [str(parent_id)]

                while stack:
                    current_parent = stack.pop()
                    children.add(current_parent)
                    for child_id in category_children.get(current_parent, []):
                        if child_id not in children:
                            children.add(child_id)
                            stack.append(child_id)
                return children

            all_target_categories = set()

            for category_list in brands_dict.values():
                for category_id in category_list:
                    if category_id == 'all':
                        all_target_categories.update(all_categories.keys())
                        all_target_categories.add(None)
                    elif isinstance(category_id, int) or category_id.isdigit():
                        all_target_categories.update(
                            get_all_child_categories(category_id)
                        )

            offers = self.root.findall('.//offer')
            offers_parent = self.root.find('.//offers') or self.root
            initial_count = len(offers)
            removed_count = 0

            for offer in offers[:]:
                vendor = offer.findtext('vendor')
                category_id = offer.findtext('categoryId')

                if vendor is None or category_id is None:
                    offers_parent.remove(offer)
                    removed_count += 1
                    continue

                vendor_lower = vendor.strip().lower()
                vendor_in_dict = vendor_lower in brands_dict

                if not vendor_in_dict:
                    offers_parent.remove(offer)
                    removed_count += 1
                    continue

                vendor_categories = brands_dict[vendor_lower]

                if 'all' in vendor_categories:
                    continue
                elif not vendor_categories:
                    offers_parent.remove(offer)
                    removed_count += 1
                    continue
                else:
                    category_in_target = category_id in all_target_categories
                    if not category_in_target:
                        offers_parent.remove(offer)
                        removed_count += 1

            remaining_count = initial_count - removed_count
            logging.info(
                'Удалено %s офферов из %s. Осталось: %s',
                removed_count, initial_count, remaining_count
            )

            if removed_count > 0:
                self._is_modified = True

        except Exception as error:
            logging.error('Ошибка в remove_non_matching_offers: %s', error)
            raise
