import logging
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime as dt

import numpy as np

from handler.calculation import clear_avg, clear_max, clear_median, clear_min
from handler.constants import (DATE_FORMAT, DECIMAL_ROUNDING, FEEDS_FOLDER,
                               NEW_FEEDS_FOLDER, NEW_PREFIX)
from handler.decorators import time_of_function, try_except
from handler.exceptions import StructureXMLError
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
        feeds_folder: str = FEEDS_FOLDER,
        new_feeds_folder: str = NEW_FEEDS_FOLDER,
        feeds_list: tuple[str, ...] = FEEDS
    ) -> None:
        self.feeds_folder = feeds_folder
        self.new_feeds_folder = new_feeds_folder
        self.feeds_list = feeds_list

    def _indent(self, elem, level=0) -> None:
        """Защищенный метод, расставляет правильные отступы в XML файлах."""
        i = '\n' + level * '  '
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + '  '
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                self._indent(child, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    def _save_xml(self, elem, file_folder, filename) -> None:
        """Защищенный метод, сохраняет отформатированные файлы."""
        root = elem
        self._indent(root)
        formatted_xml = ET.tostring(root, encoding='unicode')
        file_path = self._make_dir(file_folder)
        with open(
            file_path / filename,
            'w',
            encoding='utf-8'
        ) as f:
            f.write(formatted_xml)

    def _super_feed(self, filenames: list[str]) -> tuple:
        """Защищенный метод, создает шаблон фида с пустыми offers."""
        first_file_tree = self._get_tree(filenames[0], self.feeds_folder)
        root = first_file_tree.getroot()
        offers = root.find('.//offers')
        if offers is not None:
            offers.clear()
        else:
            raise StructureXMLError(
                'Тег пуст или структура фида не соответствует ожидаемой.'
            )
        return root, offers

    def _collect_all_offers(self, filenames: list[str]) -> tuple[dict, dict]:
        """
        Защищенный метод, подсчитывает встречался ли оффер в том или ином фиде.
        """
        offer_counts: dict = defaultdict(int)
        all_offers = {}
        for file_name in filenames:
            tree = self._get_tree(file_name, self.feeds_folder)
            root = tree.getroot()
            for offer in root.findall('.//offer'):
                offer_id = offer.get('id')
                if offer_id:
                    offer_counts[offer_id] += 1
                    all_offers[offer_id] = offer
        return offer_counts, all_offers

    @time_of_function
    @try_except
    def inner_join_feeds(self) -> bool:
        """
        Метод, объединяющий все офферы в один фид
        по принципу inner join.
        """
        filenames: list[str] = self._get_filenames_list(self.feeds_folder)
        offer_counts, all_offers = self._collect_all_offers(filenames)
        root, offers = self._super_feed(filenames)
        for offer_id, count in offer_counts.items():
            if count == len(filenames):
                offers.append(all_offers[offer_id])
        self._save_xml(root, self.new_feeds_folder, 'inner_join_feed.xml')
        return True

    @time_of_function
    @try_except
    def full_outer_join_feeds(self) -> bool:
        """
        Метод, объединяющий все офферы в один фид
        по принципу full outer join.
        """
        filenames: list[str] = self._get_filenames_list(self.feeds_folder)
        _, all_offers = self._collect_all_offers(filenames)
        root, offers = self._super_feed(filenames)
        for offer in all_offers.values():
            offers.append(offer)
        self._save_xml(root, self.new_feeds_folder, 'full_outer_join_feed.xml')
        return True

    @time_of_function
    @try_except
    def get_offers_report(self) -> list[dict]:
        """Метод, формирующий отчет по офферам."""
        result = []
        date_str = (dt.now()).strftime(DATE_FORMAT)
        filenames = self._get_filenames_list(self.feeds_folder)
        for file_name in filenames:
            tree = self._get_tree(file_name, self.feeds_folder)
            root = tree.getroot()
            category_data = {}
            all_categories = {}

            for category in root.findall('.//category'):
                category_name = category.text
                category_id = category.get('id')
                parent_id = category.get('parentId')
                all_categories[category_id] = parent_id
                category_data[category_id] = {
                    'prices': [],
                    'category_name': category_name,
                    'offers_count': 0
                }

            for offer in root.findall('.//offer'):
                category_id = offer.findtext('categoryId')
                price = offer.findtext('price')
                if category_id and price:
                    if category_id not in category_data:
                        category_data[category_id] = {
                            'prices': [],
                            'category_name': '',
                            'offers_count': 0
                        }
                    category_data[category_id]['prices'].append(int(price))
                    category_data[category_id]['offers_count'] += 1

            def aggregate_data(category_id):
                prices = category_data[category_id]['prices'].copy()
                offers_count = category_data[category_id]['offers_count']

                for child_id, parent_id in all_categories.items():
                    if parent_id == category_id:
                        child_prices, child_count = aggregate_data(child_id)
                        prices.extend(child_prices)
                        offers_count += child_count
                category_data[category_id]['prices'] = prices
                category_data[category_id]['offers_count'] = offers_count
                return prices, offers_count

            root_categories = [
                cat_id for cat_id, parent_id in all_categories.items()
                if parent_id is None
            ]
            for root_id in root_categories:
                aggregate_data(root_id)

            for category_id, data in category_data.items():
                count_offers = data['offers_count']
                price_list = data['prices']
                parent_id = all_categories.get(category_id)
                category_name = data['category_name']

                result.append({
                    'date': date_str,
                    'feed_name': file_name,
                    'category_name': category_name,
                    'category_id': category_id,
                    'parent_id': parent_id,
                    'count_offers': count_offers,
                    'min_price': min(price_list) if price_list else 0,
                    'clear_min_price': clear_min(price_list)
                    if price_list else 0,
                    'max_price': max(price_list) if price_list else 0,
                    'clear_max_price': clear_max(price_list)
                    if price_list else 0,
                    'avg_price': round(
                        sum(price_list) / len(price_list), DECIMAL_ROUNDING
                    ) if price_list else 0,
                    'clear_avg_price': clear_avg(price_list)
                    if price_list else 0,
                    'median_price': round(
                        np.median(price_list), DECIMAL_ROUNDING
                    ) if price_list else 0,
                    'clear_median_price': clear_median(price_list)
                    if price_list else 0
                })
        return result

    def delete_tags(self, tags):
        tags_dict_count = defaultdict(int)
        tags_non_dict_count = defaultdict(int)
        filenames: list[str] = self._get_filenames_list(self.feeds_folder)
        try:
            for filename in filenames:
                tree = self._get_tree(filename, self.feeds_folder)
                root = tree.getroot()
                offers = root.findall('.//offer')

                if not offers:
                    logging.debug('В файле %s не найдено offers', filename)
                    continue

                for offer in offers:
                    for tag in tags:
                        target_tag = offer.find(tag)

                        if target_tag is None:
                            tags_non_dict_count[tag] += 1
                            continue
                        offer.remove(target_tag)
                        tags_dict_count[tag] += 1
                prefix = filename.split('_')[0]
                new_filename = filename.replace(prefix, NEW_PREFIX)
                self._save_xml(root, self.new_feeds_folder, new_filename)
                logging.info(
                    '\n%s переименован в  %s'
                    '\nВсего обработано офферов - %s'
                    '\nВсего удалено тегов - %s'
                    '\nВсего отсутствовавших тегов - %s',
                    filename,
                    new_filename,
                    len(offers),
                    tags_dict_count,
                    tags_non_dict_count
                )
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise

    def delete_param(self, param):
        deleted_params = 0
        none_params = 0
        filenames: list[str] = self._get_filenames_list(self.new_feeds_folder)
        try:
            for filename in filenames:
                tree = self._get_tree(filename, self.new_feeds_folder)
                root = tree.getroot()
                parent_physicals = root.findall(f'.//*[@{param}]')

                if parent_physicals is None:
                    none_params += 1
                    continue

                for element in parent_physicals:
                    element.attrib.pop(param, None)
                    deleted_params += 1

                self._save_xml(root, self.new_feeds_folder, filename)
                logging.info(
                    '\nПараметр - %s'
                    '\nВсего найдено параметров в фиде %s - %s'
                    '\nВсего удалено параметров - %s'
                    '\nВсего отсутствовавших параметров - %s',
                    param,
                    filename,
                    len(parent_physicals),
                    deleted_params,
                    none_params
                )
                deleted_params = 0
                none_params = 0
        except Exception as error:
            logging.error('Неизвестная ошибка: %s', error)
            raise
