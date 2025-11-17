import logging
from datetime import datetime as dt

import numpy as np

from handler.calculation import clear_avg, clear_max, clear_median, clear_min
from handler.constants import (CREATE_CATALOG_TABLE, CREATE_REPORTS_TABLE,
                               DATE_FORMAT, DECIMAL_ROUNDING, INSERT_CATALOG,
                               INSERT_REPORT, NAME_OF_SHOP)
from handler.decorators import connection_db, time_of_function, try_except
from handler.exceptions import TableNameError
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()


class ReportDataBase(FileMixin):
    """Класс, предоставляющий интерфейс для работы с базой данных"""

    def __init__(self, shop_name: str = NAME_OF_SHOP):
        self.shop_name = shop_name

    def __repr__(self):
        return (
            f"ReportDataBase(shop_name='{self.shop_name}', "
        )

    @time_of_function
    @try_except
    def get_offers_report(
        self,
        filenames: list,
        feeds_folder: str
    ) -> list[dict]:
        """Метод, формирующий отчет по офферам."""
        result = []
        date_str = (dt.now()).strftime(DATE_FORMAT)
        for filename in filenames:
            tree = self._get_tree(filename, feeds_folder)
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
                    'feed_name': filename,
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

    @connection_db
    def _allowed_tables(self, cursor=None) -> list:
        """
        Защищенный метод, возвращает список существующих
        таблиц в базе данных.
        """
        cursor.execute('SHOW TABLES')
        return [table[0] for table in cursor.fetchall()]

    @connection_db
    def _create_table_if_not_exists(
        self,
        prefix,
        sql_pattern,
        cursor=None
    ) -> str:
        """
        Защищенный метод, создает таблицу в базе данных, если ее не существует.
        Если таблица есть в базе данных - возварщает ее имя.
        """
        table_name = f'{prefix}_{self.shop_name}'
        if table_name in self._allowed_tables():
            logging.info(f'Таблица {table_name} найдена в базе')
            return table_name
        create_table_query = sql_pattern.format(table_name=table_name)
        cursor.execute(create_table_query)
        logging.info(f'Таблица {table_name} успешно создана')
        return table_name

    def insert_catalog(self, data):
        table_name = self._create_table_if_not_exists(
            'catalog_categories',
            CREATE_CATALOG_TABLE
        )
        query = INSERT_CATALOG.format(table_name=table_name)
        params = [
            (
                item['category_id'],
                item['category_name']
            ) for item in data
        ]
        return query, params

    def insert_reports(self, data):
        table_name = self._create_table_if_not_exists(
            'reports_offers',
            CREATE_REPORTS_TABLE
        )
        query = INSERT_REPORT.format(table_name=table_name)
        params = [
            (
                item['date'],
                item['feed_name'],
                item['category_id'],
                item['parent_id'],
                item['count_offers'],
                item['min_price'],
                item['clear_min_price'],
                item['max_price'],
                item['clear_max_price'],
                item['avg_price'],
                item['clear_avg_price'],
                item['median_price'],
                item['clear_median_price']
            ) for item in data
        ]
        return query, params

    @connection_db
    def save_to_database(
        self,
        query_data: tuple,
        cursor=None
    ) -> None:
        """Метод сохраняется обработанные данные в базу данных."""
        query, params = query_data
        if isinstance(params, list):
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)
        logging.info('✅ Данные успешно сохранены!')

    @connection_db
    def clean_database(self, cursor=None, **tables: bool) -> None:
        """
        Метод очищает базу данных,
        не удаляя сами таблицы
        """
        try:
            existing_tables = self._allowed_tables()
            for table_name, should_clean in tables.items():
                if should_clean and table_name in existing_tables:
                    cursor.execute(f'DELETE FROM {table_name}')
                    logging.info(f'Таблица {table_name} очищена')
                else:
                    raise TableNameError(
                        f'В базе данных отсутствует таблица {table_name}.'
                    )
        except Exception as e:
            logging.error(f'Ошибка очистки: {e}')
            raise
