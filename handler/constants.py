"""Константа названия магазина."""
NAME_OF_SHOP = 'citilink'

"""Константы стоковых названий директорий."""
FEEDS_FOLDER = 'temp_feeds'
PARSE_FEEDS_FOLDER = 'new_feeds'

IMAGE_FOLDER = 'old_images'
NEW_IMAGE_FOLDER = 'new_images'

"""Константы для среза массива (без выбросов)."""
UPPER_OUTLIER_PERCENTILE = 0.75
LOWER_OUTLIER_PERCENTILE = 0.25

"""Округление до указанного количества знаков после точки."""
DECIMAL_ROUNDING = 2

"""Список id офферов для available=False."""
UNAVAILABLE_OFFER_ID_LIST = ['1621720', '1621704', '1621686']

"""Данные для вставки в оффер."""
CUSTOM_LABEL = {'asia': {
    'name': ['Видеокарта', 'Роликовые'],
    'url': ['videokarta-biostar-pci-e-rx550-4gb-amd-rx550-4gb'],
    'id': ['2043409']
}, 'computer': {
    'name': ['IVIGO', 'Сетевые'],
    'url': ['product/ultrabuk-msi-summit-e13-flip-evo-a13mt-243us-i7-1360p-16gb-ssd1tb-13-4-2043424/?referrer=reattribution%3D1&amp;utm_term=split'],
    'id': ['1860372']
}}

"""SQL запросы для взаимодейсвтия с базой данных MySQL."""

# запросы на создание таблиц.
CREATE_LOGS_TABLE = '''
CREATE TABLE IF NOT EXISTS {table_name} (
    `id` INT NOT NULL AUTO_INCREMENT,
    `date` DATE NOT NULL,
    `feed_name` VARCHAR(255) NOT NULL,
    `category_id` BIGINT UNSIGNED NOT NULL,
    `parent_id` BIGINT UNSIGNED NULL,
    `count_offers` INT UNSIGNED NOT NULL,
    `min_price` BIGINT UNSIGNED NOT NULL,
    `clear_min_price` BIGINT UNSIGNED NOT NULL,
    `max_price` BIGINT UNSIGNED NOT NULL,
    `clear_max_price` BIGINT UNSIGNED NOT NULL,
    `avg_price` DECIMAL(20,2) UNSIGNED NOT NULL,
    `median_price` DECIMAL(20,2) UNSIGNED NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `unique_{table_name}_combo` (
    `date`, `feed_name`, `category_id`
),
KEY `idx_date` (`date`),
KEY `idx_category` (`category_id`)
);
'''

# запросы заполнения таблиц данными.
INSERT_LOGS = '''
INSERT INTO {table_name} (
    date,
    feed_name,
    category_id,
    parent_id,
    count_offers,
    min_price,
    clear_min_price,
    max_price,
    clear_max_price,
    avg_price,
    median_price
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    count_offers = VALUES(count_offers),
    min_price = VALUES(min_price),
    clear_min_price = VALUES(clear_min_price),
    max_price = VALUES(max_price),
    clear_max_price = VALUES(clear_max_price),
    avg_price = VALUES(avg_price),
    median_price = VALUES(median_price)
'''
