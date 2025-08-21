from handler.citilink_db import XMLDataBase
from handler.citilink_handler import XMLHandler
# from handler.citilink_image import XMLImage
from handler.citilink_save import XMLSaver
# from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.decorators import time_of_function


@time_of_function
def main():
    saver = XMLSaver()
    handler = XMLHandler()
    db_client = XMLDataBase()
    # image_client = XMLImage()
    saver.save_xml()
    data = handler.get_offers_report()
    queries = [
        db_client.insert_reports(data),
        db_client.insert_catalog(data)
    ]
    for query in queries:
        db_client.save_to_database(query)
    # handler.process_feeds(CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST)
    # handler.full_outer_join_feeds()
    # handler.inner_join_feeds()
    # image_client.get_images()


if __name__ == '__main__':
    main()
