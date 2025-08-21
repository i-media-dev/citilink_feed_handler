from handler.xml_database import XMLDataBase
from handler.xml_handler import XMLHandler
from handler.xml_image import XMLImage
from handler.xml_saver import XMLSaver
from handler.feeds import FEEDS
from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.decorators import time_of_function


@time_of_function
def main():
    # saver = XMLSaver(FEEDS)
    handler = XMLHandler()
    db_client = XMLDataBase()
    # image_client = XMLImage()
    # saver.save_xml()
    # handler.process_feeds(CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST)
    data = handler.get_offers_report()
    handler.save_to_json(data)
    # handler.full_outer_join_feeds()
    # handler.inner_join_feeds()
    db_client.insert_data(data)
    # image_client.get_images()


if __name__ == '__main__':
    main()
