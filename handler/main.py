# from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.constants import PARAM_FOR_DELETE, TAGS_FOR_DELETE
from handler.decorators import time_of_function, time_of_script
from handler.utils import initialize_components, save_to_database


@time_of_script
@time_of_function
def main():
    saver, handler, db_client = initialize_components()
    saver.save_xml()
    data = handler.get_offers_report()
    save_to_database(db_client, data)
    handler.delete_tags(TAGS_FOR_DELETE)
    handler.delete_param(PARAM_FOR_DELETE)
    # handler.process_feeds(CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST)
    handler.full_outer_join_feeds()
    handler.inner_join_feeds()


if __name__ == '__main__':
    main()
