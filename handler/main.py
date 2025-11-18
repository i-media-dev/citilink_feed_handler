# from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.constants import (AUCTION_PREFIX, FEEDS_FOLDER, NEW_FEEDS_FOLDER,
                               NEW_PREFIX, PARAM_FOR_DELETE, TAGS_FOR_DELETE)
from handler.decorators import time_of_function, time_of_script
from handler.feeds_handler import FeedHandler
from handler.feeds_report import FeedReport
from handler.feeds_save import FeedSaver
from handler.reports_db import ReportDataBase
from handler.utils import get_filenames_list, save_to_database
from handler.vendor_category_dict import VENDOR_CATEGORY


@time_of_script
@time_of_function
def main():
    saver = FeedSaver()
    filenames = get_filenames_list(FEEDS_FOLDER)

    report = FeedReport(filenames)
    db_client = ReportDataBase()

    saver.save_xml()

    data = report.get_offers_report()

    save_to_database(db_client, data)

    for filename in filenames:
        handler = FeedHandler(filename)
        handler.processing_and_safe(
            NEW_PREFIX,
            TAGS_FOR_DELETE,
            PARAM_FOR_DELETE
        )
    # handler.process_feeds(CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST)

    new_filenames = get_filenames_list(NEW_FEEDS_FOLDER)
    # report_new = FeedReport(new_filenames)
    # report_new.full_outer_join_feeds()
    # report_new.inner_join_feeds()

    for filename in new_filenames:
        handler = FeedHandler(filename, feeds_folder=NEW_FEEDS_FOLDER)
        handler.remove_non_matching_offers(VENDOR_CATEGORY)
        handler.processing_and_safe(AUCTION_PREFIX,)


if __name__ == '__main__':
    main()
