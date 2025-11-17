# from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.constants import FEEDS_FOLDER, PARAM_FOR_DELETE, TAGS_FOR_DELETE
from handler.decorators import time_of_function, time_of_script
from handler.feeds_handler import FeedHandler
from handler.feeds_report import FeedReport
from handler.feeds_save import FeedSaver
from handler.reports_db import ReportDataBase
from handler.utils import get_filenames_list, save_to_database


@time_of_script
@time_of_function
def main():
    filenames = get_filenames_list(FEEDS_FOLDER)

    saver = FeedSaver()
    report = FeedReport(filenames)
    db_client = ReportDataBase()

    saver.save_xml()

    data = report.get_offers_report()
    report.full_outer_join_feeds()
    report.inner_join_feeds()

    save_to_database(db_client, data)

    for filename in filenames:
        handler = FeedHandler(filename)
        handler.processing_and_safe(TAGS_FOR_DELETE, PARAM_FOR_DELETE)
    # handler.process_feeds(CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST)


if __name__ == '__main__':
    main()
