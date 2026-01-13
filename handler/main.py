import logging

# from handler.constants import CUSTOM_LABEL, UNAVAILABLE_OFFER_ID_LIST
from handler.constants import (AUCTION_PREFIX, FEEDS_FOLDER, IMAGE_FOLDER,
                               NEW_FEEDS_FOLDER, NEW_PREFIX, PARAM_FOR_DELETE,
                               TAGS_FOR_DELETE)
from handler.decorators import time_of_script
from handler.feeds_handler import FeedHandler
from handler.feeds_report import FeedReport
from handler.feeds_save import FeedSaver
from handler.image_handler import FeedImage
from handler.logging_config import setup_logging
from handler.reports_db import ReportDataBase
from handler.utils import get_filenames_list, save_to_database
from handler.vendor_category_dict import VENDOR_CATEGORY
from handler.video_create import VideoCreater

setup_logging()


@time_of_script
def main():
    saver = FeedSaver()
    db_client = ReportDataBase()
    saver.save_xml()
    filenames = get_filenames_list(FEEDS_FOLDER)
    report_client = FeedReport(filenames)
    data = report_client.get_offers_report()
    save_to_database(db_client, data)

    if not filenames:
        logging.error('Директория %s пуста', FEEDS_FOLDER)
        raise FileNotFoundError(
            f'Директория {FEEDS_FOLDER} не содержит файлов'
        )

    image_client = FeedImage(filenames, images=[])
    image_client.get_images()
    images = get_filenames_list(IMAGE_FOLDER)

    if not images:
        logging.error('Директория %s пуста', IMAGE_FOLDER)
        raise FileNotFoundError(
            f'Директория {IMAGE_FOLDER} не содержит файлов'
        )
    image_client.images = images
    image_client.add_frame()
    video_client = VideoCreater(filenames)
    video_client.create_videos()

    for filename in filenames:
        handler_client = FeedHandler(filename)
        (
            handler_client
            .delete_tags(TAGS_FOR_DELETE)
            .delete_param(PARAM_FOR_DELETE)
            # .replace_images()
            # .add_video()
            .save(prefix=NEW_PREFIX)
        )
    new_filenames = get_filenames_list(NEW_FEEDS_FOLDER)
    report_client.filenames = new_filenames
    report_client.join_feeds('full_outer')
    report_client.join_feeds('inner')

    for filename in new_filenames:
        handler = FeedHandler(filename, feeds_folder=NEW_FEEDS_FOLDER)
        (
            handler
            .remove_non_matching_offers(VENDOR_CATEGORY)
            .save(prefix=AUCTION_PREFIX)
        )


if __name__ == '__main__':
    main()
