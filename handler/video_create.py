import logging
import random
import shutil
import time
from collections import defaultdict
from pathlib import Path

import cv2

from handler.constants import (FEEDS_FOLDER, FORMAT_VIDEO, FPS,
                               NEW_FEEDS_FOLDER, NEW_IMAGE_FOLDER,
                               TARGET_SECONDS_VIDEO, TOTAL_SECONDS_VIDEO,
                               VIDEO_CODEC, VIDEOS_FOLDER)
from handler.exceptions import DirectoryCreationError, EmptyFeedsListError
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()
cv2.setNumThreads(0)
GLOBAL_FILES_DICT_CACHE = None


class VideoCreater(FileMixin):

    def __init__(
        self,
        filenames: list,
        feeds_folder: str = FEEDS_FOLDER,
        new_feeds_folder: str = NEW_FEEDS_FOLDER,
        new_images_folder: str = NEW_IMAGE_FOLDER,
        videos_folder: str = VIDEOS_FOLDER,
        video_format: str = FORMAT_VIDEO,
        fps: int = FPS,
        video_codec: str = VIDEO_CODEC,
        target_second: int = TARGET_SECONDS_VIDEO,
        total_second: int = TOTAL_SECONDS_VIDEO

    ):
        self.filenames = filenames
        self.feeds_folder = feeds_folder
        self.new_feeds_folder = new_feeds_folder
        self.new_images_folder = new_images_folder
        self.videos_folder = videos_folder
        self.video_format = video_format
        self.fps = fps
        self.video_codec = video_codec
        self.target_second = target_second
        self.total_second = total_second
        self._root = None
        self._existing_videos_offers: set = set()
        self._existing_images: set = set()

    def _load_image(self, offer_id: str):
        global GLOBAL_FILES_DICT_CACHE

        if GLOBAL_FILES_DICT_CACHE is None:
            GLOBAL_FILES_DICT_CACHE = self._get_files_dict(
                self.new_images_folder
            )

        image_filename = GLOBAL_FILES_DICT_CACHE.get(offer_id)
        if not image_filename:
            return None

        image_path = Path(self.new_images_folder) / image_filename
        image = cv2.imread(str(image_path))
        return image

    def _create_single_video(
        self,
        target_offer,
        other_offers
    ) -> bool:
        """
        Создает одно видео для целевого оффера.
        Возвращает True если успешно, False если ошибка.
        """
        offer_id = target_offer.get('id')
        target_img = self._load_image(offer_id)
        if target_img is None:
            return False

        height, width = target_img.shape[:2]

        local_dir = Path('/tmp/videos')
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / f'{offer_id}.{self.video_format}'

        fourcc = cv2.VideoWriter_fourcc(*self.video_codec)
        video_writer = cv2.VideoWriter(
            str(local_path),
            fourcc,
            self.fps,
            (width, height)
        )

        if not video_writer.isOpened():
            logging.error('Не удалось создать VideoWriter для %s', local_path)
            return False

        try:
            target_frames = self.target_second * self.fps
            middle_seconds = self.total_second - self.target_second * 2
            total_middle_frames = middle_seconds * self.fps

            for _ in range(target_frames):
                video_writer.write(target_img)

            other_imgs = []
            for offer in other_offers:
                img = self._load_image(offer.get('id'))
                if img is not None:
                    other_imgs.append(cv2.resize(img, (width, height)))

            if other_imgs:
                if len(other_imgs) > middle_seconds:
                    other_imgs = random.sample(other_imgs, middle_seconds)

                items_count = len(other_imgs)
                seconds_per_item = middle_seconds // items_count
                extra_seconds = middle_seconds % items_count

                for idx, img in enumerate(other_imgs):
                    show_seconds = seconds_per_item + \
                        (1 if idx < extra_seconds else 0)
                    frames_to_write = show_seconds * self.fps

                    for _ in range(frames_to_write):
                        video_writer.write(img)
            else:
                for _ in range(total_middle_frames):
                    video_writer.write(target_img)

            for _ in range(target_frames):
                video_writer.write(target_img)

            video_writer.release()
            ftp_path = Path(self.videos_folder) / \
                f'{offer_id}.{self.video_format}'
            shutil.move(str(local_path), str(ftp_path))
            return True

        except Exception as error:
            if local_path.exists():
                local_path.unlink()
            logging.error('Ошибка видео %s: %s', offer_id, error)
            return False

        finally:
            if video_writer.isOpened():
                video_writer.release()
            time.sleep(0.01)

    def create_videos(self):
        created_video = 0
        failed_video = 0
        existing_videos = set()
        try:
            self._build_set(self.videos_folder, self._existing_videos_offers)
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.warning('Директория с видео отсутствует. Первый запуск')
        try:
            self._build_set(self.new_images_folder, self._existing_images)
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.error('Директория с изображениями отсутствует')
            raise

        for filename in self.filenames:
            root = self._get_root(filename, self.feeds_folder)
            offers = root.findall('.//offer')
            cat_ven_img_dict = defaultdict(list)
            for offer in offers:
                offer_id = str(offer.get('id'))
                vendor = offer.findtext('vendor')
                category_id = offer.findtext('categoryId')
                if offer_id in self._existing_videos_offers:
                    existing_videos.add(offer_id)
                    continue
                if offer_id not in self._existing_images:
                    continue
                cat_ven_img_dict[(category_id, vendor)].append(offer)

            for offers_in_group in cat_ven_img_dict.values():
                for index, target_offer in enumerate(offers_in_group):
                    offer_id = target_offer.get('id')

                    if offer_id in self._existing_videos_offers:
                        continue

                    other_offers = [
                        offer for i, offer in enumerate(offers_in_group)
                        if i != index
                    ]

                    ok = self._create_single_video(target_offer, other_offers)

                    if ok:
                        created_video += 1
                        self._existing_videos_offers.add(offer_id)
                    else:
                        failed_video += 1
        logging.info('Успешно созданных видео - %s', created_video)
        logging.info('Ошибок создания видео - %s', failed_video)
