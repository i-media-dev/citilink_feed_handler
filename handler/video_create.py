import logging
import random
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from pathlib import Path

import cv2
import numpy as np

from handler.constants import (FEEDS_FOLDER, FORMAT_VIDEO, FPS,
                               NEW_FEEDS_FOLDER, NEW_IMAGE_FOLDER,
                               TARGET_SECONDS_VIDEO, TOTAL_SECONDS_VIDEO,
                               VIDEO_CODEC, VIDEOS_FOLDER)
from handler.exceptions import DirectoryCreationError, EmptyFeedsListError
from handler.logging_config import setup_logging
from handler.mixins import FileMixin

setup_logging()


def _video_worker(args):
    creator, target_offer, other_offers = args
    return creator._create_single_video(target_offer, other_offers)


class VideoCreater(FileMixin):

    def __init__(
        self,
        filename: str,
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
        self.filename = filename
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

    @property
    def root(self):
        """Ленивая загрузка корневого элемента."""
        if self._root is None:
            self._root = self._get_root(self.filename, self.feeds_folder)
        return self._root

    def _load_image(self, offer_id: str) -> np.ndarray | None:
        """
        Загружает изображение по ID оффера.
        Возвращает numpy array или None если ошибка.
        """
        images_dict = self._get_files_dict(self.new_images_folder)
        image_filename = images_dict.get(offer_id)

        if not image_filename:
            logging.warning(
                'Изображение не найдено для offer_id: %s',
                offer_id
            )
            return None

        image_path = Path(self.new_images_folder) / image_filename

        if not image_path.exists():
            logging.warning('Изображение не найдено: %s', image_path)
            return None
        try:
            img = cv2.imread(str(image_path))

            if img is None:
                logging.warning(
                    'Не удалось загрузить изображение: %s',
                    image_path
                )
                return None
            return img
        except Exception as error:
            logging.error(
                'Ошибка загрузки изображения %s: %s',
                offer_id,
                error
            )
            return None

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

        output_dir = Path(self.videos_folder)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f'{offer_id}.{self.video_format}'
        fourcc = cv2.VideoWriter_fourcc(*self.video_codec)
        video_writer = cv2.VideoWriter(
            str(output_path),
            fourcc,
            self.fps,
            (width, height)
        )

        if not video_writer.isOpened():
            logging.error('Не удалось создать VideoWriter для %s', output_path)
            return False

        video_writer.set(cv2.VIDEOWRITER_PROP_QUALITY, 1)

        try:
            target_frames = self.target_second * self.fps
            other_seconds = self.total_second - (2 * self.target_second)

            for _ in range(target_frames):
                video_writer.write(target_img)

            if other_offers:
                other_imgs = []
                for offer in other_offers:
                    img = self._load_image(offer.get('id'))
                    if img is not None:
                        img_resized = cv2.resize(img, (width, height))
                        other_imgs.append(img_resized)

                if other_imgs:
                    total_other_frames = other_seconds * self.fps
                    max_others_to_show = other_seconds
                    if len(other_imgs) > max_others_to_show:
                        other_imgs = random.sample(
                            other_imgs, max_others_to_show)

                    frames_per_other = total_other_frames // len(other_imgs)
                    remaining_frames = total_other_frames % len(other_imgs)

                    for img in other_imgs:
                        for _ in range(frames_per_other):
                            video_writer.write(img)

                    if remaining_frames > 0:
                        for i in range(remaining_frames):
                            video_writer.write(other_imgs[i % len(other_imgs)])

                else:
                    for _ in range(other_seconds * self.fps):
                        video_writer.write(target_img)
            else:
                for _ in range(other_seconds * self.fps):
                    video_writer.write(target_img)

            for _ in range(target_frames):
                video_writer.write(target_img)

            video_writer.release()
            # logging.info('Создано видео: %s', output_path)
            return True

        except Exception as error:
            logging.error(
                'Ошибка при создании видео для %s: %s',
                offer_id,
                error
            )
            video_writer.release()

            if output_path.exists():
                output_path.unlink()

            return False

    def create_videos(self):
        created_video = 0
        failed_video = 0
        existing_video = 0
        offers = self.root.findall('.//offer')
        cat_ven_img_dict = defaultdict(list)
        try:
            self._build_set(
                self.videos_folder,
                self._existing_videos_offers
            )
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.warning('Директория с видео отсутствует. Первый запуск')
        try:
            self._build_set(
                self.new_images_folder,
                self._existing_images
            )
        except (DirectoryCreationError, EmptyFeedsListError):
            logging.error('Директория с изображениями отсутствует')
            raise
        for offer in offers:
            offer_id = str(offer.get('id'))
            vendor = offer.findtext('vendor')
            category_id = offer.findtext('categoryId')
            if offer_id in self._existing_videos_offers:
                existing_video += 1
                continue
            if offer_id not in self._existing_images:
                continue
            cat_ven_img_dict[(category_id, vendor)].append(offer)
        tasks = []
        for (_, _), offers_in_group in cat_ven_img_dict.items():

            for index, target_offer in enumerate(offers_in_group):
                offer_id = target_offer.get('id')

                if offer_id in self._existing_videos_offers:
                    continue

                other_offers = [
                    offer for i_offer, offer in enumerate(offers_in_group)
                    if i_offer != index
                ]

                tasks.append((self, target_offer, other_offers))
        if not tasks:
            logging.info(
                f'Уже созданных видео - {existing_video}, '
                f'Создано видео - 0, '
                f'Ошибок создания видео - 0'
            )
            return
        workers = max(cpu_count() - 1, 2)
        logging.debug(f'🚀 Параллельная генерация: {workers} процессов')

        with Pool(workers) as pool:
            results = pool.map(_video_worker, tasks)
        created_video = sum(results)
        failed_video = len(results) - created_video

        logging.info(
            f'Уже созданных видео - {existing_video}, '
            f'Создано видео - {created_video}, '
            f'Ошибок создания видео - {failed_video}'
        )
