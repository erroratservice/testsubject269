import threading
import time

from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError

from bot import DATABASE_URL, LOGGER


class DbManger:
    def __init__(self):
        self.__conn = MongoClient(DATABASE_URL)
        self.__db = self.__conn.mltb
        self.__user_settings = self.__db.user_settings
        self.__rss_feeds = self.__db.rss_feeds
        self.__scanned_files = self.__db.scanned_files # New collection
        self.__lock = threading.RLock()
        self.__scanned_files.create_index('file_name', unique=True) # Ensure filenames are unique

    # Scanned Files DB
    async def add_scanned_file(self, file_name: str) -> bool:
        with self.__lock:
            try:
                self.__scanned_files.insert_one({'file_name': file_name})
                return True # Return True if insertion is successful
            except PyMongoError:
                return False # Return False if the file_name already exists or another error occurs

    async def is_file_scanned(self, file_name: str) -> bool:
        with self.__lock:
            return self.__scanned_files.find_one({'file_name': file_name}) is not None

    async def clear_scanned_files(self):
        with self.__lock:
            self.__scanned_files.delete_many({})

    # User Settings
    def uset_get_setting(self, user_id: int, key: str):
        with self.__lock:
            user = self.__user_settings.find_one({'_id': user_id})
            if user and key in user:
                return user[key]
            return None

    def uset_set_setting(self, user_id: int, key: str, value):
        with self.__lock:
            self.__user_settings.update_one({'_id': user_id}, {'$set': {key: value}}, upsert=True)

    # RSS Feeds
    def rss_get_feeds(self):
        with self.__lock:
            return list(self.__rss_feeds.find())

    def rss_add_feed(self, title: str, feed_url: str, last_update: int, last_hash: str):
        with self.__lock:
            self.__rss_feeds.insert_one({
                'title': title,
                'feed_url': feed_url,
                'last_update': last_update,
                'last_hash': last_hash
            })

    def rss_update_feed(self, feed_url: str, last_update: int, last_hash: str):
        with self.__lock:
            self.__rss_feeds.update_one({'feed_url': feed_url}, {'$set': {'last_update': last_update, 'last_hash': last_hash}})

    def rss_remove_feed(self, feed_url: str):
        with self.__lock:
            self.__rss_feeds.delete_one({'feed_url': feed_url})


db_handler = DbManger()
