from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from dotenv import dotenv_values
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
from datetime import datetime
import os
import re
from bot import (
    user_data,
    rss_dict,
    LOGGER,
    BOT_ID,
    config_dict,
    aria2_options,
    qbit_options,
)

def sanitize_filename(filename):
    """Clean filename sanitization matching channel_leech.py exactly - NO lowercase conversion"""
    if not filename:
        return ""
    
    import re
    emoji_pattern = re.compile(
        r'['
        r'\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF'
        r'\U00002702-\U000027B0'
        r'\U000024C2-\U0001F251'
        r'\U0001F900-\U0001F9FF'
        r'\U0001FA00-\U0001FA6F'
        r'\U0001F1E0-\U0001F1FF'
        r'\u2600-\u26FF\u2700-\u27BF'
        r']+', flags=re.UNICODE
    )
    
    # Remove emojis
    filename = emoji_pattern.sub('', filename)
    # Replace special chars with dots
    filename = filename.replace('+', '.')
    filename = filename.replace('-', '.')
    filename = filename.replace(' ', '.')
    # Remove brackets and invalid chars
    filename = re.sub(r'[\[\]\(\)\{\}]', '', filename)
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Collapse multiple dots
    filename = re.sub(r'\.{2,}', '.', filename)
    # Strip leading/trailing dots
    filename = filename.strip('.')
    
    if not filename:
        filename = "file"
    return filename 

def get_duplicate_check_name(file_info):
    """
    Return the sanitized base file name for duplication check.
    If there's a caption_first_line, use it; otherwise fall back to file_name.
    """
    base = None
    caption_line = file_info.get("caption_first_line")
    if caption_line:
        base = caption_line.strip()
    
    if not base:
        base = file_info.get("file_name") or ""
    
    # Sanitize WITHOUT lowercasing (sanitize_filename no longer lowercases)
    sanitized = sanitize_filename(base)
    # Remove extension for base name
    base_name = os.path.splitext(sanitized)[0]
    
    return base_name
    
COMMON_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".wmv"]

class DbManager:
    def __init__(self):
        self._return = False
        self._db = None
        self._conn = None

    async def connect(self):
        try:
            if config_dict["DATABASE_URL"]:
                if self._conn is not None:
                    await self._conn.close()
                self._conn = AsyncIOMotorClient(
                    config_dict["DATABASE_URL"], server_api=ServerApi("1")
                )
                self._db = self._conn.ghost
                self._return = False
            else:
                self._return = True
        except PyMongoError as e:
            LOGGER.error(f"Error in DB connection: {e}")
            self._return = True

    async def disconnect(self):
        if self._conn is not None:
            await self._conn.close()
        self._conn = None
        self._return = True

    async def db_load(self):
        if self._db is None:
            await self.connect()
        if self._return:
            return
        # Save bot settings
        try:
            await self._db.settings.config.replace_one(
                {"_id": BOT_ID}, config_dict, upsert=True
            )
        except Exception as e:
            LOGGER.error(f"DataBase Collection Error: {e}")
            return
        # Save Aria2c options
        if await self._db.settings.aria2c.find_one({"_id": BOT_ID}) is None:
            await self._db.settings.aria2c.update_one(
                {"_id": BOT_ID}, {"$set": aria2_options}, upsert=True
            )
        # Save qbittorrent options
        if await self._db.settings.qbittorrent.find_one({"_id": BOT_ID}) is None:
            await self.save_qbit_settings()
        # User Data
        if await self._db.users.find_one():
            rows = self._db.users.find({})
            async for row in rows:
                uid = row["_id"]
                del row["_id"]
                thumb_path = f"Thumbnails/{uid}.jpg"
                rclone_config_path = f"rclone/{uid}.conf"
                token_path = f"tokens/{uid}.pickle"
                if row.get("thumb"):
                    if not await aiopath.exists("Thumbnails"):
                        await makedirs("Thumbnails")
                    async with aiopen(thumb_path, "wb+") as f:
                        await f.write(row["thumb"])
                    row["thumb"] = thumb_path
                if row.get("rclone_config"):
                    if not await aiopath.exists("rclone"):
                        await makedirs("rclone")
                    async with aiopen(rclone_config_path, "wb+") as f:
                        await f.write(row["rclone_config"])
                    row["rclone_config"] = rclone_config_path
                if row.get("token_pickle"):
                    if not await aiopath.exists("tokens"):
                        await makedirs("tokens")
                    async with aiopen(token_path, "wb+") as f:
                        await f.write(row["token_pickle"])
                    row["token_pickle"] = token_path
                user_data[uid] = row
            LOGGER.info("Users data has been imported from Database")
        # Rss Data
        if await self._db.rss[BOT_ID].find_one():
            rows = self._db.rss[BOT_ID].find({})
            async for row in rows:
                user_id = row["_id"]
                del row["_id"]
                rss_dict[user_id] = row
            LOGGER.info("Rss data has been imported from Database.")

    async def update_deploy_config(self):
        if self._return:
            return
        current_config = dict(dotenv_values("config.env"))
        await self._db.settings.deployConfig.replace_one(
            {"_id": BOT_ID}, current_config, upsert=True
        )

    async def update_config(self, dict_):
        if self._return:
            return
        await self._db.settings.config.update_one(
            {"_id": BOT_ID}, {"$set": dict_}, upsert=True
        )

    async def update_aria2(self, key, value):
        if self._return:
            return
        await self._db.settings.aria2c.update_one(
            {"_id": BOT_ID}, {"$set": {key: value}}, upsert=True
        )

    async def update_qbittorrent(self, key, value):
        if self._return:
            return
        await self._db.settings.qbittorrent.update_one(
            {"_id": BOT_ID}, {"$set": {key: value}}, upsert=True
        )

    async def save_qbit_settings(self):
        if self._return:
            return
        await self._db.settings.qbittorrent.replace_one(
            {"_id": BOT_ID}, qbit_options, upsert=True
        )

    async def update_private_file(self, path):
        if self._return:
            return
        if await aiopath.exists(path):
            async with aiopen(path, "rb+") as pf:
                pf_bin = await pf.read()
        else:
            pf_bin = ""
        path = path.replace(".", "__")
        await self._db.settings.files.update_one(
            {"_id": BOT_ID}, {"$set": {path: pf_bin}}, upsert=True
        )
        if path == "config.env":
            await self.update_deploy_config()

    async def update_user_data(self, user_id):
        if self._return:
            return
        data = user_data.get(user_id, {})
        if data.get("thumb"):
            del data["thumb"]
        if data.get("rclone_config"):
            del data["rclone_config"]
        if data.get("token_pickle"):
            del data["token_pickle"]
        await self._db.users.replace_one({"_id": user_id}, data, upsert=True)

    async def update_user_doc(self, user_id, key, path=""):
        if self._return:
            return
        if path:
            async with aiopen(path, "rb+") as doc:
                doc_bin = await doc.read()
        else:
            doc_bin = ""
        await self._db.users.update_one(
            {"_id": user_id}, {"$set": {key: doc_bin}}, upsert=True
        )

    async def rss_update_all(self):
        if self._return:
            return
        for user_id in list(rss_dict.keys()):
            await self._db.rss[BOT_ID].replace_one(
                {"_id": user_id}, rss_dict[user_id], upsert=True
            )

    async def rss_update(self, user_id):
        if self._return:
            return
        await self._db.rss[BOT_ID].replace_one(
            {"_id": user_id}, rss_dict[user_id], upsert=True
        )

    async def rss_delete(self, user_id):
        if self._return:
            return
        await self._db.rss[BOT_ID].delete_one({"_id": user_id})

    async def add_incomplete_task(self, cid, link, tag):
        if self._return:
            return
        await self._db.tasks[BOT_ID].insert_one({"_id": link, "cid": cid, "tag": tag})

    async def rm_complete_task(self, link):
        if self._return:
            return
        await self._db.tasks[BOT_ID].delete_one({"_id": link})

    async def get_incomplete_tasks(self):
        notifier_dict = {}
        if self._return:
            return notifier_dict
        if await self._db.tasks[BOT_ID].find_one():
            rows = self._db.tasks[BOT_ID].find({})
            async for row in rows:
                if row["cid"] in list(notifier_dict.keys()):
                    if row["tag"] in list(notifier_dict[row["cid"]]):
                        notifier_dict[row["cid"]][row["tag"]].append(row["_id"])
                    else:
                        notifier_dict[row["cid"]][row["tag"]] = [row["_id"]]
                else:
                    notifier_dict[row["cid"]] = {row["tag"]: [row["_id"]]}
        await self._db.tasks[BOT_ID].drop()
        return notifier_dict

    async def trunc_table(self, name):
        if self._return:
            return
        await self._db[name][BOT_ID].drop()

    async def add_file_entry(self, channel_id, message_id, file_data):
        """Add file entry to catalog"""
        try:
            document = {
                "channel_id": str(channel_id),
                "message_id": message_id,
                "file_unique_id": file_data.get("file_unique_id"),
                "file_name": file_data.get("file_name"),
                "caption_first_line": file_data.get("caption_first_line", ""),
                "file_size": file_data.get("file_size", 0),
                "mime_type": file_data.get("mime_type", ""),
                "file_hash": file_data.get("file_hash"),
                "search_text": file_data.get("search_text", ""),
                "date_added": file_data.get("date"),
                "indexed_at": datetime.utcnow()
            }
            await self._db.file_catalog.insert_one(document)
        except PyMongoError as e:
            LOGGER.error(f"Error adding file entry: {e}")

    async def check_file_exists(self, file_unique_id=None, file_hash=None, file_info=None):
        """
        Check if file exists in catalog.
        Priority: 1) sanitized filename (case-insensitive regex)
                  2) file_unique_id
                  3) file_hash
        """
        try:
            # FIRST PRIORITY: Check by sanitized filename with case-insensitive regex
            if file_info:
                base_name = get_duplicate_check_name(file_info)
                LOGGER.info(f"[DuplicateCheck] Base name: {base_name}")
                
                # Try both caption_first_line and file_name fields
                for ext in COMMON_EXTENSIONS:
                    for field in ("caption_first_line", "file_name"):
                        value = base_name + ext
                        # Use case-insensitive regex to match stored filenames
                        query = {field: {"$regex": f"^{re.escape(value)}$", "$options": "i"}}
                        result = await self._db.file_catalog.find_one(query)
                        LOGGER.info(f"[DuplicateCheck] Query {field}: {value} -> {bool(result)}")
                        if result:
                            return True
            
            # SECOND PRIORITY: Check by file_unique_id
            if file_unique_id:
                result = await self._db.file_catalog.find_one({"file_unique_id": file_unique_id})
                LOGGER.info(f"[DuplicateCheck] Query by file_unique_id: {file_unique_id} -> {bool(result)}")
                if result:
                    return True
            
            # THIRD PRIORITY: Check by file_hash
            if file_hash:
                result = await self._db.file_catalog.find_one({"file_hash": file_hash})
                LOGGER.info(f"[DuplicateCheck] Query by file_hash: {file_hash} -> {bool(result)}")
                if result:
                    return True
            
            return False
        
        except PyMongoError as e:
            LOGGER.error(f"Error checking file exists: {e}")
            return False

    async def get_catalog_stats(self, channel_id=None):
        """Get file catalog statistics"""
        try:
            query = {}
            if channel_id:
                query["channel_id"] = str(channel_id)

            total_files = await self._db.file_catalog.count_documents(query)

            if channel_id:
                return {"channel_id": channel_id, "total_files": total_files}
            else:
                pipeline = [
                    {"$group": {"_id": "$channel_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                channel_stats = await self._db.file_catalog.aggregate(pipeline).to_list(None)
                return {"total_files": total_files, "by_channel": channel_stats}

        except PyMongoError as e:
            LOGGER.error(f"Error getting catalog stats: {e}")
            return {}

    async def save_leech_progress(self, user_id, channel_id, data):
        """Save channel leech scan/download progress for user/channel."""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            await self._db.leech_progress.update_one({"_id": key}, {"$set": dict(data, _id=key)}, upsert=True)
        except Exception as e:
            LOGGER.error(f"Error saving leech progress: {e}")

    async def get_leech_progress(self, user_id, channel_id):
        """Retrieve saved channel leech progress for user/channel."""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            doc = await self._db.leech_progress.find_one({"_id": key})
            return doc
        except Exception as e:
            LOGGER.error(f"Error getting leech progress: {e}")
            return None

    async def clear_leech_progress(self, user_id, channel_id):
        """Delete saved channel leech progress for user/channel."""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            await self._db.leech_progress.delete_one({"_id": key})
        except Exception as e:
            LOGGER.error(f"Error clearing leech progress: {e}")

    @property
    def _return(self):  # Compatibility stub for existing code
        return self.__dict__.get("__return", False)

    @_return.setter
    def _return(self, v):
        self.__dict__["__return"] = v

database = DbManager()
