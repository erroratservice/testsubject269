from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from dotenv import dotenv_values
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
from datetime import datetime
from bot import (
    user_data,
    rss_dict,
    LOGGER,
    BOT_ID,
    config_dict,
    aria2_options,
    qbit_options,
)

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
            # return a dict ==> {_id, is_sudo, is_auth, as_doc, thumb, yt_opt, media_group, equal_splits, split_size, rclone, rclone_path, token_pickle, gdrive_id, leech_dest, lperfix, lprefix, excluded_extensions, user_transmission, index_url, default_upload}
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
            # return a dict ==> {_id, title: {link, last_feed, last_name, inf, exf, command, paused}
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
            # return a dict ==> {_id, cid, tag}
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
        return notifier_dict  # return a dict ==> {cid: {tag: [_id, _id, ...]}}

    async def trunc_table(self, name):
        if self._return:
            return
        await self._db[name][BOT_ID].drop()

    # Existing file catalog methods
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

    async def check_file_exists(self, file_unique_id=None, file_hash=None, file_name=None):
        """Check if file exists in catalog"""
        try:
            query = {}
            if file_unique_id:
                query["file_unique_id"] = file_unique_id
            elif file_hash:
                query["file_hash"] = file_hash
            elif file_name:
                query["$or"] = [
                    {"file_name": {"$regex": file_name, "$options": "i"}},
                    {"caption_first_line": {"$regex": file_name, "$options": "i"}}
                ]
            result = await self._db.file_catalog.find_one(query)
            return result is not None
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

    # NEW: Failed file tracking methods
    async def add_failed_file_entry(self, channel_id, message_id, file_data, failure_reason="incomplete_download"):
        """Add failed file entry to track download failures"""
        if self._return:
            return
        try:
            document = {
                "channel_id": str(channel_id),
                "message_id": message_id,
                "file_unique_id": file_data.get("file_unique_id"),
                "file_name": file_data.get("file_name"),
                "caption_first_line": file_data.get("caption_first_line", ""),
                "expected_size": file_data.get("file_size", 0),
                "actual_size": file_data.get("actual_size", 0),
                "failure_reason": failure_reason,
                "failed_at": datetime.utcnow(),
                "retry_count": 0,
                "file_hash": file_data.get("file_hash"),
                "search_text": file_data.get("search_text", "")
            }
            await self._db.failed_files.insert_one(document)
            LOGGER.info(f"Added failed file entry: {file_data.get('file_name')} - {failure_reason}")
        except PyMongoError as e:
            LOGGER.error(f"Error adding failed file entry: {e}")

    async def check_failed_file_exists(self, file_unique_id):
        """Check if file has failed before"""
        if self._return:
            return False
        try:
            result = await self._db.failed_files.find_one({"file_unique_id": file_unique_id})
            return result is not None
        except PyMongoError as e:
            LOGGER.error(f"Error checking failed file: {e}")
            return False

    async def get_failed_file_info(self, file_unique_id):
        """Get failed file information"""
        if self._return:
            return None
        try:
            result = await self._db.failed_files.find_one({"file_unique_id": file_unique_id})
            return result
        except PyMongoError as e:
            LOGGER.error(f"Error getting failed file info: {e}")
            return None

    async def update_failed_file_retry(self, file_unique_id):
        """Update retry count for failed file"""
        if self._return:
            return
        try:
            await self._db.failed_files.update_one(
                {"file_unique_id": file_unique_id}, 
                {
                    "$inc": {"retry_count": 1}, 
                    "$set": {"last_retry": datetime.utcnow()}
                }
            )
            LOGGER.info(f"Updated retry count for failed file: {file_unique_id}")
        except PyMongoError as e:
            LOGGER.error(f"Error updating failed file retry: {e}")

    async def get_failed_files_for_retry(self, channel_id, max_retries=3):
        """Get failed files that can be retried"""
        if self._return:
            return []
        try:
            cursor = self._db.failed_files.find({
                "channel_id": str(channel_id),
                "retry_count": {"$lt": max_retries}
            })
            
            failed_files = []
            async for doc in cursor:
                failed_files.append(doc)
            
            return failed_files
        except PyMongoError as e:
            LOGGER.error(f"Error getting failed files for retry: {e}")
            return []

    async def remove_failed_file_entry(self, file_unique_id):
        """Remove failed file entry (when successfully completed)"""
        if self._return:
            return
        try:
            result = await self._db.failed_files.delete_one({"file_unique_id": file_unique_id})
            if result.deleted_count > 0:
                LOGGER.info(f"Removed failed file entry: {file_unique_id}")
        except PyMongoError as e:
            LOGGER.error(f"Error removing failed file entry: {e}")

    async def get_failed_files_stats(self, channel_id=None):
        """Get failed files statistics"""
        if self._return:
            return {}
        try:
            query = {}
            if channel_id:
                query["channel_id"] = str(channel_id)
                
            total_failed = await self._db.failed_files.count_documents(query)
            
            # Group by failure reason
            pipeline = [
                {"$match": query},
                {"$group": {"_id": "$failure_reason", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            failure_reasons = await self._db.failed_files.aggregate(pipeline).to_list(None)
            
            # Group by retry count
            retry_pipeline = [
                {"$match": query},
                {"$group": {"_id": "$retry_count", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            retry_stats = await self._db.failed_files.aggregate(retry_pipeline).to_list(None)
            
            return {
                "total_failed": total_failed,
                "by_failure_reason": failure_reasons,
                "by_retry_count": retry_stats
            }
                
        except PyMongoError as e:
            LOGGER.error(f"Error getting failed files stats: {e}")
            return {}

    async def mark_failed_as_permanent(self, file_unique_id, reason="max_retries_exceeded"):
        """Mark a failed file as permanently failed (won't retry)"""
        if self._return:
            return
        try:
            await self._db.failed_files.update_one(
                {"file_unique_id": file_unique_id},
                {
                    "$set": {
                        "permanent_failure": True,
                        "permanent_failure_reason": reason,
                        "marked_permanent_at": datetime.utcnow()
                    }
                }
            )
            LOGGER.info(f"Marked file as permanently failed: {file_unique_id} - {reason}")
        except PyMongoError as e:
            LOGGER.error(f"Error marking file as permanently failed: {e}")

database = DbManager()
