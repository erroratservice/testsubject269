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
import logging

LOGGER = logging.getLogger(__name__)

def sanitize_filename(filename):
    """Clean filename sanitization matching channel_leech.py exactly"""
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
    
    filename = emoji_pattern.sub('', filename)
    filename = filename.replace('_', '.')
    filename = filename.replace('+', '.')
    filename = filename.replace(' ', '.')
    filename = re.sub(r'[\[\]\(\)\{\}]', '', filename)
    filename = re.sub(r'[<>:"/\|?*]', '', filename)
    filename = re.sub(r'\.{2,}', '.', filename)
    filename = filename.strip('.')
    
    if not filename:
        filename = "file"
    return filename 

def get_duplicate_check_name(file_info):
    """Return the sanitized base file name for duplication check"""
    base = None
    caption_line = file_info.get("caption_first_line")
    if caption_line:
        base = caption_line.strip()
    
    if not base:
        base = file_info.get("file_name") or ""
    
    sanitized = sanitize_filename(base)
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
        try:
            await self._db.settings.config.replace_one(
                {"_id": BOT_ID}, config_dict, upsert=True
            )
        except Exception as e:
            LOGGER.error(f"DataBase Collection Error: {e}")
            return
        
        if await self._db.settings.aria2c.find_one({"_id": BOT_ID}) is None:
            await self._db.settings.aria2c.update_one(
                {"_id": BOT_ID}, {"$set": aria2_options}, upsert=True
            )
        
        if await self._db.settings.qbittorrent.find_one({"_id": BOT_ID}) is None:
            await self.save_qbit_settings()
        
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
        
        if await self._db.rss[BOT_ID].find_one():
            rows = self._db.rss[BOT_ID].find({})
            async for row in rows:
                user_id = row["_id"]
                del row["_id"]
                rss_dict[user_id] = row
            LOGGER.info("Rss data has been imported from Database.")
        
        await self.ensure_catalog_indexes()

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
        """Add successful file entry to catalog"""
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
                "indexed_at": datetime.utcnow(),
                "status": "completed",
                "download_date": datetime.utcnow()
            }
            await self._db.file_catalog.insert_one(document)
        except PyMongoError as e:
            LOGGER.error(f"Error adding file entry: {e}")

    async def add_failed_file_entry(self, channel_id, message_id, file_data, error_reason="Download failed"):
        """Add failed file entry to prevent future download attempts"""
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
                "indexed_at": datetime.utcnow(),
                "status": "failed",
                "error_reason": error_reason,
                "failure_date": datetime.utcnow(),
                "retry_count": 1
            }
            await self._db.file_catalog.insert_one(document)
        except PyMongoError as e:
            LOGGER.error(f"Error adding failed file entry: {e}")

    async def check_file_exists(self, file_unique_id=None, file_hash=None, file_info=None):
        """Check if file exists in catalog (both completed AND failed files)"""
        try:
            if file_info:
                base_name = get_duplicate_check_name(file_info)
                
                # DEBUG: Log what we're checking
                LOGGER.debug(f"[cleech-debug] Checking file existence: base_name='{base_name}'")
                
                for ext in COMMON_EXTENSIONS:
                    for field in ("caption_first_line", "file_name"):
                        value = base_name + ext
                        query = {field: {"$regex": f"^{re.escape(value)}$", "$options": "i"}}
                        result = await self._db.file_catalog.find_one(query)
                        if result:
                            # DEBUG: Log WHY it matched
                            LOGGER.debug(f"[cleech-debug] ✓ FOUND in file_catalog: field='{field}', value='{value}', status='{result.get('status', 'unknown')}'")
                            return True
                
                # DEBUG: Log if NOT found in catalog
                LOGGER.debug(f"[cleech-debug] ✗ NOT in file_catalog: base_name='{base_name}'")
            
            if file_unique_id:
                result = await self._db.file_catalog.find_one({"file_unique_id": file_unique_id})
                if result:
                    LOGGER.debug(f"[cleech-debug] ✓ FOUND by file_unique_id: {file_unique_id}")
                    return True
            
            if file_hash:
                result = await self._db.file_catalog.find_one({"file_hash": file_hash})
                if result:
                    LOGGER.debug(f"[cleech-debug] ✓ FOUND by file_hash: {file_hash}")
                    return True
            
            return False
            
        except PyMongoError as e:
            LOGGER.error(f"Error checking file exists: {e}")
            return False

    async def should_retry_failed_file(self, file_info, max_retries=2):
        """Check if a failed file should be retried based on retry count and time"""
        try:
            base_name = get_duplicate_check_name(file_info)
            
            for ext in COMMON_EXTENSIONS:
                for field in ("caption_first_line", "file_name"):
                    value = base_name + ext
                    query = {
                        field: {"$regex": f"^{re.escape(value)}$", "$options": "i"},
                        "status": "failed"
                    }
                    result = await self._db.file_catalog.find_one(query)
                    if result:
                        retry_count = result.get('retry_count', 0)
                        failure_date = result.get('failure_date')
                        
                        if retry_count >= max_retries:
                            return False
                        
                        if failure_date:
                            time_since_failure = (datetime.utcnow() - failure_date).total_seconds()
                            if time_since_failure < 86400:  # 24 hours
                                return False
                        
                        await self._db.file_catalog.update_one(
                            {"_id": result["_id"]},
                            {"$inc": {"retry_count": 1}, "$set": {"last_retry_date": datetime.utcnow()}}
                        )
                        return True
            
            return True
            
        except Exception as e:
            LOGGER.error(f"Error checking retry status: {e}")
            return True

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
        """Save channel leech progress"""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            await self._db.leech_progress.update_one({"_id": key}, {"$set": dict(data, _id=key)}, upsert=True)
        except Exception as e:
            LOGGER.error(f"Error saving leech progress: {e}")

    async def get_leech_progress(self, user_id, channel_id):
        """Get channel leech progress"""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            doc = await self._db.leech_progress.find_one({"_id": key})
            return doc
        except Exception as e:
            LOGGER.error(f"Error getting leech progress: {e}")
            return None

    async def clear_leech_progress(self, user_id, channel_id):
        """Clear channel leech progress"""
        try:
            key = f"leech_progress:{user_id}:{channel_id}"
            await self._db.leech_progress.delete_one({"_id": key})
        except Exception as e:
            LOGGER.error(f"Error clearing leech progress: {e}")

    def _generate_sanitized_name(self, file_info, use_caption_as_filename=True):
        """Generate sanitized filename using the same logic as channel_leech.py"""
        original_filename = file_info.get('file_name', '')
        base_name = original_filename
        
        if use_caption_as_filename and file_info.get('caption_first_line'):
            base_name = file_info['caption_first_line'].strip()
        
        clean_base = sanitize_filename(base_name)
        
        original_ext = os.path.splitext(original_filename)[1]
        media_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
                            '.mp3', '.flac', '.wav', '.aac', '.m4a', '.ogg',
                            '.zip', '.rar', '.7z', '.tar', '.gz'}
                            
        if original_ext.lower() in media_extensions and not clean_base.lower().endswith(original_ext.lower()):
            clean_base += original_ext
            
        return clean_base

    def _create_enhanced_search_text(self, file_info, sanitized_name):
        """Create enhanced search text including both original and sanitized names"""
        search_parts = []
        
        if file_info.get('file_name'):
            search_parts.append(file_info['file_name'])
        
        if sanitized_name and sanitized_name != file_info.get('file_name'):
            search_parts.append(sanitized_name)
        
        if file_info.get('caption_first_line'):
            search_parts.append(file_info['caption_first_line'])
        
        original_search_text = file_info.get('search_text', '')
        if original_search_text and original_search_text not in search_parts:
            search_parts.append(original_search_text)
        
        return ' '.join(filter(None, search_parts))

    async def add_catalog_entry(self, channel_id, message_id, file_info):
        """Add message to channel catalog with enhanced filename tracking"""
        try:
            catalog_id = f"{channel_id}_{message_id}"
            
            sanitized_name = self._generate_sanitized_name(file_info)
            enhanced_search_text = self._create_enhanced_search_text(file_info, sanitized_name)
            
            enhanced_file_info = file_info.copy()
            enhanced_file_info['sanitized_name'] = sanitized_name
            enhanced_file_info['search_text'] = enhanced_search_text
            
            catalog_entry = {
                "_id": catalog_id,
                "channel_id": channel_id,
                "message_id": message_id,
                "file_info": enhanced_file_info,
                "message_date": datetime.utcnow().isoformat(),
                "last_updated": datetime.utcnow().isoformat(),
                "scan_version": 1
            }
            await self._db.channel_catalog.replace_one(
                {"_id": catalog_id}, 
                catalog_entry, 
                upsert=True
            )
        except Exception as e:
            LOGGER.error(f"Error adding catalog entry: {e}")

    async def get_catalog_files_with_stats(self, channel_id, filter_tags=None, filter_mode='and', scan_type=None, from_msg_id=None, to_msg_id=None):
        """Get files from catalog with enhanced filtering and detailed statistics tracking"""
        try:
            stats = {
                'total_processed': 0,
                'filter_rejected': 0,
                'already_downloaded': 0,
                'type_filtered': 0,
                'range_filtered': 0
            }
            
            pipeline = []
            base_match_stage = {"channel_id": channel_id}
            pipeline.append({"$match": base_match_stage})
            
            total_cursor = self._db.channel_catalog.aggregate([{"$match": base_match_stage}, {"$count": "total"}])
            total_result = await total_cursor.to_list(1)
            total_in_channel = total_result[0]['total'] if total_result else 0
            
            if from_msg_id or to_msg_id:
                msg_range = {}
                if from_msg_id:
                    msg_range["$lte"] = from_msg_id
                if to_msg_id:
                    msg_range["$gte"] = to_msg_id
                range_filter_stage = {"message_id": msg_range}
                pipeline.append({"$match": range_filter_stage})
                
                range_cursor = self._db.channel_catalog.aggregate(pipeline + [{"$count": "total"}])
                range_result = await range_cursor.to_list(1)
                after_range = range_result[0]['total'] if range_result else 0
                stats['range_filtered'] = total_in_channel - after_range
            
            if scan_type == 'document':
                type_filter = {"file_info.mime_type": {"$regex": "^(application/|text/)"}}
                pipeline.append({"$match": type_filter})
            elif scan_type == 'media':
                type_filter = {"file_info.mime_type": {"$regex": "^video/"}}
                pipeline.append({"$match": type_filter})
            
            if scan_type:
                type_cursor = self._db.channel_catalog.aggregate(pipeline + [{"$count": "total"}])
                type_result = await type_cursor.to_list(1)
                after_type = type_result[0]['total'] if type_result else 0
                before_type = total_in_channel - stats['range_filtered']
                stats['type_filtered'] = before_type - after_type
            
            before_text_cursor = self._db.channel_catalog.aggregate(pipeline + [{"$count": "total"}])
            before_text_result = await before_text_cursor.to_list(1)
            before_text_filtering = before_text_result[0]['total'] if before_text_result else 0
            stats['total_processed'] = before_text_filtering
            
            if filter_tags:
                if filter_mode == 'or':
                    or_conditions = []
                    for tag in filter_tags:
                        or_conditions.append({"file_info.search_text": {"$regex": re.escape(tag), "$options": "i"}})
                    pipeline.append({"$match": {"$or": or_conditions}})
                else:
                    for tag in filter_tags:
                        pipeline.append({"$match": {"file_info.search_text": {"$regex": re.escape(tag), "$options": "i"}}})
            
            if filter_tags:
                after_text_cursor = self._db.channel_catalog.aggregate(pipeline + [{"$count": "total"}])
                after_text_result = await after_text_cursor.to_list(1)
                after_text_filtering = after_text_result[0]['total'] if after_text_result else 0
                stats['filter_rejected'] = before_text_filtering - after_text_filtering
            
            pipeline.append({"$sort": {"message_id": -1}})
            pipeline.append({
                "$project": {
                    "_id": 1,
                    "message_id": 1,
                    "file_info": 1
                }
            })
            
            aggregation_options = {
                "allowDiskUse": True,
                "maxTimeMS": 300000,
                "cursor": {"batchSize": 1000}
            }
            
            cursor = self._db.channel_catalog.aggregate(pipeline, **aggregation_options)
            
            matched_files = []
            already_downloaded = 0
            
            async for entry in cursor:
                file_info = entry['file_info']
                sanitized_name = file_info.get('sanitized_name', file_info.get('file_name', ''))
                
                if not await self.check_file_exists(file_info=file_info):
                    matched_files.append({
                        'message_id': entry['message_id'],
                        'url': f"https://t.me/c/{str(channel_id)[4:]}/{entry['message_id']}",
                        'filename': sanitized_name,
                        'file_info': file_info
                    })
                else:
                    already_downloaded += 1
            
            stats['already_downloaded'] = already_downloaded
            return matched_files, stats
            
        except Exception as e:
            LOGGER.error(f"Error in optimized catalog query with stats: {e}")
            return [], {'total_processed': 0, 'filter_rejected': 0, 'already_downloaded': 0, 'type_filtered': 0, 'range_filtered': 0}

    async def get_catalog_files(self, channel_id, filter_tags=None, filter_mode='and', scan_type=None, from_msg_id=None, to_msg_id=None):
        """Get files from catalog with enhanced filtering"""
        try:
            pipeline = []
            match_stage = {"channel_id": channel_id}
            
            if from_msg_id or to_msg_id:
                msg_range = {}
                if from_msg_id:
                    msg_range["$lte"] = from_msg_id
                if to_msg_id:
                    msg_range["$gte"] = to_msg_id
                match_stage["message_id"] = msg_range
            
            if scan_type == 'document':
                match_stage["file_info.mime_type"] = {"$regex": "^(application/|text/)"}
            elif scan_type == 'media':
                match_stage["file_info.mime_type"] = {"$regex": "^video/"}
            
            pipeline.append({"$match": match_stage})
            
            if filter_tags:
                if filter_mode == 'or':
                    or_conditions = []
                    for tag in filter_tags:
                        or_conditions.append({"file_info.search_text": {"$regex": re.escape(tag), "$options": "i"}})
                    pipeline.append({"$match": {"$or": or_conditions}})
                else:
                    for tag in filter_tags:
                        pipeline.append({"$match": {"file_info.search_text": {"$regex": re.escape(tag), "$options": "i"}}})
            
            pipeline.append({"$sort": {"message_id": -1}})
            pipeline.append({
                "$project": {
                    "_id": 1,
                    "message_id": 1,
                    "file_info": 1
                }
            })
            
            aggregation_options = {
                "allowDiskUse": True,
                "maxTimeMS": 300000,
                "cursor": {"batchSize": 1000}
            }
            
            cursor = self._db.channel_catalog.aggregate(pipeline, **aggregation_options)
            matched_files = []
            
            async for entry in cursor:
                file_info = entry['file_info']
                sanitized_name = file_info.get('sanitized_name', file_info.get('file_name', ''))
                
                if not await self.check_file_exists(file_info=file_info):
                    matched_files.append({
                        'message_id': entry['message_id'],
                        'url': f"https://t.me/c/{str(channel_id)[4:]}/{entry['message_id']}",
                        'filename': sanitized_name,
                        'file_info': file_info
                    })
            
            return matched_files
            
        except Exception as e:
            LOGGER.error(f"Error in optimized catalog query: {e}")
            return []

    async def get_channel_metadata(self, channel_id):
        """Get channel scanning metadata"""
        try:
            return await self._db.channel_metadata.find_one({"_id": channel_id})
        except Exception as e:
            LOGGER.error(f"Error getting channel metadata: {e}")
            return None

    async def update_channel_metadata(self, channel_id, latest_msg_id=None, total_cataloged=None, channel_username=None, catalog_status='incomplete'):
        """Update channel metadata after scan with completion status"""
        try:
            update_data = {
                "last_incremental_scan": datetime.utcnow().isoformat(),
                "catalog_status": catalog_status
            }
            
            if catalog_status == 'complete':
                update_data["last_full_scan"] = datetime.utcnow().isoformat()
            
            if latest_msg_id:
                update_data["latest_message_id"] = latest_msg_id
            if total_cataloged is not None:
                update_data["total_messages_cataloged"] = total_cataloged
            if channel_username:
                update_data["channel_username"] = channel_username
            
            await self._db.channel_metadata.update_one(
                {"_id": channel_id},
                {"$set": update_data},
                upsert=True
            )
        except Exception as e:
            LOGGER.error(f"Error updating channel metadata: {e}")

    async def get_catalog_stats(self, channel_id):
        """Get catalog statistics for a channel"""
        try:
            pipeline = [
                {"$match": {"channel_id": channel_id}},
                {"$group": {
                    "_id": "$channel_id",
                    "total_files": {"$sum": 1},
                    "latest_message": {"$max": "$message_id"},
                    "oldest_message": {"$min": "$message_id"}
                }}
            ]
            result = await self._db.channel_catalog.aggregate(pipeline).to_list(1)
            return result[0] if result else None
        except Exception as e:
            LOGGER.error(f"Error getting catalog stats: {e}")
            return None

    async def ensure_catalog_indexes(self):
        """Create optimized database indexes for maximum performance"""
        try:
            await self._db.channel_catalog.create_index([
                ("channel_id", 1),
                ("file_info.mime_type", 1),
                ("message_id", -1)
            ], background=True, name="channel_mime_msgid")
            
            await self._db.channel_catalog.create_index([
                ("channel_id", 1),
                ("message_id", -1)
            ], background=True, name="channel_msgid")
            
            await self._db.channel_catalog.create_index([
                ("channel_id", 1),
                ("file_info.search_text", "text")
            ], background=True, name="channel_search_text")
            
            await self._db.channel_catalog.create_index([
                ("channel_id", 1),
                ("file_info.sanitized_name", 1)
            ], background=True, name="channel_sanitized")
            
            await self._db.file_catalog.create_index([
                ("channel_id", 1),
                ("file_name", 1),
                ("status", 1)
            ], background=True, name="file_catalog_main")
            
            await self._db.file_catalog.create_index([
                ("status", 1),
                ("retry_count", 1),
                ("failure_date", 1)
            ], background=True, name="status_retry_failure")
            
            await self._db.file_catalog.create_index([
                ("caption_first_line", 1),
                ("status", 1)
            ], background=True, name="caption_status")
            
            LOGGER.info("[DB] Created optimized catalog indexes")
            
        except Exception as e:
            pass  # Indexes likely already exist

    @property
    def _return(self):
        return self.__dict__.get("__return", False)

    @_return.setter
    def _return(self, v):
        self.__dict__["__return"] = v

database = DbManager()
