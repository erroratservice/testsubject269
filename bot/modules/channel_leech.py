from pyrogram.filters import command, chat, document, video
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import Message
from pyrogram import enums
from bot import bot, user, LOGGER, config_dict, user_data
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
from ..helper.listeners.task_listener import TaskListener
import asyncio
import os
import re
import time 
import weakref
from datetime import datetime, timezone

def sanitize_filename(filename):
    """Clean filename sanitization with proper dot conversion"""
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
    filename = filename.replace('+', '.')
    filename = filename.replace('_', '.')
    filename = filename.replace(' ', '.')
    filename = re.sub(r'[\[\]\(\)\{\}]', '', filename)
    filename = re.sub(r'[<>:"/\|?*]', '', filename)
    filename = re.sub(r'\.{2,}', '.', filename)
    filename = filename.strip('.')
    if not filename:
        filename = "file"
    return filename

class SimpleChannelLeechCoordinator(TaskListener):
    _active_coordinators = weakref.WeakValueDictionary()
    _coordinator_counter = 0

    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None
        self.filter_tags = []
        self.filter_mode = 'and'
        self.scan_type = None
        self.completed_scan_type = None
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.max_concurrent = 5
        self.check_interval = 10
        self.pending_files = []
        self.pending_file_ids = set()
        self.pending_sanitized_names = set()
        self.our_active_links = set()
        self.completed_count = 0
        self.failed_count = 0
        self.total_files = 0
        self.link_to_file_mapping = {}
        self._last_status_text = ""
        self.resume_mode = False
        self.resume_from_msg_id = None
        self.scanned_message_ids = set()
        self.start_time = datetime.now()
        
        self.from_msg_id = None
        self.to_msg_id = None
        self.scan_direction = 'newest_to_oldest'
        self.use_catalog = True
        self.catalog_mode = None
        
        SimpleChannelLeechCoordinator._coordinator_counter += 1
        self._coordinator_id = f"{message.from_user.id}_{self.channel_id}_{SimpleChannelLeechCoordinator._coordinator_counter}"
        self._is_active = False
        
        self.last_message_time = time.time()
        self.last_batch_time = time.time()
        self.scan_start_time = time.time()
        self.messages_processed_this_minute = 0
        self.last_minute_check = time.time()
        super().__init__()

    @classmethod
    async def handle_task_completion(cls, link, name, size, files, folders, mime_type):
        """Called by TaskListener when any task completes"""
        for coord_id, coordinator in list(cls._active_coordinators.items()):
            try:
                if coordinator and link in coordinator.our_active_links:
                    await coordinator._handle_our_task_completion(link, name, size, files, folders, mime_type)
                    return
            except (AttributeError, ReferenceError):
                continue

    @classmethod
    async def handle_task_failure(cls, link, error):
        """Called by TaskListener when any task fails"""
        for coord_id, coordinator in list(cls._active_coordinators.items()):
            try:
                if coordinator and link in coordinator.our_active_links:
                    await coordinator._handle_our_task_failure(link, error)
                    return
            except (AttributeError, ReferenceError):
                continue

    @classmethod
    def cleanup_stale_coordinators(cls):
        """Clean up any stale coordinator references"""
        pass

    @classmethod
    def get_active_coordinator_count(cls):
        """Get count of active coordinators for monitoring"""
        cls.cleanup_stale_coordinators()
        return len(cls._active_coordinators)

    def _register_coordinator(self):
        """Register this coordinator safely"""
        if not self._is_active:
            self._active_coordinators[self._coordinator_id] = self
            self._is_active = True

    def _unregister_coordinator(self):
        """Unregister this coordinator safely"""
        if self._is_active:
            self._active_coordinators.pop(self._coordinator_id, None)
            self._is_active = False

    def _determine_scan_direction(self):
        """Determine scan direction based on from/to message IDs"""
        if not self.from_msg_id and not self.to_msg_id:
            return 'newest_to_oldest'
        
        if self.from_msg_id and self.to_msg_id:
            if self.from_msg_id > self.to_msg_id:
                return 'newest_to_oldest'
            else:
                return 'oldest_to_newest'
        
        if self.from_msg_id:
            return 'from_specified'
        
        if self.to_msg_id:
            return 'to_specified'
        
        return 'newest_to_oldest'

    def _get_range_description(self):
        """Get human-readable description of scan range"""
        if not self.from_msg_id and not self.to_msg_id:
            return " Full channel (newest → oldest)"
        
        if self.from_msg_id and self.to_msg_id:
            direction = "→ oldest" if self.from_msg_id > self.to_msg_id else "→ newest"
            return f" Messages {min(self.from_msg_id, self.to_msg_id)} to {max(self.from_msg_id, self.to_msg_id)} ({direction})"
        
        if self.from_msg_id:
            return f" From message {self.from_msg_id} → oldest"
        
        if self.to_msg_id:
            return f" Newest → message {self.to_msg_id}"
        
        return " Full channel"

    def _get_filter_description(self):
        """Get human-readable description of filter settings"""
        if not self.filter_tags:
            return " No filter"
        
        tags_text = ' '.join(self.filter_tags)
        if self.filter_mode == 'or':
            return f" ANY of: `{tags_text}` (OR mode)"
        else:
            return f" ALL of: `{tags_text}` (AND mode)"

    def _get_scan_filter_status(self):
        """Get detailed filter status for scanning progress"""
        if not self.filter_tags:
            return "**Filter:** None (downloading all files)"
        
        tags_text = ' '.join(self.filter_tags)
        if self.filter_mode == 'or':
            return f"**Filter (OR):** Files containing ANY of: `{tags_text}`"
        else:
            return f"**Filter (AND):** Files containing ALL of: `{tags_text}`"

    def _check_filter_match(self, search_text):
        """Enhanced filter matching with AND/OR logic"""
        if not self.filter_tags:
            return True
        
        search_text_lower = search_text.lower()
        
        if self.filter_mode == 'or':
            return any(tag.lower() in search_text_lower for tag in self.filter_tags)
        else:
            return all(tag.lower() in search_text_lower for tag in self.filter_tags)

    def _should_process_message(self, message):
        """Check if message is within specified range"""
        msg_id = message.id
        
        if self.from_msg_id and self.to_msg_id:
            min_id = min(self.from_msg_id, self.to_msg_id)
            max_id = max(self.from_msg_id, self.to_msg_id)
            return min_id <= msg_id <= max_id
        
        if self.from_msg_id:
            return msg_id <= self.from_msg_id
        
        if self.to_msg_id:
            return msg_id >= self.to_msg_id
        
        return True

    def _get_scan_offset_and_limit(self):
        """Get appropriate offset_id for Pyrogram get_chat_history"""
        if self.scan_direction == 'oldest_to_newest':
            if self.from_msg_id and self.to_msg_id:
                return min(self.from_msg_id, self.to_msg_id) - 1
            return 0
        else:
            if self.from_msg_id:
                return self.from_msg_id
            return self.resume_from_msg_id if self.resume_from_msg_id > 0 else 0

    async def _determine_catalog_mode(self):
        """Smart catalog strategy: Check actual catalog coverage vs channel messages"""
        try:
            metadata = await database.get_channel_metadata(self.channel_chat_id)
            catalog_stats = await database.get_catalog_stats(self.channel_chat_id)
            
            if not metadata or not catalog_stats:
                return 'full'
            
            catalog_status = metadata.get('catalog_status', 'incomplete')
            if catalog_status == 'incomplete':
                return 'full'
            
            catalog_bounds = await database.get_catalog_bounds(self.channel_chat_id)
            if not catalog_bounds:
                return 'full'
            
            catalog_highest = catalog_bounds.get('highest_msg_id', 0)
            
            try:
                async for latest_message in user.get_chat_history(self.channel_chat_id, limit=1):
                    channel_latest = latest_message.id
                    break
                else:
                    channel_latest = catalog_highest
                
                new_messages_count = max(0, channel_latest - catalog_highest)
                
                if new_messages_count == 0:
                    return 'filter_only'
                else:
                    return 'incremental'
                    
            except Exception as e:
                LOGGER.error(f"Error checking channel bounds: {e}")
                return 'filter_only'
            
        except Exception as e:
            LOGGER.error(f"Error determining catalog mode: {e}")
            return 'full'

    async def _is_download_duplicate(self, file_item):
        """Enhanced duplicate checking with queue integration and failed file handling"""
        try:
            sanitized_name = self._generate_clean_filename(file_item['file_info'])
            file_info = file_item['file_info']
            
            if sanitized_name in self.pending_sanitized_names:
                return True
            
            file_unique_id = file_info.get('file_unique_id')
            if file_unique_id and file_unique_id in self.pending_file_ids:
                return True
            
            url = file_item['url']
            if url in self.our_active_links:
                return True
            
            if await self._check_bot_task_queue(sanitized_name, url):
                return True
            
            if await database.check_file_exists(file_info=file_info):
                if not await database.should_retry_failed_file(file_info):
                    return True
                return False
            
            return False
            
        except Exception as e:
            LOGGER.error(f"Error checking duplicate: {e}")
            return False

    async def _check_bot_task_queue(self, sanitized_name, url):
        """Check if file is already in bot's global download queue"""
        try:
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            
            if await database._db.tasks[bot_token_first_half].find_one({"_id": url}):
                return True
            
            base_name = os.path.splitext(sanitized_name)[0]
            cursor = database._db.tasks[bot_token_first_half].find({})
            
            async for task in cursor:
                task_url = task.get('_id', '')
                if task_url.startswith('https://t.me/') and base_name.lower() in task_url.lower():
                    return True
            
            return False
            
        except Exception as e:
            LOGGER.error(f"Error checking bot task queue: {e}")
            return False

    async def _process_from_catalog(self):
        """Process files directly from catalog with current filters and live progress updates"""
        try:
            await self._safe_edit_message(self.status_message, 
                "**🗄️ Loading catalog statistics...**\n\n"
                f"**Filter:** {self._get_filter_description()}")
            
            catalog_stats = await database.get_catalog_stats(self.channel_chat_id)
            total_catalog_files = catalog_stats.get('total_files', 0) if catalog_stats else 0
            
            self.catalog_start_time = time.time()
            self.catalog_processed_count = 0
            self.catalog_matched_count = 0
            self.catalog_downloaded_count = 0
            
            await self._safe_edit_message(self.status_message, 
                f"**🗄️ Processing catalog ({total_catalog_files:,} total files)...**\n\n"
                f"**Filter:** {self._get_filter_description()}\n"
                f"**Range:** {self._get_range_description()}\n\n"
                f"**📊 Initializing catalog scan...**")
            
            async def catalog_progress_callback(processed, total, matched, downloaded):
                try:
                    self.catalog_processed_count = processed
                    self.catalog_matched_count = matched
                    self.catalog_downloaded_count = downloaded
                    
                    progress_percent = (processed / total * 100) if total > 0 else 0
                    elapsed_time = time.time() - self.catalog_start_time
                    processing_rate = processed / elapsed_time if elapsed_time > 0 else 0
                    
                    remaining = total - processed
                    eta_seconds = (remaining / processing_rate) if processing_rate > 0 else 0
                    eta_text = f" (ETA: {eta_seconds/60:.1f}m)" if eta_seconds > 0 and eta_seconds < 3600 else ""
                    
                    scan_type_text = f" {self.scan_type}" if self.scan_type else ""
                    
                    await self._safe_edit_message(self.status_message,
                        f"**🗄️ Processing{scan_type_text} catalog... ({processed:,}/{total:,} - {progress_percent:.1f}%)**\n\n"
                        f"**Filter:** {self._get_filter_description()}\n"
                        f"**Range:** {self._get_range_description()}\n\n"
                        f"**📊 Live Progress:**\n"
                        f"**Processed:** {processed:,} files | **Rate:** {processing_rate:.1f} files/s{eta_text}\n"
                        f"**Filter matches:** {matched:,}\n"
                        f"**Already downloaded:** {downloaded:,}\n"
                        f"**Available for download:** {matched - downloaded:,}"
                    )
                except Exception as e:
                    LOGGER.error(f"Error updating catalog progress: {e}")
            
            catalog_files, catalog_statistics = await database.get_catalog_files_with_stats(
                channel_id=self.channel_chat_id,
                filter_tags=self.filter_tags,
                filter_mode=self.filter_mode,
                scan_type=self.scan_type,
                from_msg_id=self.from_msg_id,
                to_msg_id=self.to_msg_id,
                progress_callback=catalog_progress_callback
            )
            
            self.pending_files = catalog_files
            total_found = len(catalog_files)
            
            total_processed = catalog_statistics.get('total_processed', 0)
            filter_rejected = catalog_statistics.get('filter_rejected', 0)
            already_downloaded = catalog_statistics.get('already_downloaded', 0)
            
            filter_match_rate = ((total_processed - filter_rejected) / total_processed * 100) if total_processed > 0 else 0
            
            filtered_files = []
            duplicate_count = 0
            
            if len(self.pending_files) > 0:
                await self._safe_edit_message(self.status_message,
                    f"**🗄️ Catalog scan completed! Checking for duplicates...**\n\n"
                    f"**📊 Catalog Results:**\n"
                    f"**Total processed:** {total_processed:,}\n"
                    f"**Filter matches:** {total_processed - filter_rejected:,}\n"
                    f"**Already downloaded:** {already_downloaded:,}\n"
                    f"**Checking {len(self.pending_files):,} files for queue duplicates...**"
                )
            
            for i, file_item in enumerate(self.pending_files):
                if i > 0 and i % 100 == 0:
                    await self._safe_edit_message(self.status_message,
                        f"**🗄️ Checking duplicates... ({i:,}/{len(self.pending_files):,})**\n\n"
                        f"**Queue duplicates found:** {duplicate_count}\n"
                        f"**Remaining to check:** {len(self.pending_files) - i:,}"
                    )
                
                if not await self._is_download_duplicate(file_item):
                    filtered_files.append(file_item)
                else:
                    duplicate_count += 1
            
            self.pending_files = filtered_files
            catalog_statistics['duplicate_rejected'] = duplicate_count
            total_found = len(self.pending_files)
            
            if total_found == 0:
                await self._safe_edit_message(self.status_message, 
                    f"**✅ Catalog processed! No matching files found for download**\n\n"
                    f"**📊 Detailed Statistics:**\n"
                    f"**Total files processed:** {total_processed:,}\n"
                    f"**Filter rejections:** {filter_rejected:,}\n"
                    f"**Already downloaded:** {already_downloaded:,}\n"
                    f"**Queue duplicates:** {duplicate_count:,}\n"
                    f"**Available for download:** {total_found} files\n\n"
                    f"**📈 Filter match rate:** {filter_match_rate:.1f}%\n\n"
                    f"**Filter:** {self._get_filter_description()}\n"
                    f"**Range:** {self._get_range_description()}"
                )
                await self._show_final_results(total_processed, filter_rejected)
                return
            
            for file_item in self.pending_files:
                sanitized_name = self._generate_clean_filename(file_item['file_info'])
                self.pending_sanitized_names.add(sanitized_name)
                file_unique_id = file_item['file_info'].get('file_unique_id')
                if file_unique_id:
                    self.pending_file_ids.add(file_unique_id)
            
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            
            total_pending = len(self.our_active_links) + len(self.pending_files)
            scan_type_text = f" {self.scan_type}" if self.scan_type else ""
            catalog_time = time.time() - self.catalog_start_time
            
            await self._safe_edit_message(self.status_message, 
                f"**✅ Catalog processed! Found {total_found:,} files for download**\n\n"
                f"**📊 Detailed Statistics:**\n"
                f"**Total{scan_type_text} files processed:** {total_processed:,} ({catalog_time:.1f}s)\n"
                f"**Filter rejections:** {filter_rejected:,}\n"
                f"**Already downloaded:** {already_downloaded:,}\n"
                f"**Queue duplicates:** {duplicate_count:,}\n"
                f"**Available for download:** {total_found:,} files\n\n"
                f"**📈 Filter match rate:** {filter_match_rate:.1f}%\n\n"
                f"**Filter:** {self._get_filter_description()}\n"
                f"**Range:** {self._get_range_description()}\n\n"
                f"**📥 Downloads queued: {total_pending} files**\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                f"**⏳ Waiting for queued processes to complete...**"
            )
            
            await self._wait_for_completion_callback_mode()
            await self._show_final_results(total_processed, filter_rejected)
            
        except Exception as e:
            LOGGER.error(f"Error processing from catalog: {e}", exc_info=True)
            raise

    async def _incremental_scan_and_catalog(self):
        """Scan only messages newer than highest cataloged message"""
        try:
            catalog_bounds = await database.get_catalog_bounds(self.channel_chat_id)
            if not catalog_bounds:
                LOGGER.error(f"No catalog bounds found - falling back to full scan")
                await self._full_scan_and_catalog()
                return
                
            catalog_highest = catalog_bounds.get('highest_msg_id', 0)
            
            await self._safe_edit_message(self.status_message, 
                f"**📊 Incremental scan from message {catalog_highest + 1}...**\n\n"
                f"**Catalog coverage up to:** {catalog_highest:,}\n"
                f"**Scanning:** New messages beyond catalog\n\n"
                f"**Filter:** {self._get_filter_description()}")
            
            new_files_cataloged = 0
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            new_highest_msg_id = catalog_highest
            
            message_iterator = user.get_chat_history(
                chat_id=self.channel_chat_id,
                offset_id=0
            )
            
            processed_count = 0
            async for message in message_iterator:
                if message.id <= catalog_highest:
                    break
                
                processed_count += 1
                new_highest_msg_id = max(new_highest_msg_id, message.id)
                
                if processed_count % 50 == 0:
                    await self._safe_edit_message(self.status_message, 
                        f"**📊 Incremental scan... {processed_count:,} new messages processed**\n\n"
                        f"**Previous catalog up to:** {catalog_highest:,}\n"
                        f"**Current message ID:** {message.id:,}\n"
                        f"**New files cataloged:** {new_files_cataloged}\n\n"
                        f"**Filter:** {self._get_filter_description()}")
                
                if not (message.document or message.video):
                    continue
                    
                if self.scan_type == 'document' and not message.document:
                    continue
                elif self.scan_type == 'media' and not message.video:
                    continue
                
                file_info = await scanner._extract_file_info(message)
                if file_info:
                    await database.add_catalog_entry(
                        self.channel_chat_id, 
                        message.id, 
                        file_info
                    )
                    new_files_cataloged += 1
            
            try:
                chat = await user.get_chat(self.channel_id)
                channel_username = chat.username if hasattr(chat, 'username') and chat.username else None
            except:
                channel_username = None
                
            await database.update_channel_metadata(
                self.channel_chat_id, 
                latest_msg_id=new_highest_msg_id,
                total_cataloged=new_files_cataloged,
                channel_username=channel_username,
                catalog_status='complete'
            )
            
            await self._safe_edit_message(self.status_message, 
                f"**✅ Incremental scan complete!**\n\n"
                f"**📊 Update Results:**\n"
                f"**Previous catalog:** up to message {catalog_highest:,}\n"
                f"**Updated catalog:** up to message {new_highest_msg_id:,}\n"
                f"**New messages processed:** {processed_count:,}\n"
                f"**New files added:** {new_files_cataloged}\n\n"
                f"**Now processing from updated catalog...**")
                
        except Exception as e:
            LOGGER.error(f"Error in incremental scan: {e}")
            raise

    async def _full_scan_and_catalog(self):
        """Full channel scan with catalog building and downloading"""
        try:
            await self._safe_edit_message(self.status_message, 
                "**🚀 Starting channel scan with catalog building...**")
            
            await database.update_channel_metadata(
                self.channel_chat_id, 
                catalog_status='incomplete'
            )
            
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            
            batch_size = 100
            batch_sleep = 5
            processed_messages = 0
            completion_task = None
            
            last_status_update = 0
            status_update_interval = 10
            self.scan_start_time = time.time()
            self.iteration_timeout = 60
            self.stuck_recovery_attempts = 0
            self.max_recovery_attempts = 3
            self.last_iteration_time = time.time()
            
            api_request_count = 0
            files_processed_count = 0
            last_rate_reset = time.time()
            max_api_requests_per_30s = 25
            max_files_per_30s = 500
            
            skipped_filter_mismatch = 0
            skipped_existing_files = 0
            skipped_duplicates_in_queue = 0
            
            cataloged_count = 0
            latest_msg_id = 0
            oldest_msg_id = float('inf')

            if self.resume_mode:
                await self._restore_resume_state(scanner)
                while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                    await self._start_next_download()

            if self.our_active_links or self.pending_files:
                completion_task = asyncio.create_task(self._wait_for_completion_callback_mode())

            scan_totals = {}
            scan_types = []
            if self.scan_type == 'document':
                scan_types.append({'name': 'document', 'filter': enums.MessagesFilter.DOCUMENT})
            elif self.scan_type == 'media':
                scan_types.append({'name': 'media', 'filter': enums.MessagesFilter.VIDEO})
            else:
                scan_types.append({'name': 'document', 'filter': enums.MessagesFilter.DOCUMENT})
                scan_types.append({'name': 'media', 'filter': enums.MessagesFilter.VIDEO})

            for scan in scan_types:
                try:
                    total_count = await user.search_messages_count(
                        chat_id=self.channel_chat_id,
                        filter=scan['filter']
                    )
                    scan_totals[scan['name']] = total_count
                except Exception as e:
                    scan_totals[scan['name']] = 0

            offset_id = self._get_scan_offset_and_limit()
            
            if self.resume_mode:
                if self.resume_from_msg_id > 0:
                    await self._safe_edit_message(self.status_message, 
                        f"**📋 Resuming scan from message {offset_id}...**")
                else:
                    await self._safe_edit_message(self.status_message, 
                        f"**🔄 Starting fresh scan...**")
            else:
                range_desc = self._get_range_description()
                await self._safe_edit_message(self.status_message, 
                    f"**🚀 Starting scan{range_desc}...**")
            
            total_media_files = sum(scan_totals.values())
            scanned_media_count = 0
            current_batch = []
            loop_iteration = 0
            
            message_iterator = user.get_chat_history(
                chat_id=self.channel_chat_id,
                offset_id=offset_id
            )
            
            async for message in message_iterator:
                loop_iteration += 1
                current_time = time.time()
                self.last_message_time = current_time
                self.last_iteration_time = current_time
                
                latest_msg_id = max(latest_msg_id, message.id)
                oldest_msg_id = min(oldest_msg_id, message.id)
                
                if not self._should_process_message(message):
                    continue
                
                if self.to_msg_id and self.scan_direction in ['newest_to_oldest', 'to_specified'] and message.id <= self.to_msg_id:
                    break
                
                iteration_idle = current_time - self.last_iteration_time
                if iteration_idle > self.iteration_timeout:
                    LOGGER.error(f"Iteration timeout at message {loop_iteration}")
                    
                    if self.stuck_recovery_attempts < self.max_recovery_attempts:
                        self.stuck_recovery_attempts += 1
                        LOGGER.warning(f"Recovery attempt {self.stuck_recovery_attempts}/{self.max_recovery_attempts}")
                        await asyncio.sleep(10)
                        self.last_iteration_time = time.time()
                        continue
                    else:
                        await self._save_progress(interrupted=True)
                        raise TimeoutError(f"Scan stalled at iteration {loop_iteration}")
                
                if self.is_cancelled:
                    break
                
                if not (message.document or message.video):
                    continue
                
                if self.scan_type == 'document' and not message.document:
                    continue
                elif self.scan_type == 'media' and not message.video:
                    continue
                
                scanned_media_count += 1
                processed_messages += 1
                current_batch.append(message)
                self.scanned_message_ids.add(message.id)
                self.stuck_recovery_attempts = 0
                
                if current_time - self.last_minute_check >= 60:
                    self.messages_processed_this_minute = 0
                    self.last_minute_check = current_time
                self.messages_processed_this_minute += 1
                
                file_info = await scanner._extract_file_info(message)
                if file_info:
                    await database.add_catalog_entry(
                        self.channel_chat_id, 
                        message.id, 
                        file_info
                    )
                    cataloged_count += 1

                if completion_task is None and (self.our_active_links or self.pending_files):
                    completion_task = asyncio.create_task(self._wait_for_completion_callback_mode())

                if current_time - last_status_update >= status_update_interval:
                    progress_percent = (scanned_media_count / total_media_files * 100) if total_media_files > 0 else 0
                    scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "Media files"
                    
                    elapsed_total = current_time - self.scan_start_time
                    overall_rate = scanned_media_count / elapsed_total if elapsed_total > 0 else 0
                    
                    total_skipped = skipped_filter_mismatch + skipped_existing_files + skipped_duplicates_in_queue
                    filter_status = self._get_scan_filter_status()
                    
                    await self._safe_edit_message(self.status_message, 
                        f"**Scanning {scan_type_text}... ({scanned_media_count}/{total_media_files} - {progress_percent:.1f}%)**\n"
                        f"{filter_status}\n"
                        f"**Catalog:** Building ({cataloged_count} files cataloged)\n\n"
                        f"**Current Msg ID:** {message.id} | **Rate:** {overall_rate:.1f} files/s\n"
                        f"**Skipped:** {total_skipped}\n"
                        f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                        f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
                    )
                    last_status_update = current_time

                if len(current_batch) >= batch_size:
                    current_time = time.time()
                    
                    if current_time - last_rate_reset >= 30:
                        api_request_count = 0
                        files_processed_count = 0
                        last_rate_reset = current_time
                    
                    api_limit_hit = api_request_count >= max_api_requests_per_30s
                    files_limit_hit = files_processed_count >= max_files_per_30s
                    
                    if api_limit_hit or files_limit_hit:
                        wait_time = 30 - (current_time - last_rate_reset)
                        if wait_time > 0:
                            await self._safe_edit_message(self.status_message, 
                                f"⏳ **Rate limit protection - waiting {wait_time:.0f}s**\n\n"
                                f"**Processed:** {scanned_media_count}/{total_media_files}\n"
                                f"**Catalog:** {cataloged_count} files cataloged"
                            )
                            await asyncio.sleep(wait_time + 1)
                            api_request_count = 0
                            files_processed_count = 0
                            last_rate_reset = time.time()
                    
                    api_request_count += 1
                    files_processed_count += len(current_batch)
                    
                    self.last_batch_time = current_time
                    batch_skip_counts = await self._process_batch_with_skip_tracking(current_batch, scanner, processed_messages)
                    
                    skipped_filter_mismatch += batch_skip_counts['filter']
                    skipped_existing_files += batch_skip_counts['existing']
                    skipped_duplicates_in_queue += batch_skip_counts['queued']
                    
                    current_batch = []
                    
                    sleep_time = batch_sleep
                    if api_request_count > max_api_requests_per_30s * 0.8 or files_processed_count > max_files_per_30s * 0.8:
                        sleep_time = batch_sleep * 2
                    
                    await asyncio.sleep(sleep_time)
                    await self._save_progress()

            if current_batch and not self.is_cancelled:
                batch_skip_counts = await self._process_batch_with_skip_tracking(current_batch, scanner, processed_messages)
                skipped_filter_mismatch += batch_skip_counts['filter']
                skipped_existing_files += batch_skip_counts['existing']
                skipped_duplicates_in_queue += batch_skip_counts['queued']
                await self._save_progress()
            
            try:
                chat = await user.get_chat(self.channel_id)
                channel_username = chat.username if hasattr(chat, 'username') and chat.username else None
            except:
                channel_username = None
                
            await database.update_channel_metadata(
                self.channel_chat_id, 
                latest_msg_id=latest_msg_id,
                oldest_msg_id=oldest_msg_id if oldest_msg_id != float('inf') else None,
                total_cataloged=cataloged_count,
                channel_username=channel_username,
                catalog_status='complete'
            )
            
            self.completed_scan_type = "all" if not self.scan_type else self.scan_type
            await self._save_progress()
            
            if completion_task and not completion_task.done():
                completion_task.cancel()
                try:
                    await completion_task
                except asyncio.CancelledError:
                    pass

            scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "All media"
            total_pending = len(self.our_active_links) + len(self.pending_files)
            
            if total_pending > 0:
                await self._safe_edit_message(self.status_message, 
                    f"**✅ {scan_type_text} scan completed! ({scanned_media_count}/{total_media_files})**\n"
                    f"**Catalog:** Built with {cataloged_count} files (future scans will be instant!)\n\n"
                    f"**📥 Downloads queued: {total_pending} files**\n"
                    f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                    f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}\n\n"
                    f"**⏳ Waiting for queued processes to complete...**"
                )
            else:
                await self._safe_edit_message(self.status_message, 
                    f"**✅ {scan_type_text} scan completed! ({scanned_media_count}/{total_media_files})**\n"
                    f"**Catalog:** Built with {cataloged_count} files (future scans will be instant!)\n\n"
                    f"**📥 No downloads needed** - all files already exist or filtered out\n"
                    f"**Final Results:** {self.completed_count} completed | {self.failed_count} failed"
                )
                return

            await self._wait_for_completion_callback_mode()
            await self._show_final_results(processed_messages, 0)
            
        except TimeoutError as e:
            LOGGER.error(f"Scan timeout: {e}")
            await self._safe_edit_message(self.status_message, f"❌ **Scan timeout: {str(e)}**")
            await self._save_progress(interrupted=True)
            
            if self.our_active_links or self.pending_files:
                await self._wait_for_completion_callback_mode()
                
        except Exception as e:
            LOGGER.error(f"Processing error: {e}", exc_info=True)
            await self._save_progress(interrupted=True)
            raise

    async def _handle_our_task_completion(self, link, name, size, files, folders, mime_type):
        """Handle completion of our tracked task - with database tracking"""
        try:
            self.our_active_links.discard(link)
            file_item = self.link_to_file_mapping.pop(link, None)
            
            if not file_item:
                return
                
            sanitized_name = self._generate_clean_filename(file_item['file_info'])
            self.pending_sanitized_names.discard(sanitized_name)
            file_unique_id = file_item['file_info'].get('file_unique_id')
            if file_unique_id:
                self.pending_file_ids.discard(file_unique_id)

            self.completed_count += 1
            
            await database.add_file_entry(
                channel_id=self.channel_chat_id, 
                message_id=file_item['message_id'], 
                file_data=file_item['file_info']
            )
            
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"Error handling task completion: {e}")

    async def _handle_our_task_failure(self, link, error):
        """Handle failure of our tracked task - with database tracking to prevent retries"""
        try:
            self.our_active_links.discard(link)
            file_item = self.link_to_file_mapping.pop(link, None)
            
            if file_item:
                sanitized_name = self._generate_clean_filename(file_item['file_info'])
                self.pending_sanitized_names.discard(sanitized_name)
                file_unique_id = file_item['file_info'].get('file_unique_id')
                if file_unique_id:
                    self.pending_file_ids.discard(file_unique_id)
                
                await database.add_failed_file_entry(
                    channel_id=self.channel_chat_id,
                    message_id=file_item['message_id'], 
                    file_data=file_item['file_info'],
                    error_reason=str(error)
                )
            
            self.failed_count += 1
            
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"Error handling task failure: {e}")

    async def _safe_edit_message(self, message, text):
        """Safely edit message with FloodWait handling"""
        try:
            await edit_message(message, text)
        except FloodWait as e:
            LOGGER.warning(f"FloodWait for {e.value} seconds")
            await asyncio.sleep(e.value)
            try:
                await edit_message(message, text)
            except Exception as retry_error:
                LOGGER.error(f"Failed to edit message after FloodWait: {retry_error}")
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"Message edit error: {e}")

    async def new_event(self):
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [-feither filter_text] [--no-caption] [-type document|media] [-from msg_id] [-to msg_id]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`  ← Must contain ALL words\n"
                "`/cleech -ch @movies_channel -feither 2024 BluRay`  ← Must contain ANY word\n"
                "`/cleech -ch @docs_channel -f PRT x265 -type document`\n"
                "`/cleech -ch @channel -feither HEVC x264 -from 100000`\n\n"
                "**Filter Options:**\n"
                "• `-f <words>` - **AND filter**: Must contain ALL specified words\n"
                "• `-feither <words>` - **OR filter**: Must contain ANY of the specified words\n"
                "• Cannot use both `-f` and `-feither` together\n\n"
                "**Message Range:**\n"
                "• `-from <msg_id>` - Start from specific message ID\n"
                "• `-to <msg_id>` - End at specific message ID\n"
                "• Without range: Scans newest → oldest (default)\n"
                "• With range: Auto-determines direction based on from/to values"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.filter_mode = args.get('filter_mode', 'and')
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.scan_type = args.get('type')
        
        if not self.filter_tags:
            self.filter_mode = 'and'
        
        self.from_msg_id = args.get('from_msg_id')
        self.to_msg_id = args.get('to_msg_id')
        self.scan_direction = self._determine_scan_direction()

        self._register_coordinator()

        try:
            progress = await database.get_leech_progress(self.message.from_user.id, self.channel_id)
            if progress:
                self.resume_mode = True
                self.scanned_message_ids = set(progress.get("scanned_message_ids", []))
                self.completed_scan_type = progress.get("completed_scan_type")
                
                if not self.scan_type and progress.get("scan_type"):
                    self.scan_type = progress.get("scan_type")
                
                scanned_ids = self.scanned_message_ids
                if scanned_ids:
                    self.resume_from_msg_id = min(scanned_ids)
                    await send_message(self.message, f"⏸️ Resuming scan from message ID: {self.resume_from_msg_id}")
                else:
                    self.resume_from_msg_id = 0
                    await send_message(self.message, "🔄 Starting fresh scan")
                
                if self.completed_scan_type:
                    await send_message(self.message, f"✅ Scan for `{self.completed_scan_type}` already completed. Resuming next scan type.")
            else:
                self.scanned_message_ids = set()
                self.resume_from_msg_id = 0

            try:
                chat = await user.get_chat(self.channel_id)
                self.channel_chat_id = chat.id
            except Exception as e:
                await send_message(self.message, f"Could not resolve channel: {e}")
                return

            self.operation_key = await channel_status.start_operation(
                self.message.from_user.id, self.channel_id, "simple_channel_leech"
            )

            filter_text = self._get_filter_description()
            scan_type_text = f" of type `{self.scan_type}`" if self.scan_type else " of all media types"
            range_text = self._get_range_description()
            
            self.status_message = await send_message(
                self.message,
                f"**Channel Leech Starting{' (Resumed)' if self.resume_mode else ''}...**\n"
                f"**Channel:** `{self.channel_id}`\n"
                f"**Scanning:**{scan_type_text}\n"
                f"**Filter:**{filter_text}\n"
                f"**Range:**{range_text}"
            )

            try:
                await self._coordinate_simple_leech()
            except Exception as e:
                LOGGER.error(f"Coordination error: {e}", exc_info=True)
                await self._safe_edit_message(self.status_message, f"Error: {str(e)}")
            finally:
                if self.operation_key:
                    await channel_status.stop_operation(self.operation_key)
                if not self.is_cancelled:
                    await database.clear_leech_progress(self.message.from_user.id, self.channel_id)
                    
        except Exception as e:
            LOGGER.error(f"Critical error in new_event: {e}", exc_info=True)
            await send_message(self.message, f"❌ Error: {str(e)}")
        finally:
            self._unregister_coordinator()

    async def _coordinate_simple_leech(self):
        if self.use_catalog:
            catalog_mode = await self._determine_catalog_mode()
            self.catalog_mode = catalog_mode
            
            if catalog_mode == 'filter_only':
                await self._process_from_catalog()
                return
            elif catalog_mode == 'incremental':
                await self._incremental_scan_and_catalog()
                await self._process_from_catalog()
                return
            else:
                await self._full_scan_and_catalog()
                return

    async def _wait_for_completion_callback_mode(self):
        """Wait for completion using TaskListener callbacks with periodic status updates"""
        last_status_update = 0
        status_update_interval = 10
        start_time = time.time()
        
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            
            current_time = time.time()
            
            if current_time - last_status_update >= status_update_interval:
                try:
                    total_pending = len(self.our_active_links) + len(self.pending_files)
                    
                    if total_pending > 0:
                        total_processed = self.completed_count + self.failed_count
                        total_started = total_processed + total_pending
                        progress_percent = (total_processed / total_started * 100) if total_started > 0 else 0
                        
                        elapsed_time = current_time - start_time
                        downloads_per_minute = (total_processed / elapsed_time * 60) if elapsed_time > 0 else 0
                        
                        eta_minutes = (total_pending / downloads_per_minute) if downloads_per_minute > 0 else 0
                        eta_text = f" (ETA: {eta_minutes:.1f}m)" if eta_minutes > 0 and eta_minutes < 300 else ""
                        
                        scan_type_text = f" {self.scan_type}" if self.scan_type else ""
                        
                        if self.catalog_mode == 'filter_only':
                            status_text = (
                                f"**✅ Catalog processed! Downloads in progress...**\n\n"
                                f"**📊 Progress: {progress_percent:.1f}% ({total_processed}/{total_started})**\n"
                                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Queued:** {len(self.pending_files)}\n"
                                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}\n"
                                f"**Rate:** {downloads_per_minute:.1f} files/min{eta_text}\n\n"
                                f"**Filter:** {self._get_filter_description()}\n"
                                f"**Range:** {self._get_range_description()}\n\n"
                                f"**⏳ Processing remaining {total_pending} downloads...**"
                            )
                        else:
                            status_text = (
                                f"**✅{scan_type_text.title()} scan completed! Downloads in progress...**\n\n"
                                f"**📊 Progress: {progress_percent:.1f}% ({total_processed}/{total_started})**\n"
                                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Queued:** {len(self.pending_files)}\n"
                                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}\n"
                                f"**Rate:** {downloads_per_minute:.1f} files/min{eta_text}\n\n"
                                f"**Filter:** {self._get_filter_description()}\n"
                                f"**Range:** {self._get_range_description()}\n\n"
                                f"**⏳ Processing remaining {total_pending} downloads...**"
                            )
                        
                        await self._safe_edit_message(self.status_message, status_text)
                        last_status_update = current_time
                        
                except Exception as e:
                    LOGGER.error(f"Error updating status during downloads: {e}")
            
            await asyncio.sleep(10)

    async def _restore_resume_state(self, scanner):
        try:
            progress = await database.get_leech_progress(self.message.from_user.id, self.channel_id)
            if not progress: 
                return
            
            pending_msg_ids = progress.get("pending_files", [])
            for msg_id in pending_msg_ids:
                try:
                    message = await user.get_messages(self.channel_chat_id, msg_id)
                    file_info = await scanner._extract_file_info(message)
                    if file_info:
                        message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{msg_id}"
                        self.pending_files.append({
                            'url': message_link,
                            'filename': file_info['file_name'],
                            'message_id': msg_id,
                            'file_info': file_info,
                        })
                        sanitized_name = self._generate_clean_filename(file_info)
                        self.pending_sanitized_names.add(sanitized_name)
                        if file_info.get('file_unique_id'):
                            self.pending_file_ids.add(file_info['file_unique_id'])
                except Exception as e:
                    LOGGER.warning(f"Could not restore pending file {msg_id}: {e}")
            
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    if row["_id"].startswith("https://t.me/c/"):
                        self.our_active_links.add(row["_id"])
        except Exception as e:
            LOGGER.error(f"Error restoring resume state: {e}")

    async def _process_batch_with_skip_tracking(self, message_batch, scanner, processed_so_far):
        skip_counts = {'filter': 0, 'existing': 0, 'queued': 0}
        
        for message in message_batch:
            if self.is_cancelled:
                break
                
            try:
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                if not self._check_filter_match(file_info['search_text']):
                    skip_counts['filter'] += 1
                    continue

                sanitized_name = self._generate_clean_filename(file_info)
                file_unique_id = file_info.get('file_unique_id')
                
                if await database.check_file_exists(file_info=file_info):
                    skip_counts['existing'] += 1
                    continue
                    
                if sanitized_name in self.pending_sanitized_names:
                    skip_counts['queued'] += 1
                    continue
                    
                if file_unique_id and file_unique_id in self.pending_file_ids:
                    skip_counts['queued'] += 1
                    continue
                    
                message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{message.id}"
                self.pending_files
