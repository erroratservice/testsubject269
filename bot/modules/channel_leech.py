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
def enhance_prt_filename(filename):
    """Add XXX before quality and PRT ONLY before .mp4/.mkv extension"""
    if not filename:
        return filename
    
    # Add XXX before quality if missing
    if not re.search(r'\bXXX\b', filename, re.IGNORECASE):
        filename = re.sub(r'(\.720p|\.1080p)', r'.XXX\1', filename, flags=re.IGNORECASE)
    
    # Check if already has .PRT before extension
    if re.search(r'\.PRT(?:\.(mp4|mkv))?$', filename, re.IGNORECASE):
        return filename
    
    # Split filename to find extension
    basename, dot, ext = filename.rpartition('.')
    ext_with_dot = f".{ext}" if dot else ""
    
    # Only add .PRT before .mp4 or .mkv
    if ext_with_dot.lower() in [".mp4", ".mkv"]:
        if not basename.lower().endswith('.prt'):
            return f"{basename}.PRT{ext_with_dot}"
        else:
            return filename
    else:
        # No valid extension, add .PRT.mp4 at the end
        if not filename.lower().endswith('.prt'):
            return f"{filename}.PRT.mp4"
        else:
            return f"{filename}.mp4"
    
class SimpleChannelLeechCoordinator(TaskListener):
    # Memory-safe coordinator tracking using WeakValueDictionary
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
        self.prt_mode = False
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
        
        # Message range support
        self.from_msg_id = None
        self.to_msg_id = None
        self.scan_direction = 'newest_to_oldest'
        
        # Unique coordinator ID for memory management
        SimpleChannelLeechCoordinator._coordinator_counter += 1
        self._coordinator_id = f"{message.from_user.id}_{self.channel_id}_{SimpleChannelLeechCoordinator._coordinator_counter}"
        self._is_active = False
        
        # Essential timing variables
        self.last_message_time = time.time()
        self.last_batch_time = time.time()
        self.scan_start_time = time.time()
        self.messages_processed_this_minute = 0
        self.last_minute_check = time.time()
        super().__init__()

    # Task completion handler
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

    # Task failure handler
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

    # Memory management utilities
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

    async def is_download_duplicate(self, file_item):
        """Enhanced duplicate checking with queue integration and failed file handling"""
        try:
            sanitized_name = self.generate_clean_filename(file_item['file_info'])
            file_info = file_item['file_info']
            
            # Check if already in our pending queue
            if any(f['file_info'].get('file_unique_id') == file_info.get('file_unique_id') 
                   for f in self.pending_files):
                return True
            
            # Check if in any active link's queue
            for link_obj in list(self.our_active_links.values()):
                if hasattr(link_obj, 'listener') and hasattr(link_obj.listener, 'name'):
                    if link_obj.listener.name == sanitized_name:
                        return True
            
            # Check if file already exists (includes both completed AND failed files)
            # MODIFIED LINE - Pass prt_mode parameter
            if await database.check_file_exists(file_info=file_info, prt_mode=self.prt_mode):
                return True
            
            return False
        except Exception as e:
            LOGGER.error(f"[cleech] Error checking duplicate: {e}")
            return False

    async def _check_bot_task_queue(self, sanitized_name, url):
        """Check if file is already in bot's global download queue"""
        try:
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            
            # Check by URL
            if await database._db.tasks[bot_token_first_half].find_one({"_id": url}):
                return True
            
            # Check by similar filename pattern
            base_name = os.path.splitext(sanitized_name)[0]
            cursor = database._db.tasks[bot_token_first_half].find({})
            
            async for task in cursor:
                task_url = task.get('_id', '')
                if task_url.startswith('https://t.me/') and base_name.lower() in task_url.lower():
                    return True
            
            return False
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error checking bot task queue: {e}")
            return False

    async def _full_scan(self):
        completion_task = None
        """Simple channel scan and download"""
        try:
            await self._safe_edit_message(self.status_message, 
                "**🚀 Starting channel scan...**")
            
            scanner = ChannelScanner(
                user_client=user,
                bot_client=bot,
                channel_id=self.channel_id,
                filter_tags=self.filter_tags,
                batch_size=200,
                max_messages=0
            )   
            
            # Scan variables
            batch_size = 100
            batch_sleep = 5
            processed_messages = 0
            
            # Timing and throttling
            last_status_update = 0
            status_update_interval = 10
            self.scan_start_time = time.time()
            self.iteration_timeout = 60
            self.stuck_recovery_attempts = 0
            self.max_recovery_attempts = 3
            self.last_iteration_time = time.time()
            
            # Rate limiting
            api_request_count = 0
            files_processed_count = 0
            last_rate_reset = time.time()
            max_api_requests_per_30s = 25
            max_files_per_30s = 500
            
            # Skip tracking
            skipped_filter_mismatch = 0
            skipped_existing_files = 0
            skipped_duplicates_in_queue = 0
            
            if self.resume_mode:
                await self._restore_resume_state(scanner)
                while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                    await self._start_next_download()

            # REMOVED: completion_task creation here - will run after scan completes

            # Get scan totals
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
            
            # Main scanning loop
            async for message in message_iterator:
                loop_iteration += 1
                current_time = time.time()
                self.last_message_time = current_time
                self.last_iteration_time = current_time
                
                if not self._should_process_message(message):
                    continue
                
                if self.to_msg_id and self.scan_direction in ['newest_to_oldest', 'to_specified'] and message.id <= self.to_msg_id:
                    break
                
                # Progress logging for large scans
                if loop_iteration % 5000 == 0:
                    elapsed = current_time - self.scan_start_time
                    rate = loop_iteration / elapsed if elapsed > 0 else 0
                    LOGGER.info(f"[cleech] Progress: {loop_iteration} messages scanned in {elapsed:.1f}s (rate: {rate:.1f} msg/s)")
                
                # Check for iteration timeout
                iteration_idle = current_time - self.last_iteration_time
                if iteration_idle > self.iteration_timeout:
                    LOGGER.error(f"[cleech] Iteration timeout at message {loop_iteration}")
                    
                    if self.stuck_recovery_attempts < self.max_recovery_attempts:
                        self.stuck_recovery_attempts += 1
                        LOGGER.warning(f"[cleech] Recovery attempt {self.stuck_recovery_attempts}/{self.max_recovery_attempts}")
                        await asyncio.sleep(10)
                        self.last_iteration_time = time.time()
                        continue
                    else:
                        await self._save_progress(interrupted=True)
                        raise TimeoutError(f"Scan stalled at iteration {loop_iteration}")
                
                if self.is_cancelled:
                    break
                
                # Only process media messages
                if not (message.document or message.video):
                    continue
                
                # Respect scan type
                if self.scan_type == 'document' and not message.document:
                    continue
                elif self.scan_type == 'media' and not message.video:
                    continue
                
                scanned_media_count += 1
                processed_messages += 1
                current_batch.append(message)
                self.scanned_message_ids.add(message.id)
                self.stuck_recovery_attempts = 0
                
                # Track processing rate
                if current_time - self.last_minute_check >= 60:
                    self.messages_processed_this_minute = 0
                    self.last_minute_check = current_time
                self.messages_processed_this_minute += 1
                
                # REMOVED: completion_task creation here - will run after scan completes

                # Status updates
                if current_time - last_status_update >= status_update_interval:
                    progress_percent = (scanned_media_count / total_media_files * 100) if total_media_files > 0 else 0
                    scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "Media files"
                    
                    elapsed_total = current_time - self.scan_start_time
                    overall_rate = scanned_media_count / elapsed_total if elapsed_total > 0 else 0
                    
                    total_skipped = skipped_filter_mismatch + skipped_existing_files + skipped_duplicates_in_queue
                    filter_status = self._get_scan_filter_status()
                    
                    await self._safe_edit_message(self.status_message, 
                        f"**Scanning {scan_type_text}... ({scanned_media_count}/{total_media_files} - {progress_percent:.1f}%)**\n"
                        f"{filter_status}\n\n"
                        f"**Current Msg ID:** {message.id} | **Rate:** {overall_rate:.1f} files/s\n"
                        f"**Skipped:** {total_skipped}\n"
                        f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                        f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
                    )
                    last_status_update = current_time

                # Batch processing
                if len(current_batch) >= batch_size:
                    current_time = time.time()
                    
                    # Rate limiting
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
                                f"**Processed:** {scanned_media_count}/{total_media_files}"
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

            # Process remaining batch
            if current_batch and not self.is_cancelled:
                batch_skip_counts = await self._process_batch_with_skip_tracking(current_batch, scanner, processed_messages)
                skipped_filter_mismatch += batch_skip_counts['filter']
                skipped_existing_files += batch_skip_counts['existing']
                skipped_duplicates_in_queue += batch_skip_counts['queued']
                await self._save_progress()
            
            self.completed_scan_type = "all" if not self.scan_type else self.scan_type
            await self._save_progress()
            

            # Cancel completion task
            if completion_task and not completion_task.done():
                completion_task.cancel()
                try:
                    await completion_task
                except asyncio.CancelledError:
                    pass
            
            # Wait for completion with download progress updates
            await self._wait_for_completion_callback_mode()
            await self._show_final_results(processed_messages, 0)
            
        except TimeoutError as e:
            LOGGER.error(f"[cleech] Timeout error: {e}")
            await self._safe_edit_message(self.status_message, f"❌ **Scan timeout: {str(e)}**")
            await self._save_progress(interrupted=True)
            
            if self.our_active_links or self.pending_files:
                await self._wait_for_completion_callback_mode()
                
        except Exception as e:
            LOGGER.error(f"[cleech] Processing error: {e}", exc_info=True)
            await self._save_progress(interrupted=True)
            raise

    async def _handle_our_task_completion(self, link, name, size, files, folders, mime_type):
        """Handle completion of our tracked task - with database tracking"""
        try:
            self.our_active_links.discard(link)
            file_item = self.link_to_file_mapping.pop(link, None)
            
            if not file_item:
                return
                
            # Clean up queue tracking
            sanitized_name = self._generate_clean_filename(file_item['file_info'])
            self.pending_sanitized_names.discard(sanitized_name)
            file_unique_id = file_item['file_info'].get('file_unique_id')
            if file_unique_id:
                self.pending_file_ids.discard(file_unique_id)

            # SUCCESS: Add to database to prevent future downloads
            self.completed_count += 1
            
            # CRITICAL: Mark file as completed in database
            await database.add_file_entry(
                channel_id=self.channel_chat_id, 
                message_id=file_item['message_id'], 
                file_data=file_item['file_info']
            )
            
            LOGGER.info(f"[cleech] ✅ Successfully downloaded and marked: {sanitized_name}")
            
            # Start next downloads
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error handling task completion: {e}")

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
                
                # CRITICAL: Mark failed file in database to prevent infinite retries
                await database.add_failed_file_entry(
                    channel_id=self.channel_chat_id,
                    message_id=file_item['message_id'], 
                    file_data=file_item['file_info'],
                    error_reason=str(error)
                )
                
                LOGGER.info(f"[cleech] ❌ Failed download marked to prevent retry: {sanitized_name} (Error: {error})")
            
            self.failed_count += 1
            
            # Start next downloads
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error handling task failure: {e}")

    async def _safe_edit_message(self, message, text):
        """Safely edit message with FloodWait handling"""
        try:
            await edit_message(message, text)
        except FloodWait as e:
            LOGGER.warning(f"[cleech] FloodWait for {e.value} seconds")
            await asyncio.sleep(e.value)
            try:
                await edit_message(message, text)
            except Exception as retry_error:
                LOGGER.error(f"[cleech] Failed to edit message after FloodWait: {retry_error}")
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"[cleech] Message edit error: {e}")

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

        # Parse arguments
        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.filter_mode = args.get('filter_mode', 'and')
        if self.filter_tags:
            self.prt_mode = any('PRT' in str(tag).upper() for tag in self.filter_tags)        
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.scan_type = args.get('type')
        
        if not self.filter_tags:
            self.filter_mode = 'and'
        
        # Message range support
        self.from_msg_id = args.get('from_msg_id')
        self.to_msg_id = args.get('to_msg_id')
        self.scan_direction = self._determine_scan_direction()

        # Register coordinator
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

            # Status message
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
                LOGGER.error(f"[cleech] Coordination error: {e}", exc_info=True)
                await self._safe_edit_message(self.status_message, f"Error: {str(e)}")
            finally:
                if self.operation_key:
                    await channel_status.stop_operation(self.operation_key)
                if not self.is_cancelled:
                    await database.clear_leech_progress(self.message.from_user.id, self.channel_id)
                    
        except Exception as e:
            LOGGER.error(f"[cleech] Critical error in new_event: {e}", exc_info=True)
            await send_message(self.message, f"❌ Error: {str(e)}")
        finally:
            self._unregister_coordinator()

    async def _coordinate_simple_leech(self):
        """Simple scan - no catalog complexity"""
        try:
            self._register_coordinator()
            
            # Always do a simple scan (no catalog)
            await self._full_scan()
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error in coordination: {e}", exc_info=True)
            raise
        finally:
            self._unregister_coordinator()

    async def _wait_for_completion_callback_mode(self):
        """Wait for completion using TaskListener callbacks with detailed status updates"""
        last_status_update = 0
        status_update_interval = 10  # Update every 10 seconds
        start_time = time.time()
        
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            # Start new downloads if slots available
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            
            current_time = time.time()
            
            # Update status message every 10 seconds
            if current_time - last_status_update >= status_update_interval:
                try:
                    total_pending = len(self.our_active_links) + len(self.pending_files)
                    
                    if total_pending > 0:
                        # Calculate progress statistics
                        total_processed = self.completed_count + self.failed_count
                        total_started = total_processed + total_pending
                        progress_percent = (total_processed / total_started * 100) if total_started > 0 else 0
                        
                        elapsed_time = current_time - start_time
                        downloads_per_minute = (total_processed / elapsed_time * 60) if elapsed_time > 0 else 0
                        
                        # Estimate time remaining
                        eta_minutes = (total_pending / downloads_per_minute) if downloads_per_minute > 0 else 0
                        eta_text = f" (ETA: {eta_minutes:.1f}m)" if eta_minutes > 0 and eta_minutes < 300 else ""
                        
                        # Determine scan type text
                        scan_type_text = f" {self.scan_type}" if self.scan_type else ""
                        
                        # Create status text (NO catalog_mode check!)
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
                    LOGGER.error(f"[cleech] Error updating status during downloads: {e}")
            
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
                    file_info = scanner._extract_file_info(message)
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
                    LOGGER.warning(f"[cleech] Could not restore pending file {msg_id}: {e}")
            
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    if row["_id"].startswith("https://t.me/c/"):
                        self.our_active_links.add(row["_id"])
        except Exception as e:
            LOGGER.error(f"[cleech] Error restoring resume state: {e}")

    async def _process_batch_with_skip_tracking(self, message_batch, scanner, processed_so_far):
        skip_counts = {'filter': 0, 'existing': 0, 'queued': 0}
        
        for message in message_batch:
            if self.is_cancelled:
                break
                
            try:
                file_info = scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                # Check filter match
                if not self._check_filter_match(file_info['search_text']):
                    skip_counts['filter'] += 1
                    continue

                # Generate sanitized filename
                sanitized_name = self._generate_clean_filename(file_info)
                file_unique_id = file_info.get('file_unique_id')
                
                # Check database for existing files
                if await database.check_file_exists(file_info=file_info):
                    skip_counts['existing'] += 1
                    continue
                    
                # Check processing queue
                if sanitized_name in self.pending_sanitized_names:
                    skip_counts['queued'] += 1
                    continue
                    
                if file_unique_id and file_unique_id in self.pending_file_ids:
                    skip_counts['queued'] += 1
                    continue
                    
                message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{message.id}"
                self.pending_files.append({
                    'url': message_link,
                    'filename': file_info['file_name'],
                    'message_id': message.id,
                    'file_info': file_info,
                })
                
                # Track in queue
                self.pending_sanitized_names.add(sanitized_name)
                if file_unique_id:
                    self.pending_file_ids.add(file_unique_id)
                    
            except Exception as e:
                LOGGER.error(f"[cleech] Error processing message {message.id}: {e}")

        # Start downloads
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
            
        return skip_counts

    async def _start_next_download(self):
        """Start next download - files are pre-filtered, no duplicate check needed"""
        if not self.pending_files or len(self.our_active_links) >= self.max_concurrent:
            return
        
        # Files in pending_files are already verified non-duplicates
        # Just pop and send - no need to check again
        file_item = self.pending_files.pop(0)
        
        # Add to tracking
        sanitized_name = self._generate_clean_filename(file_item['file_info'])
        url = file_item['url']
        
        # Files are already in pending sets from when added to queue
        # Just add to active links
        self.our_active_links.add(url)
        self.link_to_file_mapping[url] = file_item
        
        try:
            COMMAND_CHANNEL_ID = int(config_dict.get('LEECH_DUMP_CHAT') or self.message.chat.id)
            clean_name = self._generate_clean_filename(file_item['file_info'])
            leech_cmd = f'/leech {file_item["url"]} -n {clean_name}'
            
            command_message = await user.send_message(chat_id=COMMAND_CHANNEL_ID, text=leech_cmd)
            actual_stored_url = f"https://t.me/c/{str(COMMAND_CHANNEL_ID)[4:]}/{command_message.id}"
            
            await asyncio.sleep(2)
            
            # Update tracking with actual URL
            self.our_active_links.discard(url)
            self.our_active_links.add(actual_stored_url)
            self.link_to_file_mapping[actual_stored_url] = file_item
            self.link_to_file_mapping.pop(url, None)
            
            
        except Exception as e:
            # Clean up tracking on failure
            self.our_active_links.discard(url)
            self.link_to_file_mapping.pop(url, None)
            # Remove from pending sets so it can be retried later if needed
            self.pending_sanitized_names.discard(sanitized_name)
            file_unique_id = file_item['file_info'].get('file_unique_id')
            if file_unique_id:
                self.pending_file_ids.discard(file_unique_id)

    async def _save_progress(self, interrupted=False):
        """Save progress for resume capability"""
        try:
            progress = {
                "user_id": self.message.from_user.id,
                "channel_id": self.channel_id,
                "filter_tags": self.filter_tags,
                "scan_type": self.scan_type,
                "scanned_message_ids": list(self.scanned_message_ids),
                "pending_files": [f['message_id'] for f in self.pending_files] + 
                                [item['message_id'] for item in self.link_to_file_mapping.values()],
                "timestamp": datetime.utcnow().isoformat(),
                "interrupted": interrupted,
                "completed_scan_type": self.completed_scan_type
            }
            await database.save_leech_progress(self.message.from_user.id, self.channel_id, progress)
                
        except Exception as e:
            LOGGER.error(f"[cleech] Error saving progress: {e}")

    async def _show_final_results(self, processed_messages, skipped_duplicates):
        """Display final results after completion"""
        total_attempted = self.completed_count + self.failed_count
        success_rate = (self.completed_count / total_attempted * 100) if total_attempted > 0 else 0
        
        text = (
            f"**✅ Channel Leech Completed!**\n\n"
            f"**📊 Scanning Results:**\n"
            f"**Scanned:** {processed_messages:,} items\n"
            f"**Downloaded:** {self.completed_count} | **Failed:** {self.failed_count}\n"
            f"**Success Rate:** {success_rate:.1f}%"
        )
        
        await self._safe_edit_message(self.status_message, text)

    def _generate_clean_filename(self, file_info):
        """Generate clean filename with optional PRT enhancement"""
        original_filename = file_info.get('file_name', '')
        base_name = original_filename
        
        if self.use_caption_as_filename and file_info.get('caption_first_line'):
            base_name = file_info['caption_first_line'].strip()
        
        clean_base = sanitize_filename(base_name)
        
        # Apply PRT enhancement if in prt_mode BEFORE adding extension
        if self.prt_mode:
            clean_base = enhance_prt_filename(clean_base)
        
        # Add extension if missing
        original_ext = os.path.splitext(original_filename)[1]
        media_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
                            '.mp3', '.flac', '.wav', '.aac', '.m4a', '.ogg',
                            '.zip', '.rar', '.7z', '.tar', '.gz'}
        
        if original_ext.lower() in media_extensions and not clean_base.lower().endswith(original_ext.lower()):
            clean_base += original_ext
        
        return clean_base

    def _parse_arguments(self, args):
        parsed, i = {}, 0
        while i < len(args):
            if args[i] == '-ch':
                if i + 1 < len(args):
                    parsed['channel'] = args[i+1]
                    i += 2
                else:
                    i += 1
            elif args[i] == '-f':
                filter_words = []
                i += 1
                while i < len(args) and not args[i].startswith('-'):
                    filter_words.append(args[i])
                    i += 1
                if filter_words:
                    parsed['filter'] = filter_words
                    parsed['filter_mode'] = 'and'
            elif args[i] == '-feither':
                filter_words = []
                i += 1
                while i < len(args) and not args[i].startswith('-'):
                    filter_words.append(args[i])
                    i += 1
                if filter_words:
                    parsed['filter'] = filter_words
                    parsed['filter_mode'] = 'or'
            elif args[i] == '--no-caption':
                parsed['no_caption'] = True
                i += 1
            elif args[i] == '-type':
                if i + 1 < len(args) and args[i+1].lower() in ['document', 'media']:
                    parsed['type'] = args[i+1].lower()
                    i += 2
                else:
                    i += 1
            elif args[i] == '-from':
                if i + 1 < len(args) and args[i+1].isdigit():
                    parsed['from_msg_id'] = int(args[i+1])
                    i += 2
                else:
                    i += 1
            elif args[i] == '-to':
                if i + 1 < len(args) and args[i+1].isdigit():
                    parsed['to_msg_id'] = int(args[i+1])
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True
        LOGGER.info(f"Cancelling Channel Leech for {self.channel_id}")
        asyncio.create_task(self._save_progress(interrupted=True))
        self._unregister_coordinator()

    def __del__(self):
        """Ensure coordinator is unregistered on garbage collection"""
        try:
            if hasattr(self, '_is_active') and self._is_active:
                pass
        except:
            pass

# Keep existing classes unchanged
class ChannelScanListener(TaskListener):
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.scanner = None
        super().__init__()

    async def new_event(self):
        text = self.message.text.split()
        if len(text) < 2:
            await send_message(self.message, "**Usage:** `/scan <channel_id> [filter]`")
            return
        self.channel_id = text[1]
        self.filter_tags = text[2:] if len(text) > 2 else []
        if not user:
            await send_message(self.message, "User session required!")
            return
        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        status_msg = await send_message(
            self.message, 
            f"Starting scan for `{self.channel_id}`{filter_text}"
        )
        try:
            self.scanner = ChannelScanner(
                user_client=user,
                bot_client=bot,
                channel_id=self.channel_id,
                 filter_tags=self.filter_tags
            )    
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
        except Exception as e:
            LOGGER.error(f"[CHANNEL-SCANNER] Error: {e}")
            await edit_message(status_msg, f"Scan failed: {str(e)}")

    def cancel_task(self):
        self.is_cancelled = True
        if self.scanner:
            self.scanner.running = False

@new_task
async def channel_scan(client, message):
    await ChannelScanListener(user, message).new_event()

@new_task
async def simple_channel_leech_cmd(client, message):
    await SimpleChannelLeechCoordinator(client, message).new_event()

bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))
bot.add_handler(MessageHandler(
    simple_channel_leech_cmd,
    filters=command("cleech") & CustomFilters.authorized
))
