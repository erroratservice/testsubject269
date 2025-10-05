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
from datetime import datetime

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
    # Memory-safe coordinator tracking using WeakValueDictionary
    _active_coordinators = weakref.WeakValueDictionary()
    _coordinator_counter = 0

    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None
        self.filter_tags = []
        self.filter_mode = 'and'  # NEW: 'and' or 'or'
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
        
        # Message range support
        self.from_msg_id = None
        self.to_msg_id = None
        self.scan_direction = 'newest_to_oldest'  # Default
        
        # Unique coordinator ID for memory management
        SimpleChannelLeechCoordinator._coordinator_counter += 1
        self._coordinator_id = f"{message.from_user.id}_{self.channel_id}_{SimpleChannelLeechCoordinator._coordinator_counter}"
        self._is_active = False
        
        # Debugging variables
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
        # WeakValueDictionary automatically cleans up garbage collected references
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
            return 'newest_to_oldest'  # Default behavior
        
        if self.from_msg_id and self.to_msg_id:
            if self.from_msg_id > self.to_msg_id:
                return 'newest_to_oldest'  # from=newer, to=older
            else:
                return 'oldest_to_newest'  # from=older, to=newer
        
        if self.from_msg_id:
            return 'from_specified'  # Start from specific message, go to oldest
        
        if self.to_msg_id:
            return 'to_specified'  # Start from newest, go to specified message
        
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

    def _check_filter_match(self, search_text):
        """Enhanced filter matching with AND/OR logic"""
        if not self.filter_tags:
            return True  # No filter = match all
        
        search_text_lower = search_text.lower()
        
        if self.filter_mode == 'or':
            # OR logic: Match if ANY tag is found
            return any(tag.lower() in search_text_lower for tag in self.filter_tags)
        else:
            # AND logic: Match if ALL tags are found (existing behavior)
            return all(tag.lower() in search_text_lower for tag in self.filter_tags)

    def _should_process_message(self, message):
        """Check if message is within specified range"""
        msg_id = message.id
        
        if self.from_msg_id and self.to_msg_id:
            # Range specified: check if message is within bounds
            min_id = min(self.from_msg_id, self.to_msg_id)
            max_id = max(self.from_msg_id, self.to_msg_id)
            return min_id <= msg_id <= max_id
        
        if self.from_msg_id:
            # Only from specified: process messages <= from_msg_id
            return msg_id <= self.from_msg_id
        
        if self.to_msg_id:
            # Only to specified: process messages >= to_msg_id
            return msg_id >= self.to_msg_id
        
        # No range specified: process all messages
        return True

    def _get_scan_offset_and_limit(self):
        """Get appropriate offset_id for Pyrogram get_chat_history"""
        if self.scan_direction == 'oldest_to_newest':
            # For oldest to newest, we need to start from the older message
            if self.from_msg_id and self.to_msg_id:
                return min(self.from_msg_id, self.to_msg_id) - 1
            return 0  # Start from very beginning
        else:
            # For newest to oldest (default)
            if self.from_msg_id:
                return self.from_msg_id
            return self.resume_from_msg_id if self.resume_from_msg_id > 0 else 0

    async def _handle_our_task_completion(self, link, name, size, files, folders, mime_type):
        """Handle completion of our tracked task - called by TaskListener callback"""
        try:
            # Remove from our tracking
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

            # SUCCESS: Add to database and update counters
            self.completed_count += 1
            await database.add_file_entry(self.channel_chat_id, file_item['message_id'], file_item['file_info'])
            
            # Start next downloads immediately
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"[CLEECH] Error handling task completion: {e}", exc_info=True)

    # Task failure handler
    async def _handle_our_task_failure(self, link, error):
        """Handle failure of our tracked task - called by TaskListener callback"""
        try:
            # Clean up tracking
            self.our_active_links.discard(link)
            file_item = self.link_to_file_mapping.pop(link, None)
            
            if file_item:
                # Clean up queue tracking
                sanitized_name = self._generate_clean_filename(file_item['file_info'])
                self.pending_sanitized_names.discard(sanitized_name)
                file_unique_id = file_item['file_info'].get('file_unique_id')
                if file_unique_id:
                    self.pending_file_ids.discard(file_unique_id)
            
            # Update counters
            self.failed_count += 1
            
            # Start next downloads immediately
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
                
            await self._save_progress()
            
        except Exception as e:
            LOGGER.error(f"[CLEECH] Error handling task failure: {e}", exc_info=True)

    async def _safe_edit_message(self, message, text):
        """Safely edit message with FloodWait handling"""
        try:
            await edit_message(message, text)
        except FloodWait as e:
            LOGGER.warning(f"[cleech] FloodWait for {e.value} seconds on message edit")
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

        # Enhanced filter parsing
        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.filter_mode = args.get('filter_mode', 'and')
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.scan_type = args.get('type')
        
        # Validate filter usage
        if not self.filter_tags:
            self.filter_mode = 'and'  # Default if no filters
        
        # Message range support
        self.from_msg_id = args.get('from_msg_id')
        self.to_msg_id = args.get('to_msg_id')
        self.scan_direction = self._determine_scan_direction()

        # Register coordinator safely
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
                    await send_message(self.message, f"⏸️ Resuming scan from message ID: {self.resume_from_msg_id} (continuing to older messages).")
                else:
                    self.resume_from_msg_id = 0
                    await send_message(self.message, "🔄 Starting fresh scan from newest message.")
                
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

            # Enhanced status message with filter mode info
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
                LOGGER.error(f"[cleech] Coordination Error: {e}", exc_info=True)
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
            # Always unregister coordinator
            self._unregister_coordinator()

    async def _coordinate_simple_leech(self):
        scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
        batch_size = 100
        batch_sleep = 5
        processed_messages = 0
        skipped_duplicates = 0
        completion_task = None
        
        # Throttling variables
        last_status_update = 0
        status_update_interval = 10
        
        # Enhanced debugging and timeout protection
        scan_timeout = 300
        self.scan_start_time = time.time()
        self.iteration_timeout = 60
        self.stuck_recovery_attempts = 0
        self.max_recovery_attempts = 3
        self.last_iteration_time = time.time()
        
        # Rate limit protection
        api_request_count = 0
        files_processed_count = 0
        last_rate_reset = time.time()
        max_api_requests_per_30s = 25
        max_files_per_30s = 500
        
        # Track skipped files for status
        skipped_filter_mismatch = 0
        skipped_existing_files = 0
        skipped_duplicates_in_queue = 0

        if self.resume_mode:
            await self._restore_resume_state(scanner)
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
        
        # Start callback-based completion monitoring
        if self.our_active_links or self.pending_files:
            completion_task = asyncio.create_task(self._wait_for_completion_callback_mode())
            LOGGER.info(f"[cleech-debug] Started completion monitoring task: Active={len(self.our_active_links)}, Pending={len(self.pending_files)}")

        # Get total counts for progress display
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

        try:
            # Enhanced offset calculation based on range
            offset_id = self._get_scan_offset_and_limit()
            
            if self.resume_mode:
                if self.resume_from_msg_id > 0:
                    await self._safe_edit_message(self.status_message, 
                        f"**📋 Resuming scan from message {offset_id}...**")
                else:
                    await self._safe_edit_message(self.status_message, 
                        f"**🔄 Starting fresh scan from newest message...**")
            else:
                range_desc = self._get_range_description()
                await self._safe_edit_message(self.status_message, 
                    f"**🚀 Starting scan{range_desc}...**")
            
            total_media_files = sum(scan_totals.values())
            scanned_media_count = 0
            current_batch = []
            loop_iteration = 0
            
            # Standard message iterator (newest to oldest is default)
            message_iterator = user.get_chat_history(
                chat_id=self.channel_chat_id,
                offset_id=offset_id
            )
            
            async for message in message_iterator:
                loop_iteration += 1
                current_time = time.time()
                self.last_message_time = current_time
                self.last_iteration_time = current_time
                
                # Check if message is within our range
                if not self._should_process_message(message):
                    continue
                
                # Stop if we've reached the end message ID for newest_to_oldest
                if self.to_msg_id and self.scan_direction in ['newest_to_oldest', 'to_specified'] and message.id <= self.to_msg_id:
                    LOGGER.info(f"[cleech] Reached end message ID {self.to_msg_id}, stopping scan")
                    break
                
                if loop_iteration % 2000 == 0:
                    elapsed = current_time - self.scan_start_time
                    rate = loop_iteration / elapsed if elapsed > 0 else 0
                    LOGGER.info(f"[cleech] Milestone: {loop_iteration} messages scanned in {elapsed:.1f}s (rate: {rate:.1f} msg/s) | Current ID: {message.id}")
                
                # Check for iteration timeout
                iteration_idle = current_time - self.last_iteration_time
                if iteration_idle > self.iteration_timeout:
                    LOGGER.error(f"[cleech] ITERATION TIMEOUT at message {loop_iteration}")
                    
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
                
                # IMMEDIATE FILTER: Only process media messages
                if not (message.document or message.video):
                    continue
                
                # Respect resumed scan type
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

                if completion_task is None and (self.our_active_links or self.pending_files):
                    completion_task = asyncio.create_task(self._wait_for_completion_callback_mode())
                    LOGGER.info(f"[cleech-debug] Started completion monitoring during scan: Active={len(self.our_active_links)}, Pending={len(self.pending_files)}")

                # Enhanced status updates with skipped file details
                if current_time - last_status_update >= status_update_interval:
                    progress_percent = (scanned_media_count / total_media_files * 100) if total_media_files > 0 else 0
                    scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "Media files"
                    
                    elapsed_total = current_time - self.scan_start_time
                    overall_rate = scanned_media_count / elapsed_total if elapsed_total > 0 else 0
                    
                    total_skipped = skipped_filter_mismatch + skipped_existing_files + skipped_duplicates_in_queue
                    
                    await self._safe_edit_message(self.status_message, 
                        f"**Scanning {scan_type_text}... ({scanned_media_count}/{total_media_files} - {progress_percent:.1f}%)**\n\n"
                        f"**Current Msg ID:** {message.id} | **Rate:** {overall_rate:.1f} files/s\n"
                        f"**Skipped:** {total_skipped} (**Filter:** {skipped_filter_mismatch} | **Existing:** {skipped_existing_files} | **Queued:** {skipped_duplicates_in_queue})\n"
                        f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                        f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
                    )
                    last_status_update = current_time

                if len(current_batch) >= batch_size:
                    current_time = time.time()
                    
                    # Rate limiting check
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
            
            scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "All media"
            await self._safe_edit_message(self.status_message, 
                f"**✅ {scan_type_text} scan completed! ({scanned_media_count}/{total_media_files})**\n\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
            )

            # 🔧 CRITICAL FIX: Always ensure all downloads complete - Don't rely on completion_task
            LOGGER.info(f"[cleech-debug] Scan finished. Starting post-scan completion monitoring: Active={len(self.our_active_links)}, Pending={len(self.pending_files)}")
            
            # Cancel any existing completion task since we're handling it manually now
            if completion_task and not completion_task.done():
                LOGGER.info(f"[cleech-debug] Cancelling existing completion task")
                completion_task.cancel()
                try:
                    await completion_task
                except asyncio.CancelledError:
                    pass

            # FIXED: Manual completion monitoring - ensure ALL downloads finish
            post_scan_iterations = 0
            while (self.our_active_links or self.pending_files) and not self.is_cancelled:
                post_scan_iterations += 1
                
                # Start any remaining downloads
                downloads_started = 0
                while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                    await self._start_next_download()
                    downloads_started += 1
                
                # Debug logging every 10 iterations (100 seconds)
                if post_scan_iterations % 10 == 0 or downloads_started > 0:
                    LOGGER.info(f"[cleech-debug] Post-scan iteration {post_scan_iterations}: Started {downloads_started} downloads. Active: {len(self.our_active_links)}, Pending: {len(self.pending_files)}, Completed: {self.completed_count}")
                
                # Update status periodically during post-scan downloads
                if post_scan_iterations % 3 == 0:  # Every 30 seconds
                    await self._safe_edit_message(self.status_message,
                        f"**✅ Scan completed! Processing remaining downloads...**\n\n"
                        f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                        f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}\n"
                        f"**Processing:** Iteration {post_scan_iterations}"
                    )
                
                # Keep monitoring via TaskListener callbacks
                await asyncio.sleep(10)
            
            LOGGER.info(f"[cleech-debug] Post-scan completion finished after {post_scan_iterations} iterations. Active={len(self.our_active_links)}, Pending={len(self.pending_files)}")
            
            await self._show_final_results(processed_messages, skipped_duplicates)

        except TimeoutError as e:
            LOGGER.error(f"[cleech] Timeout error: {e}")
            await self._safe_edit_message(self.status_message, f"❌ **Scan timeout: {str(e)}**")
            await self._save_progress(interrupted=True)
            
            # Still try to complete remaining downloads on timeout
            if self.our_active_links or self.pending_files:
                LOGGER.info(f"[cleech-debug] Timeout occurred but continuing with remaining downloads: Active={len(self.our_active_links)}, Pending={len(self.pending_files)}")
                await self._wait_for_completion_callback_mode()
                
        except Exception as e:
            LOGGER.error(f"[cleech] Processing error: {e}", exc_info=True)
            await self._save_progress(interrupted=True)
            raise
        finally:
            # Final cleanup
            if completion_task and not completion_task.done():
                completion_task.cancel()
                try:
                    await completion_task
                except asyncio.CancelledError:
                    pass

    # Callback-based completion waiting (no polling!)
    async def _wait_for_completion_callback_mode(self):
        """Wait for completion using TaskListener callbacks - no database polling!"""
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            # Start new downloads if slots available
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            
            # TaskListener callbacks handle completion, we just keep alive
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
                    LOGGER.warning(f"[cleech] Could not restore pending file {msg_id}: {e}")
            
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    if row["_id"].startswith("https://t.me/c/"):
                        self.our_active_links.add(row["_id"])
        except Exception as e:
            LOGGER.error(f"[cleech] Error restoring resume state: {e}", exc_info=True)

    async def _process_batch_with_skip_tracking(self, message_batch, scanner, processed_so_far):
        skip_counts = {'filter': 0, 'existing': 0, 'queued': 0}
        
        for message in message_batch:
            if self.is_cancelled:
                break
                
            try:
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                # ENHANCED: Check filter match with AND/OR logic
                if not self._check_filter_match(file_info['search_text']):
                    skip_counts['filter'] += 1
                    continue

                # Generate sanitized filename (PRIMARY duplicate check method)
                sanitized_name = self._generate_clean_filename(file_info)
                file_unique_id = file_info.get('file_unique_id')
                
                # PRIORITY 1: Check database by sanitized filename
                if await database.check_file_exists(file_info=file_info):
                    skip_counts['existing'] += 1
                    continue
                    
                # PRIORITY 2: Check if already in processing queue (sanitized name first)
                if sanitized_name in self.pending_sanitized_names:
                    skip_counts['queued'] += 1
                    continue
                    
                # PRIORITY 3: Check by file_unique_id in queue (secondary)
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
                
                # PRIMARY: Track sanitized name in queue
                self.pending_sanitized_names.add(sanitized_name)
                # SECONDARY: Track file_unique_id in queue
                if file_unique_id:
                    self.pending_file_ids.add(file_unique_id)
                    
            except Exception as e:
                LOGGER.error(f"[cleech] Error processing message {message.id}: {e}")

        # Start downloads for new files
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
            
        return skip_counts

    async def _start_next_download(self):
        if not self.pending_files: 
            return
        file_item = self.pending_files.pop(0)
        try:
            COMMAND_CHANNEL_ID = int(config_dict.get('LEECH_DUMP_CHAT') or self.message.chat.id)
            clean_name = self._generate_clean_filename(file_item['file_info'])
            leech_cmd = f'/leech {file_item["url"]} -n {clean_name}'
            
            command_message = await user.send_message(chat_id=COMMAND_CHANNEL_ID, text=leech_cmd)
            actual_stored_url = f"https://t.me/c/{str(COMMAND_CHANNEL_ID)[4:]}/{command_message.id}"
            
            await asyncio.sleep(2)
            self.our_active_links.add(actual_stored_url)
            self.link_to_file_mapping[actual_stored_url] = file_item
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error starting download for {file_item.get('filename')}: {e}")
            self.pending_files.insert(0, file_item)
            
            # Clean up tracking sets on failure to prevent permanent blocking
            sanitized_name = self._generate_clean_filename(file_item['file_info'])
            self.pending_sanitized_names.discard(sanitized_name)
            file_unique_id = file_item['file_info'].get('file_unique_id')
            if file_unique_id:
                self.pending_file_ids.discard(file_unique_id)

    async def _save_progress(self, interrupted=False):
        try:
            progress = {
                "user_id": self.message.from_user.id,
                "channel_id": self.channel_id,
                "filter_tags": self.filter_tags,
                "scan_type": self.scan_type,
                "scanned_message_ids": list(self.scanned_message_ids),
                "pending_files": [f['message_id'] for f in self.pending_files] + [item['message_id'] for item in self.link_to_file_mapping.values()],
                "timestamp": datetime.utcnow().isoformat(),
                "interrupted": interrupted,
                "completed_scan_type": self.completed_scan_type
            }
            await database.save_leech_progress(self.message.from_user.id, self.channel_id, progress)
        except Exception as e:
            LOGGER.error(f"[cleech] Error saving progress: {e}")

    async def _show_final_results(self, processed_messages, skipped_duplicates):
        total_attempted = self.completed_count + self.failed_count
        success_rate = (self.completed_count / total_attempted * 100) if total_attempted > 0 else 0
        text = (
            f"**✅ Channel Leech Completed!**\n\n"
            f"**Scanned:** {processed_messages} items\n"
            f"**Skipped:** {skipped_duplicates} existing duplicates\n"
            f"**Downloaded:** {self.completed_count} | **Failed:** {self.failed_count}\n"
            f"**Success Rate:** {success_rate:.1f}%"
        )
        await self._safe_edit_message(self.status_message, text)

    def _generate_clean_filename(self, file_info):
        original_filename = file_info.get('file_name', '')
        base_name = original_filename
        if self.use_caption_as_filename and file_info.get('caption_first_line'):
            base_name = file_info['caption_first_line'].strip()
        
        clean_base = sanitize_filename(base_name)
        
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
                parsed['channel'] = args[i+1]; i+=2
            elif args[i] == '-f':
                text = args[i+1]
                parsed['filter'] = text[1:-1].split() if text.startswith('"') else text.split()
                parsed['filter_mode'] = 'and'  # Default AND mode for -f
                i+=2
            elif args[i] == '-feither':
                text = args[i+1]
                parsed['filter'] = text[1:-1].split() if text.startswith('"') else text.split()
                parsed['filter_mode'] = 'or'   # OR mode for -feither
                i+=2
            elif args[i] == '--no-caption':
                parsed['no_caption'] = True; i+=1
            elif args[i] == '-type':
                if i + 1 < len(args) and args[i+1].lower() in ['document', 'media']:
                    parsed['type'] = args[i+1].lower()
                i+=2
            elif args[i] == '-from':
                if i + 1 < len(args) and args[i+1].isdigit():
                    parsed['from_msg_id'] = int(args[i+1])
                i+=2
            elif args[i] == '-to':
                if i + 1 < len(args) and args[i+1].isdigit():
                    parsed['to_msg_id'] = int(args[i+1])
                i+=2
            else:
                i+=1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True
        LOGGER.info(f"Cancelling Channel Leech for {self.channel_id}")
        asyncio.create_task(self._save_progress(interrupted=True))
        # Clean up coordinator registration
        self._unregister_coordinator()

    # Add destructor for extra safety
    def __del__(self):
        """Ensure coordinator is unregistered on garbage collection"""
        try:
            if hasattr(self, '_is_active') and self._is_active:
                # This won't work in async context, but provides safety net
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
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
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
