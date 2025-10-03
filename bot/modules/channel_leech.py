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

class DestinationWatcher:
    """
    Monitors the leech destination for new files using USER session
    for more reliable detection of bot's own uploads.
    """
    def __init__(self, client_session, destination_id, start_time):
        self.client = client_session
        self.destination_id = destination_id
        self.start_time = start_time
        self.verified_files = set()
        filters = (document | video) & chat(self.destination_id)
        self.handler = MessageHandler(self._on_new_message, filters=filters)
        self._is_active = False

    async def _on_new_message(self, client, message: Message):
        if message.date < self.start_time:
            return
        sanitized_name = None
        if message.caption:
            caption_first_line = message.caption.split('\n')[0].strip()
            sanitized_name = sanitize_filename(caption_first_line)
        if not sanitized_name:
            file_name = getattr(message.document or message.video, 'file_name', None)
            if file_name:
                sanitized_name = sanitize_filename(file_name)
        if sanitized_name:
            LOGGER.info(f"[Watcher] Detected new file in destination: {sanitized_name}")
            self.verified_files.add(sanitized_name)

    def start(self):
        if not self._is_active:
            self.client.add_handler(self.handler)
            self._is_active = True
            session_name = "BOT" if hasattr(self.client, 'me') and self.client.me.is_bot else "USER"
            LOGGER.info(f"[Watcher] Started listening for media in destination chat {self.destination_id} using {session_name} session.")

    def stop(self):
        if self._is_active:
            self.client.remove_handler(self.handler)
            self._is_active = False
            LOGGER.info(f"[Watcher] Stopped listening in destination chat: {self.destination_id}")

    def is_verified(self, sanitized_name):
        return sanitized_name in self.verified_files

class SimpleChannelLeechCoordinator(TaskListener):
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None
        self.filter_tags = []
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
        # REMOVED: self.last_success_msg_id - no longer needed
        self.start_time = datetime.now()
        self.watcher = None
        # Debugging variables
        self.last_message_time = time.time()
        self.last_batch_time = time.time()
        self.scan_start_time = time.time()
        self.messages_processed_this_minute = 0
        self.last_minute_check = time.time()
        super().__init__()

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
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption] [-type document|media]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch @docs_channel -type document`"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.scan_type = args.get('type')

        if not await self._initialize_watcher():
            return

        progress = await database.get_leech_progress(self.message.from_user.id, self.channel_id)
        if progress:
            self.resume_mode = True
            self.scanned_message_ids = set(progress.get("scanned_message_ids", []))
            self.completed_scan_type = progress.get("completed_scan_type")
            
            if not self.scan_type and progress.get("scan_type"):
                self.scan_type = progress.get("scan_type")
                LOGGER.info(f"[cleech] Restored original scan type: {self.scan_type}")
            
            # FIXED: Use the LOWEST (oldest) scanned message ID as resume point
            scanned_ids = self.scanned_message_ids
            
            if scanned_ids:
                # Resume from the oldest (lowest ID) scanned message
                self.resume_from_msg_id = min(scanned_ids)
                LOGGER.info(f"[cleech] Resuming from oldest scanned message ID: {self.resume_from_msg_id}")
                await send_message(self.message, f"⏸️ Resuming scan from message ID: {self.resume_from_msg_id} (continuing to older messages).")
            else:
                self.resume_from_msg_id = 0
                LOGGER.info(f"[cleech] No previous scanned messages, starting fresh")
                await send_message(self.message, "🔄 Starting fresh scan from newest message.")
            
            if self.completed_scan_type:
                await send_message(self.message, f"✅ Scan for `{self.completed_scan_type}` already completed. Resuming next scan type.")

        else:
            self.scanned_message_ids = set()
            self.resume_from_msg_id = 0
            LOGGER.info(f"[cleech] No previous progress found, starting fresh scan")

        try:
            chat = await user.get_chat(self.channel_id)
            self.channel_chat_id = chat.id
        except Exception as e:
            await send_message(self.message, f"Could not resolve channel: {e}")
            if self.watcher:
                self.watcher.stop()
            return

        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "simple_channel_leech"
        )

        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        scan_type_text = f" of type `{self.scan_type}`" if self.scan_type else " of all media types"
        self.status_message = await send_message(
            self.message,
            f"**Channel Leech Starting{' (Resumed)' if self.resume_mode else ''}...**\n"
            f"**Channel:** `{self.channel_id}`\n"
            f"**Scanning:**{scan_type_text}\n"
            f"**Filter:**{filter_text}"
        )

        try:
            await self._coordinate_simple_leech()
        except Exception as e:
            LOGGER.error(f"[cleech] Coordination Error: {e}", exc_info=True)
            await self._safe_edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)
            if self.watcher:
                self.watcher.stop()
            if not self.is_cancelled:
                await database.clear_leech_progress(self.message.from_user.id, self.channel_id)

    async def _initialize_watcher(self):
        destination_id = await self._get_leech_destination()
        if not destination_id:
            await send_message(self.message, "❌ Could not determine leech destination.")
            return False
        try:
            # FIXED: Use USER session for watcher instead of BOT session
            # This ensures we can see the bot's own uploads
            await user.get_chat_member(destination_id, user.me.id)
            self.watcher = DestinationWatcher(user, destination_id, self.start_time)  # Changed from bot to user
            self.watcher.start()
            return True
        except UserNotParticipant:
            await send_message(self.message, f"❌ User session is not a member of the destination channel ({destination_id}).")
            return False
        except Exception as e:
            await send_message(self.message, f"❌ Error setting up watcher: {e}")
            return False

    async def _get_leech_destination(self):
        try:
            if user:
                data = user_data.get(user.me.id, {})
                if dest := data.get('leech_dest'):
                    return int(dest)
            return int(config_dict['LEECH_DUMP_CHAT'])
        except:
            return None

    async def _coordinate_simple_leech(self):
        scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
        batch_size = 100  # Reasonable batch size for processing
        batch_sleep = 5   # Delay between batches
        processed_messages = 0
        skipped_duplicates = 0
        completion_task = None
        
        # Throttling variables
        last_status_update = 0
        status_update_interval = 10
        
        # Enhanced debugging and timeout protection
        scan_timeout = 300  # 5 minutes timeout for batch processing
        self.scan_start_time = time.time()
        self.iteration_timeout = 60  # 1 minute timeout per iteration
        self.stuck_recovery_attempts = 0
        self.max_recovery_attempts = 3
        self.last_iteration_time = time.time()
        
        # CORRECTED: Dual rate limit protection
        api_request_count = 0  # Track actual API requests (max 30 per 30s)
        files_processed_count = 0  # Track processed files (max 300 per 30s) 
        last_rate_reset = time.time()
        max_api_requests_per_30s = 25  # Stay under Telegram's hard limit of 30
        max_files_per_30s = 500  # Your setting to avoid overloading user session

        if self.resume_mode:
            await self._restore_resume_state(scanner)
            LOGGER.info(f"[cleech] Restored {len(self.pending_files)} pending, {len(self.our_active_links)} active downloads.")
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
        
        if self.our_active_links or self.pending_files:
            LOGGER.info(f"[cleech] Starting completion check task immediately.")
            completion_task = asyncio.create_task(self._wait_for_completion())

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

        LOGGER.info(f"[cleech] Getting total counts for scan types: {[s['name'] for s in scan_types]}")
        for scan in scan_types:
            try:
                total_count = await user.search_messages_count(
                    chat_id=self.channel_chat_id,
                    filter=scan['filter']
                )
                scan_totals[scan['name']] = total_count
                LOGGER.info(f"[cleech] Found {total_count} {scan['name']} messages in channel")
            except Exception as e:
                LOGGER.warning(f"[cleech] Could not get {scan['name']} count: {e}")
                scan_totals[scan['name']] = 0

        try:
            # Proper resume logic using lowest (oldest) scanned message ID
            offset_id = self.resume_from_msg_id if self.resume_from_msg_id > 0 else 0
            
            if self.resume_mode:
                if self.resume_from_msg_id > 0:
                    LOGGER.info(f"[cleech] RESUMING: Starting from message ID {offset_id} and scanning towards older messages")
                    await self._safe_edit_message(self.status_message, 
                        f"**📋 Resuming scan from message {offset_id}...**")
                else:
                    LOGGER.info(f"[cleech] FRESH START: No previous progress, starting from newest message")
                    await self._safe_edit_message(self.status_message, 
                        f"**🔄 Starting fresh scan from newest message...**")
            else:
                LOGGER.info(f"[cleech] NEW SCAN: Starting from newest message")
                await self._safe_edit_message(self.status_message, 
                    f"**🚀 Starting new scan from newest message...**")
            
            total_media_files = sum(scan_totals.values())
            scanned_media_count = 0
            current_batch = []
            loop_iteration = 0
            
            LOGGER.info(f"[cleech] Beginning get_chat_history iteration with offset_id: {offset_id}")
            LOGGER.info(f"[cleech] Rate limiting: API {max_api_requests_per_30s}/30s, Files {max_files_per_30s}/30s, batch size: {batch_size}")
            
            # Timeout protected iteration
            message_iterator = user.get_chat_history(
                chat_id=self.channel_chat_id,
                offset_id=offset_id
            )
            
            async for message in message_iterator:
                loop_iteration += 1
                current_time = time.time()
                self.last_message_time = current_time
                self.last_iteration_time = current_time
                
                # Enhanced debugging with timeout detection
                if loop_iteration % 500 == 0:
                    elapsed = current_time - self.scan_start_time
                    rate = loop_iteration / elapsed if elapsed > 0 else 0
                    LOGGER.info(f"[cleech] Processed {loop_iteration} messages in {elapsed:.1f}s (rate: {rate:.1f} msg/s)")
                
                # Check for iteration timeout
                iteration_idle = current_time - self.last_iteration_time
                if iteration_idle > self.iteration_timeout:
                    LOGGER.error(f"[cleech] ITERATION TIMEOUT: No message received in {iteration_idle:.1f}s at iteration {loop_iteration}")
                    await self._safe_edit_message(self.status_message, 
                        f"❌ **Scan stalled at {scanned_media_count}/{total_media_files} - Attempting recovery...**")
                    
                    # Attempt recovery
                    if self.stuck_recovery_attempts < self.max_recovery_attempts:
                        self.stuck_recovery_attempts += 1
                        LOGGER.warning(f"[cleech] Recovery attempt {self.stuck_recovery_attempts}/{self.max_recovery_attempts}")
                        await asyncio.sleep(10)
                        self.last_iteration_time = time.time()
                        continue
                    else:
                        LOGGER.error(f"[cleech] Max recovery attempts exceeded, saving progress and stopping scan")
                        await self._save_progress(interrupted=True)
                        raise TimeoutError(f"Scan stalled at iteration {loop_iteration} after {self.max_recovery_attempts} recovery attempts")
                
                # Check for batch timeout
                if current_time - self.last_batch_time > scan_timeout:
                    LOGGER.warning(f"[cleech] BATCH TIMEOUT: No batch processed in {scan_timeout}s")
                    await self._safe_edit_message(self.status_message, 
                        f"⚠️ **Potential batch hang at message {loop_iteration}**")
                
                if self.is_cancelled:
                    LOGGER.info(f"[cleech] Scan cancelled at iteration {loop_iteration}")
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
                
                # Reset recovery attempts on successful progress
                self.stuck_recovery_attempts = 0
                
                # Track processing rate
                if current_time - self.last_minute_check >= 60:
                    LOGGER.info(f"[cleech] Processed {self.messages_processed_this_minute} media messages in last minute")
                    self.messages_processed_this_minute = 0
                    self.last_minute_check = current_time
                self.messages_processed_this_minute += 1

                if completion_task is None and (self.our_active_links or self.pending_files):
                    LOGGER.info(f"[cleech] Starting completion check task during scan.")
                    completion_task = asyncio.create_task(self._wait_for_completion())

                # Throttled status updates with dual rate limit info
                if current_time - last_status_update >= status_update_interval:
                    progress_percent = (scanned_media_count / total_media_files * 100) if total_media_files > 0 else 0
                    scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "Media files"
                    
                    # Show rate and timeout status
                    elapsed_total = current_time - self.scan_start_time
                    overall_rate = scanned_media_count / elapsed_total if elapsed_total > 0 else 0
                    rate_status = f"API: {api_request_count}/{max_api_requests_per_30s} | Files: {files_processed_count}/{max_files_per_30s}"
                    
                    await self._safe_edit_message(self.status_message, 
                        f"**Scanning {scan_type_text}... ({scanned_media_count}/{total_media_files} - {progress_percent:.1f}%)**\n\n"
                        f"**Current Msg ID:** {message.id} | **Rate:** {overall_rate:.1f} files/s\n"
                        f"**{rate_status}** | **Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                        f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
                    )
                    last_status_update = current_time

                if len(current_batch) >= batch_size:
                    current_time = time.time()
                    
                    # CORRECTED: Dual rate limiting check
                    if current_time - last_rate_reset >= 30:
                        # Reset both counters every 30 seconds
                        api_request_count = 0
                        files_processed_count = 0
                        last_rate_reset = current_time
                        LOGGER.info(f"[cleech] Rate limit window reset - API: {api_request_count}, Files: {files_processed_count}, Messages: {loop_iteration}")
                    
                    # Check both limits
                    api_limit_hit = api_request_count >= max_api_requests_per_30s
                    files_limit_hit = files_processed_count >= max_files_per_30s
                    
                    if api_limit_hit or files_limit_hit:
                        wait_time = 30 - (current_time - last_rate_reset)
                        if wait_time > 0:
                            limit_type = "API requests" if api_limit_hit else "file processing"
                            LOGGER.warning(f"[cleech] Rate limit protection ({limit_type}): waiting {wait_time:.1f}s")
                            await self._safe_edit_message(self.status_message, 
                                f"⏳ **Rate limit protection ({limit_type}) - waiting {wait_time:.0f}s**\n\n"
                                f"**Processed:** {scanned_media_count}/{total_media_files}\n"
                                f"**API:** {api_request_count}/{max_api_requests_per_30s} | **Files:** {files_processed_count}/{max_files_per_30s}\n"
                                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}"
                            )
                            await asyncio.sleep(wait_time + 1)
                            api_request_count = 0
                            files_processed_count = 0
                            last_rate_reset = time.time()
                    
                    # Increment counters
                    api_request_count += 1  # This batch processing counts as 1 API request
                    files_processed_count += len(current_batch)  # Count actual files processed
                    
                    LOGGER.info(f"[cleech] Processing batch - API: {api_request_count}/{max_api_requests_per_30s}, Files: {files_processed_count}/{max_files_per_30s} (msg ID: {message.id})")
                    self.last_batch_time = current_time
                    batch_skipped = await self._process_batch(current_batch, scanner, processed_messages)
                    skipped_duplicates += batch_skipped
                    current_batch = []
                    
                    # ENHANCED: Adaptive sleep based on rate limit status
                    sleep_time = batch_sleep
                    if api_request_count > max_api_requests_per_30s * 0.8 or files_processed_count > max_files_per_30s * 0.8:
                        sleep_time = batch_sleep * 2
                        LOGGER.info(f"[cleech] Near rate limit - increased sleep to {sleep_time}s")
                    
                    await asyncio.sleep(sleep_time)
                    await self._save_progress()
                    LOGGER.info(f"[cleech] Batch processed, continuing scan...")

            LOGGER.info(f"[cleech] Completed get_chat_history iteration. Total iterations: {loop_iteration}")
            
            # Process remaining batch
            if current_batch and not self.is_cancelled:
                LOGGER.info(f"[cleech] Processing final batch of {len(current_batch)} messages")
                batch_skipped = await self._process_batch(current_batch, scanner, processed_messages)
                skipped_duplicates += batch_skipped
                await self._save_progress()
            
            # Mark scan as completed
            self.completed_scan_type = "all" if not self.scan_type else self.scan_type
            await self._save_progress()
            
            scan_type_text = f"{self.scan_type.title()}s" if self.scan_type else "All media"
            await self._safe_edit_message(self.status_message, 
                f"**✅ {scan_type_text} scan completed! ({scanned_media_count}/{total_media_files})**\n\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
            )

            # Final completion
            if completion_task:
                LOGGER.info(f"[cleech] Waiting for completion task to finish")
                await completion_task
            elif self.our_active_links or self.pending_files:
                LOGGER.info(f"[cleech] Starting final wait for completion")
                await self._wait_for_completion()
                
            await self._show_final_results(processed_messages, skipped_duplicates)

        except TimeoutError as e:
            LOGGER.error(f"[cleech] Timeout error: {e}")
            await self._safe_edit_message(self.status_message, f"❌ **Scan timeout: {str(e)}**")
            await self._save_progress(interrupted=True)
            # Don't re-raise, let it complete normally with current progress
        except Exception as e:
            LOGGER.error(f"[cleech] Processing error at iteration {getattr(locals(), 'loop_iteration', 0)}: {e}", exc_info=True)
            await self._save_progress(interrupted=True)
            raise

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

    async def _process_batch(self, message_batch, scanner, processed_so_far):
        batch_skipped = 0
        for message in message_batch:
            if self.is_cancelled:
                break
            file_info = await scanner._extract_file_info(message)
            if not file_info: 
                continue
            
            if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                continue

            file_unique_id = file_info.get('file_unique_id')
            sanitized_name = self._generate_clean_filename(file_info)
            
            if await database.check_file_exists(file_unique_id=file_unique_id):
                batch_skipped += 1
                continue
                
            if sanitized_name in self.pending_sanitized_names or (file_unique_id and file_unique_id in self.pending_file_ids):
                continue
                
            message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{message.id}"
            self.pending_files.append({
                'url': message_link,
                'filename': file_info['file_name'],
                'message_id': message.id,
                'file_info': file_info,
            })
            self.pending_sanitized_names.add(sanitized_name)
            if file_unique_id:
                self.pending_file_ids.add(file_unique_id)

        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        return batch_skipped

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
            LOGGER.error(f"[cleech] Error starting download for {file_item.get('filename')}: {e}", exc_info=True)
            self.pending_files.insert(0, file_item)

    async def _wait_for_completion(self):
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            await self._check_and_verify_tasks()
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            await asyncio.sleep(self.check_interval)

    async def _check_and_verify_tasks(self):
        try:
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            active_db_tasks = set()
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    active_db_tasks.add(row["_id"])
            
            potentially_completed_links = self.our_active_links - active_db_tasks
            if not potentially_completed_links: 
                return

            LOGGER.info(f"[cleech] Verifying {len(potentially_completed_links)} potentially completed downloads")
            verification_coros = [self._verify_upload_in_destination(link) for link in potentially_completed_links]
            results = await asyncio.gather(*verification_coros)

            for link, is_success in zip(list(potentially_completed_links), results):
                self.our_active_links.discard(link)
                file_item = self.link_to_file_mapping.pop(link, None)
                if not file_item: 
                    continue

                if is_success:
                    self.completed_count += 1
                    # REMOVED: self.last_success_msg_id tracking - no longer needed
                    await database.add_file_entry(self.channel_chat_id, file_item['message_id'], file_item['file_info'])
                    LOGGER.info(f"[cleech] Successfully completed: {file_item['filename']}")
                else:
                    self.failed_count += 1
                    LOGGER.warning(f"[cleech] Verification failed for {file_item['filename']}. Marked as failed.")
                await self._save_progress()

        except Exception as e:
            LOGGER.error(f"[cleech] CRITICAL ERROR in task completion check: {e}", exc_info=True)

    async def _verify_upload_in_destination(self, completed_link):
        file_item = self.link_to_file_mapping.get(completed_link)
        if not file_item: 
            return False
        
        sanitized_name_to_check = self._generate_clean_filename(file_item['file_info'])
        
        for _ in range(3):
            if self.watcher and self.watcher.is_verified(sanitized_name_to_check):
                LOGGER.info(f"[cleech] VERIFIED upload for: {sanitized_name_to_check}")
                return True
            await asyncio.sleep(10)

        LOGGER.warning(f"[cleech] FAILED verification for: {sanitized_name_to_check}. Not found in destination.")
        return False

    async def _save_progress(self, interrupted=False):
        try:
            progress = {
                "user_id": self.message.from_user.id,
                "channel_id": self.channel_id,
                "filter_tags": self.filter_tags,
                "scan_type": self.scan_type,
                # REMOVED: "last_success_msg_id" - no longer needed
                "scanned_message_ids": list(self.scanned_message_ids),
                "pending_files": [f['message_id'] for f in self.pending_files] + [item['message_id'] for item in self.link_to_file_mapping.values()],
                "timestamp": datetime.utcnow().isoformat(),
                "interrupted": interrupted,
                "completed_scan_type": self.completed_scan_type
            }
            await database.save_leech_progress(self.message.from_user.id, self.channel_id, progress)
            LOGGER.info(f"[cleech] Progress saved: completed={self.completed_count}, failed={self.failed_count}, pending={len(self.pending_files)}")
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
                parsed['filter'] = text[1:-1].split() if text.startswith('"') else text.split(); i+=2
            elif args[i] == '--no-caption':
                parsed['no_caption'] = True; i+=1
            elif args[i] == '-type':
                if i + 1 < len(args) and args[i+1].lower() in ['document', 'media']:
                    parsed['type'] = args[i+1].lower()
                i+=2
            else:
                i+=1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True
        LOGGER.info(f"Cancelling Channel Leech for {self.channel_id}")
        asyncio.create_task(self._save_progress(interrupted=True))

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
