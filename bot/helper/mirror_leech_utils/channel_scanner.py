import asyncio
import hashlib
import logging
from datetime import datetime
from pyrogram.errors import FloodWait, PeerIdInvalid
from pyrogram import types

from ..ext_utils.db_handler import database, sanitize_filename
from ..telegram_helper.message_utils import edit_message

LOGGER = logging.getLogger(__name__)

class ChannelScanner:
    """
    Optimized channel scanner with sanitized_name priority system:
    1. Caption first line → sanitized_name (PRIMARY)
    2. File name → sanitized_name (FALLBACK)
    3. All operations use sanitized_name for consistency
    """
    
    def __init__(self, user_client, bot_client, channel_id, batch_size=200, max_messages=0, filter_tags=None):
        self.user_client = user_client
        self.bot_client = bot_client
        self.channel_id = channel_id
        self.batch_size = batch_size
        self.max_messages = max_messages
        self.filter_tags = filter_tags or []
        self.running = True
        self.processed = 0
        self.db_entries = 0
        self.skipped_duplicates = 0
        self.status_message = None
        self.listener = None
        
        # Optimized timings for Telegram API limits
        self.batch_sleep = 0.3  
        self.api_delay = 0.05  # 50ms between API calls
        self.status_update_interval = 10
        self.last_status_update = 0         

    async def scan(self, status_msg=None):
        """Main scanning function with ID-based batching"""
        self.status_message = status_msg
        
        try:
            # STEP 1: Get channel info using user session
            try:
                chat = await self.user_client.get_chat(self.channel_id)
                await self._update_status(f"📋 Scanning channel: **{chat.title}**")
                LOGGER.info(f"Starting scan for channel: {chat.title} ({self.channel_id})")
            except PeerIdInvalid:
                error_msg = (
                    f"❌ **Access Denied to {self.channel_id}**\n\n"
                    f"**Reason:** User session is not a member of this channel\n\n"
                    f"**Solutions:**\n"
                    f"• Join the channel with your user account\n"
                    f"• For private channels: Get invited first\n"
                    f"• Verify the channel ID is correct"
                )
                await self._update_status(error_msg)
                return
            except Exception as e:
                LOGGER.error(f"Error accessing channel: {e}")
                raise
            
            # STEP 2: Get total message count and latest message ID
            total_messages = await self.user_client.get_chat_history_count(self.channel_id)
            
            # Get latest message ID (starting point)
            latest_msg = None
            async for msg in self.user_client.get_chat_history(self.channel_id, limit=1):
                latest_msg = msg
                break
            
            if not latest_msg:
                await self._update_status("❌ **No messages found in channel**")
                return
            
            start_id = latest_msg.id
            LOGGER.info(f"Channel has {total_messages} messages, starting from ID {start_id}")
            
            # STEP 3: Process in batches using bot session
            await self._process_in_batches(start_id, total_messages)
            
            # Final status
            if not (hasattr(self, 'listener') and self.listener and self.listener.is_cancelled):
                summary = (
                    f'✅ **Scan complete!**\n\n'
                    f'📊 Processed: {self.processed} messages\n'
                    f'✨ New files: {self.db_entries}\n'
                    f'🔄 Duplicates skipped: {self.skipped_duplicates}'
                )
                await self._update_status(summary)
                LOGGER.info(f"Scan complete: {self.db_entries} new files, {self.skipped_duplicates} duplicates")

        except FloodWait as e:
            LOGGER.warning(f'FloodWait: waiting {e.x}s')
            await self._update_status(f'⏳ Rate limited, waiting {e.x}s...')
            await asyncio.sleep(e.x + 1)
            await self.scan(status_msg)  # Resume
            
        except Exception as e:
            LOGGER.error(f"Scanning error: {e}", exc_info=True)
            await self._update_status(f'❌ Scanning error: {str(e)}')

    async def _process_in_batches(self, start_id, total_messages):
        """Process messages in batches using bot session's get_messages()"""
        current_id = start_id
        batch_num = 0
        
        while current_id > 0:
            # Check cancellation
            if not self.running or (hasattr(self, 'listener') and self.listener and self.listener.is_cancelled):
                LOGGER.info("Scan cancelled by user")
                break
            
            # Generate batch of message IDs
            message_ids = list(range(max(1, current_id - self.batch_size + 1), current_id + 1))
            message_ids.reverse()
            
            batch_num += 1
            LOGGER.info(f"Batch {batch_num}: Fetching messages {message_ids[-1]}-{message_ids[0]}")
            
            try:
                messages = await self.bot_client.get_messages(
                    self.channel_id,
                    message_ids=message_ids
                )
                
                valid_messages = [msg for msg in messages if msg and not isinstance(msg, int)]
                
                # Process batch
                await self._process_batch(valid_messages)
                
                self.processed += len(valid_messages)
                
                # UPDATE STATUS WITH 10-SECOND THROTTLING
                current_time = asyncio.get_event_loop().time()
                if current_time - self.last_status_update >= self.status_update_interval:
                    progress_pct = (self.processed / total_messages * 100) if total_messages > 0 else 0
                    await self._update_status(
                        f'🔍 Batch {batch_num} | {self.processed}/{total_messages} ({progress_pct:.1f}%)\n'
                        f'✨ New: {self.db_entries} | 🔄 Skipped: {self.skipped_duplicates}'
                    )
                    self.last_status_update = current_time  # Update timestamp
                    LOGGER.debug(f"Status updated at batch {batch_num}")
                
                # Sleep between batches
                await asyncio.sleep(self.batch_sleep)
                
            except FloodWait as e:
                LOGGER.warning(f'FloodWait in batch: {e.x}s')
                await asyncio.sleep(e.x + 1)
                continue
            
            except Exception as e:
                LOGGER.error(f"Error fetching batch: {e}")
            
            current_id -= self.batch_size
            
            if self.max_messages > 0 and self.processed >= self.max_messages:
                break

    async def _process_batch(self, messages):
        """
        Process a batch of messages in parallel
        All operations use sanitized_name for consistency
        """
        tasks = []
        
        for message in messages:
            if not message:
                continue
            
            # Extract file info with sanitized_name
            file_info = self._extract_file_info_sync(message)
            if file_info:
                # Create async task for each file
                tasks.append(self._process_single_file(message, file_info))
        
        # Process all files in batch concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_file(self, message, file_info):
        """
        Process individual file with sanitized_name-based duplicate check
        """
        try:
            # Apply filter tags on sanitized_name
            if self.filter_tags:
                search_text = f"{file_info['sanitized_name']} {file_info['search_text']}"
                if not any(tag.lower() in search_text.lower() for tag in self.filter_tags):
                    return
            
            # DUPLICATE CHECK using sanitized_name
            # This checks: sanitized_name, caption_first_line, file_name, file_unique_id, file_hash
            exists = await database.check_file_exists(
                file_unique_id=file_info.get('file_unique_id'),
                file_hash=file_info.get('file_hash'),
                file_info=file_info  # Contains sanitized_name
            )
            
            if exists:
                self.skipped_duplicates += 1
                LOGGER.debug(f"Skipped duplicate: {file_info['sanitized_name']}")
                return
            
            # Add to database with sanitized_name
            await database.add_file_entry(
                self.channel_id, message.id, file_info
            )
            self.db_entries += 1
            LOGGER.debug(f"Added new file: {file_info['sanitized_name']}")
            
        except Exception as e:
            LOGGER.error(f"Error processing file {file_info.get('sanitized_name', 'unknown')}: {e}")

    def _extract_file_info_sync(self, message):
        """
        Extract file info with SANITIZED_NAME as primary identifier
        Priority: caption_first_line → file_name
        """
        file_info = {}
        media = None

        # Determine media type
        if message.document:
            media = message.document
        elif message.video:
            media = message.video
        elif message.audio:
            media = message.audio
        elif message.photo:
            media = message.photo
        else:
            return None

        # Extract basic file data
        file_info['file_unique_id'] = media.file_unique_id
        file_info['file_name'] = getattr(media, 'file_name', '') or f"file_{message.id}"
        file_info['file_size'] = getattr(media, 'file_size', 0)
        file_info['mime_type'] = getattr(media, 'mime_type', '')
        file_info['date'] = message.date
        file_info['message_id'] = message.id

        # Extract caption first line (PRIORITY SOURCE)
        if message.caption:
            first_line = message.caption.split('\n')[0].strip()
            file_info['caption_first_line'] = first_line
        else:
            file_info['caption_first_line'] = ''

        # === SANITIZED_NAME GENERATION (CORE LOGIC) ===
        # Priority: caption_first_line > file_name
        if file_info['caption_first_line']:
            # Use caption as primary source
            base_name = file_info['caption_first_line']
        else:
            # Fallback to file_name
            base_name = file_info['file_name']
        
        # Apply sanitization (same function as channel_leech)
        file_info['sanitized_name'] = sanitize_filename(base_name)

        # Build searchable text
        file_info['search_text'] = f"{file_info['file_name']} {file_info['caption_first_line']}"

        # Generate hash for duplicate detection
        hash_source = f"{file_info['file_unique_id']}|{file_info['file_size']}"
        file_info['file_hash'] = hashlib.md5(hash_source.encode()).hexdigest()

        return file_info

    async def stop(self):
        """Stop the scanning process"""
        self.running = False
        LOGGER.info("Channel scan stopped by user")

    async def _update_status(self, msg):
        """Update status message"""
        if self.status_message:
            try:
                await edit_message(self.status_message, msg)
            except Exception as e:
                LOGGER.error(f"Failed to update status: {e}")
