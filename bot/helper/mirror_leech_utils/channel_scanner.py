import asyncio
import hashlib
import logging
from datetime import datetime
from pyrogram.errors import FloodWait, PeerIdInvalid
from pyrogram import types

from ..ext_utils.db_handler import database
from ..telegram_helper.message_utils import edit_message

LOGGER = logging.getLogger(__name__)

class ChannelScanner:
    """
    Optimized channel scanner using ID-based batching:
    1. User session gets latest message ID (fast)
    2. Bot session fetches messages in batches of 200 by ID (20x faster)
    3. Processes batches with parallel DB operations
    """
    
    def __init__(self, user_client, bot_client, channel_id, batch_size=200, max_messages=0, filter_tags=None):
        self.user_client = user_client
        self.bot_client = bot_client  # ADD bot client
        self.channel_id = channel_id
        self.batch_size = batch_size  # Increased from 100 to 200
        self.max_messages = max_messages
        self.filter_tags = filter_tags or []
        self.running = True
        self.processed = 0
        self.db_entries = 0
        self.status_message = None
        self.listener = None
        
        # Optimized timings
        self.batch_sleep = 2  # Reduced from 5s to 2s
        self.api_delay = 0.05  # 50ms between API calls (well under 20/sec limit)

    async def scan(self, status_msg=None):
        """Main scanning function with ID-based batching"""
        self.status_message = status_msg
        
        try:
            # STEP 1: Use user session to get channel info and latest message ID
            try:
                chat = await self.user_client.get_chat(self.channel_id)
                await self._update_status(f"📋 Scanning channel: **{chat.title}**")
            except PeerIdInvalid:
                error_msg = (
                    f"❌ **Access Denied to {self.channel_id}**\n\n"
                    f"**Reason:** User session is not a member of this channel\n\n"
                    f"**Solutions:**\n"
                    f"• Join the channel with your user account\n"
                    f"• For private channels: Get invited first"
                )
                await self._update_status(error_msg)
                return
            except Exception as e:
                raise e
            
            # STEP 2: Get total message count and latest message ID using user session
            total_messages = await self.user_client.get_chat_history_count(self.channel_id)
            
            # Get the latest message ID (starting point)
            latest_msg = None
            async for msg in self.user_client.get_chat_history(self.channel_id, limit=1):
                latest_msg = msg
                break
            
            if not latest_msg:
                await self._update_status("❌ **No messages found in channel**")
                return
            
            start_id = latest_msg.id
            LOGGER.info(f"Channel has {total_messages} messages, starting from ID {start_id}")
            
            # STEP 3: Process in batches using bot session (much faster)
            await self._process_in_batches(start_id, total_messages)
            
            # Final status
            if not (hasattr(self, 'listener') and self.listener and self.listener.is_cancelled):
                await self._update_status(
                    f'✅ **Scan complete!** Processed: {self.processed} | New files: {self.db_entries}'
                )

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
                LOGGER.info("Scan cancelled")
                break
            
            # Generate batch of message IDs (200 at a time)
            message_ids = list(range(max(1, current_id - self.batch_size + 1), current_id + 1))
            message_ids.reverse()  # Process newest to oldest
            
            batch_num += 1
            LOGGER.info(f"Batch {batch_num}: Fetching messages {message_ids[0]} to {message_ids[-1]}")
            
            # STEP 4: Use BOT session to fetch batch (20x faster than iteration)
            try:
                messages = await self.bot_client.get_messages(
                    self.channel_id,
                    message_ids=message_ids
                )
                
                # Filter out None/empty messages
                valid_messages = [msg for msg in messages if msg and not isinstance(msg, int)]
                
                # Process batch
                await self._process_batch(valid_messages)
                
                self.processed += len(valid_messages)
                
                # Update status
                await self._update_status(
                    f'🔍 Batch {batch_num} | Scanned: {self.processed}/{total_messages} | New files: {self.db_entries}'
                )
                
                # Small delay to stay under rate limits
                await asyncio.sleep(self.batch_sleep)
                
            except FloodWait as e:
                LOGGER.warning(f'FloodWait in batch: {e.x}s')
                await asyncio.sleep(e.x + 1)
                continue  # Retry this batch
            
            except Exception as e:
                LOGGER.error(f"Error fetching batch: {e}")
                # Continue with next batch on error
            
            # Move to next batch
            current_id -= self.batch_size
            
            # Check max messages limit
            if self.max_messages > 0 and self.processed >= self.max_messages:
                break

    async def _process_batch(self, messages):
        """Process a batch of messages in parallel"""
        tasks = []
        
        for message in messages:
            if not message:
                continue
            
            # Extract file info
            file_info = self._extract_file_info_sync(message)
            if file_info:
                # Create async task for each file
                tasks.append(self._process_single_file(message, file_info))
        
        # Process all files in batch concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_file(self, message, file_info):
        """Process individual file with duplicate check"""
        try:
            # Apply filter tags
            if self.filter_tags:
                if not any(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                    return
            
            # Check if already exists
            exists = await database.check_file_exists(
                file_unique_id=file_info.get('file_unique_id'),
                file_hash=file_info.get('file_hash'),
                file_info=file_info
            )
            
            if exists:
                return
            
            # Add to database
            await database.add_file_entry(
                self.channel_id, message.id, file_info
            )
            self.db_entries += 1
            
        except Exception as e:
            LOGGER.error(f"Error processing file {file_info.get('file_name')}: {e}")

    def _extract_file_info_sync(self, message):
        """
        Synchronous file info extraction (no await needed)
        Much faster than async version
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

        # Extract caption first line
        if message.caption:
            first_line = message.caption.split('\n')[0].strip()
            file_info['caption_first_line'] = first_line
        else:
            file_info['caption_first_line'] = ''

        # Build searchable text
        file_info['search_text'] = f"{file_info['file_name']} {file_info['caption_first_line']}"

        # Generate hash
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
