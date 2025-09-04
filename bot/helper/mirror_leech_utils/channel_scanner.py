import asyncio
import hashlib
import logging
from datetime import datetime

from pyrogram.errors import FloodWait

from ..ext_utils.db_handler import database
from ..telegram_helper.message_utils import edit_message

LOGGER = logging.getLogger(__name__)

class ChannelScanner:
    def __init__(self, user_client, channel_id, batch_size=50, max_messages=0, filter_tags=None):
        self.user_client = user_client
        self.channel_id = channel_id
        self.batch_size = batch_size
        self.max_messages = max_messages  # 0 for all messages
        self.filter_tags = filter_tags or []
        self.running = True
        self.processed = 0
        self.db_entries = 0
        self.status_message = None
        self.batch_sleep = 2  # Sleep 2 seconds after each batch
        self.message_sleep = 0.1  # Small delay between messages

    async def scan(self, status_msg=None):
        """Main scanning function with proper batching"""
        self.status_message = status_msg
        batch_count = 0
        
        try:
            async for message in self.user_client.get_chat_history(self.channel_id, limit=self.max_messages):
                if not self.running:
                    break
                
                self.processed += 1
                batch_count += 1
                
                # Process message
                file_info = await self._extract_file_info(message)
                if file_info:
                    await self._process_file_info(message, file_info)

                # Small delay between messages to be gentle on API
                await asyncio.sleep(self.message_sleep)
                
                # Batch processing with status updates and sleep
                if batch_count >= self.batch_size:
                    await self._handle_batch_complete(batch_count)
                    batch_count = 0
                    
                    # Sleep after each batch to respect rate limits
                    LOGGER.info(f"Completed batch, sleeping for {self.batch_sleep}s")
                    await asyncio.sleep(self.batch_sleep)

            # Handle remaining messages if any
            if batch_count > 0:
                await self._handle_batch_complete(batch_count)

            await self._update_status(
                f'✅ Scan complete! Processed: {self.processed} | New files: {self.db_entries}'
            )

        except FloodWait as e:
            LOGGER.warning(f'FloodWait encountered: waiting {e.x} seconds')
            await self._update_status(f'⏳ Rate limited, waiting {e.x} seconds...')
            await asyncio.sleep(e.x + 1)  # Add 1 second buffer
            
            # Resume scanning from where we left off
            LOGGER.info("Resuming scan after FloodWait")
            await self.scan(status_msg)
            
        except Exception as e:
            LOGGER.error(f"Scanning error: {e}")
            await self._update_status(f'❌ Scanning error: {str(e)}')

    async def _process_file_info(self, message, file_info):
        """Process individual file info"""
        if self.filter_tags:
            if not any(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                return

        # Check if already exists in database
        exists = await database.check_file_exists(
            file_info.get('file_unique_id'),
            file_info.get('file_hash'),
            file_info.get('file_name')
        )

        if exists:
            return

        # Add to database
        try:
            await database.add_file_entry(
                self.channel_id, message.id, file_info
            )
            self.db_entries += 1
        except Exception as e:
            LOGGER.error(f"Error adding file to database: {e}")

    async def _handle_batch_complete(self, batch_count):
        """Handle batch completion with status update"""
        await self._update_status(
            f'🔍 Scanned: {self.processed} messages | New files: {self.db_entries} | Batch: {batch_count}'
        )
        
        # Log batch completion
        LOGGER.info(f"Batch completed: {batch_count} messages processed")

    async def stop(self):
        """Stop the scanning process"""
        self.running = False
        LOGGER.info("Channel scan stopped by user")

    async def _extract_file_info(self, message):
        """Extract file information from message"""
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

        # Build searchable text combining filename and caption
        file_info['search_text'] = f"{file_info['file_name']} {file_info['caption_first_line']}"

        # Generate hash for duplicate detection
        hash_source = f"{file_info['file_unique_id']}|{file_info['file_size']}"
        file_info['file_hash'] = hashlib.md5(hash_source.encode()).hexdigest()

        return file_info

    async def _update_status(self, msg):
        """Update status message"""
        if self.status_message:
            try:
                await edit_message(self.status_message, msg)
            except Exception as e:
                LOGGER.error(f"Failed to update status: {e}")
