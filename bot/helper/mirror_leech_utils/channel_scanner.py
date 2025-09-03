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

    async def scan(self, status_msg=None):
        """Main scanning function"""
        self.status_message = status_msg
        try:
            async for message in self.user_client.get_chat_history(self.channel_id, limit=self.max_messages):
                if not self.running:
                    break
                
                self.processed += 1
                file_info = await self._extract_file_info(message)

                if not file_info:
                    continue

                # Apply filter if provided
                if self.filter_tags:
                    if not any(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                        continue

                # Check if already exists in database
                exists = await database.check_file_exists(
                    file_info.get('file_unique_id'),
                    file_info.get('file_hash'),
                    file_info.get('file_name')
                )

                if exists:
                    continue

                # Add to database
                await database.add_file_entry(
                    self.channel_id, message.id, file_info
                )
                self.db_entries += 1

                # Update status periodically
                if self.processed % self.batch_size == 0:
                    await self._update_status(f'🔍 Scanned: {self.processed} | New files: {self.db_entries}')

            await self._update_status(f'✅ Scan complete! Processed: {self.processed} | New files: {self.db_entries}')

        except FloodWait as e:
            LOGGER.warning(f'FloodWait: waiting {e.x} seconds')
            await asyncio.sleep(e.x + 1)
            # Continue scanning after flood wait
            await self.scan(status_msg)

    async def stop(self):
        """Stop the scanning process"""
        self.running = False

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
