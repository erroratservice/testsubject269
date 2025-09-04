import asyncio
import logging
import os
import re
import time
from contextlib import suppress
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message
from bot import (DATABASE_URL, IS_PREMIUM_USER, LOGGER, bot,
                 leech_log, user_data)
from bot.helper.ext_utils.bot_utils import (get_readable_time, new_thread,
                                            sync_to_async)
from bot.helper.ext_utils.db_handler import DbManger
from bot.helper.ext_utils.files_utils import get_path_size
from bot.helper.ext_utils.task_manager import task_utils
from bot.helper.listeners.task_listener import TaskListener
from bot.helper.mirror_leech_utils.download_utils.telegram_download import \
    TelegramDownloadHelper
from bot.helper.telegram_helper.message_utils import (delete_links,
                                                      edit_message,
                                                      send_message,
                                                      delete_message)

class ChannelLeech(TaskListener):
    def __init__(self, message: Message, tag: str, is_leech=False, pswd=None):
        super().__init__(message, tag, is_leech=is_leech, pswd=pswd)
        self.total_files = 0
        self.corrupted_files = 0
        self.move_to_channel = False
        self.multi = 0
        self.leech_log = leech_log
        self.filter_list = []
        self.as_doc = False
        self.thumb = None
        self.first_message = None
        self.first_message_lock = asyncio.Lock()
        self.last_message_id = 0
        self.is_cancelled = False
        self.leech_log_id = 0
        self.user_settings()

    async def on_task_start(self):
        async with self.first_message_lock:
            if self.first_message is None:
                self.first_message = await send_message(
                    self.message,
                    "Channel leeching has started..."
                )
                self.leech_log_id = self.first_message.id

    async def on_task_complete(self):
        await self.log_to_channel()
        if self.is_cancelled:
            return

        total_size = await get_path_size(self.dir)
        msg = f"<b>Leech completed from channel:</b> <code>{self.tag}</code>\n"
        msg += f"<b>Total files leeched:</b> {self.total_files}\n"
        msg += f"<b>Corrupted files:</b> {self.corrupted_files}\n"
        msg += f"<b>Total size:</b> {total_size}\n"
        msg += f"<b>Time taken:</b> {get_readable_time(time.time() - self.start_time)}\n"
        await send_message(self.message, msg)
        await delete_links(self.message)

    async def on_task_cancel(self):
        self.is_cancelled = True
        await self.log_to_channel()
        if self.message.from_user.id in self.leech_log:
            del self.leech_log[self.message.from_user.id]

    async def on_task_error(self, error: str):
        self.is_cancelled = True
        await self.log_to_channel()
        if self.message.from_user.id in self.leech_log:
            del self.leech_log[self.message.from_user.id]
        await send_message(self.message, f"An error occurred: {error}")

    def user_settings(self):
        user_id = self.message.from_user.id
        user_dict = user_data.get(user_id, {})
        self.as_doc = user_dict.get('as_doc', False)
        self.thumb = user_dict.get('thumb', None)

    async def run(self, messages: list):
        self.total_files = len(messages)
        self.leech_log[self.message.from_user.id] = self

        for message in messages:
            if self.is_cancelled:
                break
            if not message.media:
                self.corrupted_files += 1
                continue
            
            try:
                name = self._get_unique_filename(message)
                await self._download_file(message, name)
                self.last_message_id = message.id
            except Exception as e:
                self.corrupted_files += 1
                LOGGER.error(f"Failed to leech file: {e}")
            
            await asyncio.sleep(1) # Delay between downloads

        await self.on_task_complete()

    def _get_unique_filename(self, message):
        """Generates a unique filename for each message."""
        original_filename = getattr(message, 'document', None) or getattr(message, 'video', None) or \
                            getattr(message, 'audio', None) or getattr(message, 'photo', None)
        
        if original_filename:
            original_filename = original_filename.file_name
        else:
            original_filename = f"file_{message.id}.unknown"

        # Sanitize filename (optional, but recommended)
        sanitized_filename = re.sub(r'[\\/*?:"<>|]', "", original_filename)
        
        # Add a unique prefix/suffix, e.g., message ID and timestamp
        unique_prefix = f"leech_{message.id}_{int(time.time())}"
        
        return f"{unique_prefix}_{sanitized_filename}"

    async def _download_file(self, message, name):
        """Downloads a single file using TelegramDownloadHelper."""
        try:
            download_path = f"{self.dir}/{name}"
            
            # THE FIX: Assign the unique name to the listener
            self.name = name
            
            # Create a new TelegramDownloadHelper for each download
            telegram_helper = TelegramDownloadHelper(self)
            
            # Start the download
            await telegram_helper.add_download(message, download_path, self.message.from_user.id)
            
        except Exception as e:
            LOGGER.error(f"Error downloading file {name}: {e}")
            self.corrupted_files += 1
            await self._log_error(f"Failed to download: {name}. Reason: {e}")

    async def _log_error(self, error_message):
        """Logs an error message to the leech log channel."""
        if self.leech_log_id:
            try:
                await bot.send_message(
                    chat_id=self.message.chat.id,
                    text=error_message,
                    reply_to_message_id=self.leech_log_id
                )
            except RPCError as e:
                LOGGER.error(f"Could not log error to channel: {e}")

    async def log_to_channel(self):
        if not self.leech_log_id:
            return
        
        log_message = f"**Channel Leech Log**\n\n"
        log_message += f"**User:** {self.tag}\n"
        log_message += f"**Total Files Processed:** {self.total_files}\n"
        log_message += f"**Successfully Leeched:** {self.total_files - self.corrupted_files}\n"
        log_message += f"**Corrupted/Skipped:** {self.corrupted_files}\n"
        log_message += f"**Status:** {'Cancelled' if self.is_cancelled else 'Completed'}\n"
        
        if self.last_message_id:
            log_message += f"**Last Processed Message ID:** {self.last_message_id}\n"
            
        try:
            await edit_message(self.first_message, log_message)
        except RPCError as e:
            LOGGER.error(f"Could not update leech log: {e}")


@new_thread
async def channel_leech(client, message: Message):
    try:
        args = message.text.split()
        if len(args) < 2:
            await send_message(message, "Usage: /channelleech [channel_id] [start_msg_id] [end_msg_id]")
            return

        channel_id = int(args[1])
        start_msg_id = int(args[2]) if len(args) > 2 else 0
        end_msg_id = int(args[3]) if len(args) > 3 else 0

        tag = message.from_user.mention
        listener = ChannelLeech(message, tag, is_leech=True)

        status_msg = await send_message(message, f"Fetching messages from {channel_id}...")

        messages_to_leech = []
        current_id = start_msg_id
        
        while True if end_msg_id == 0 else current_id <= end_msg_id:
            try:
                batch = await client.get_messages(
                    chat_id=channel_id,
                    message_ids=range(current_id, min(current_id + 100, end_msg_id + 1) if end_msg_id else current_id + 100)
                )

                if not batch:
                    break

                for msg in batch:
                    if msg.media:
                        messages_to_leech.append(msg)
                
                current_id = batch[-1].id + 1

                if end_msg_id and current_id > end_msg_id:
                    break
                
                await edit_message(status_msg, f"Fetched {len(messages_to_leech)} messages so far...")
                await asyncio.sleep(2) # Flood protection

            except FloodWait as e:
                await asyncio.sleep(e.x)
            except Exception as e:
                await edit_message(status_msg, f"An error occurred: {e}")
                LOGGER.error(f"Error fetching messages: {e}")
                break

        await edit_message(status_msg, f"Found {len(messages_to_leech)} files to leech. Starting download...")
        
        if messages_to_leech:
            await listener.run(messages_to_leech)
        else:
            await send_message(message, "No files found to leech in the specified range.")

    except Exception as e:
        LOGGER.error(f"Channel leech command failed: {e}")
        await send_message(message, f"Error: {e}")
