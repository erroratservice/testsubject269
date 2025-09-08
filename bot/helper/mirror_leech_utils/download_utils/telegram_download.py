import hashlib
import re
from asyncio import Lock, sleep
from time import time
from pyrogram.errors import FloodWait
from bot import (
    LOGGER,
    task_dict,
    task_dict_lock,
    bot,
    user,
)
from ...ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from ...mirror_leech_utils.status_utils.queue_status import QueueStatus
from ...mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ...telegram_helper.message_utils import send_status_message

global_lock = Lock()
GLOBAL_GID = set()

def generate_universal_telegram_gid(chat_id: int, message_id: int) -> str:
    """
    Generate deterministic GID for ALL Telegram downloads
    This ensures perfect tracking for both channel leech and regular downloads
    """
    combined = f"tg_{chat_id}_{message_id}"
    hash_obj = hashlib.sha256(combined.encode('utf-8'))
    gid = hash_obj.hexdigest()[:12]
    return gid

def extract_telegram_ids_from_message(message) -> tuple:
    """Extract chat_id and message_id from Telegram message object"""
    try:
        chat_id = message.chat.id
        message_id = message.id
        return chat_id, message_id
    except Exception as e:
        LOGGER.error(f"[TELEGRAM-GID] Failed to extract IDs: {e}")
        return None, None

class TelegramDownloadHelper:
    def __init__(self, listener):
        self._processed_bytes = 0
        self._start_time = time()
        self._listener = listener
        self._id = ""
        self.session = ""

    @property
    def speed(self):
        return self._processed_bytes / (time() - self._start_time)

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def _on_download_start(self, file_id, from_queue):
        self._id = file_id
        async with task_dict_lock:
            task_dict[self._listener.mid] = TelegramStatus(
                self._listener, self, file_id[:12], "dl"
            )
        if not from_queue:
            await self._listener.on_download_start()
            if self._listener.multi <= 1:
                await send_status_message(self._listener.message)
            LOGGER.info(f"Download from Telegram: {self._listener.name}")
        else:
            LOGGER.info(f"Start Queued Download from Telegram: {self._listener.name}")

    async def _on_download_progress(self, current, total):
        if self._listener.is_cancelled:
            if self.session == "user":
                user.stop_transmission()
            else:
                bot.stop_transmission()
        self._processed_bytes = current

    async def _on_download_error(self, error):
        await self._listener.on_download_error(error)

    async def _on_download_complete(self):
        await self._listener.on_download_complete()

    async def _download(self, message, path):
        try:
            download = await message.download(
                file_name=path, progress=self._on_download_progress
            )
            if self._listener.is_cancelled:
                await self._on_download_error("Cancelled by user!")
                return
        except FloodWait as f:
            LOGGER.warning(str(f))
            await sleep(f.value)
        except Exception as e:
            LOGGER.error(str(e))
            await self._on_download_error(str(e))
            return
        if download is not None:
            await self._on_download_complete()
        elif not self._listener.is_cancelled:
            await self._on_download_error("Internal error occurred")

    async def add_download(self, message, path, session, name="", caption=""):
        self.session = session
        if (
            self.session not in ["user", "bot"]
            and self._listener.user_transmission
            and self._listener.is_super_chat
        ):
            self.session = "user"
            message = await user.get_messages(
                chat_id=message.chat.id, message_ids=message.id
            )
        elif self.session != "user":
            self.session = "bot"

        media = (
            message.document
            or message.photo
            or message.video
            or message.audio
            or message.voice
            or message.video_note
            or message.sticker
            or message.animation
            or None
        )

        if media is None:
            await self._on_download_error("No media found in the message.")
            return

        chat_id, message_id = extract_telegram_ids_from_message(message)
        gid = generate_universal_telegram_gid(chat_id, message_id) if chat_id and message_id else media.file_unique_id
        
        async with global_lock:
            if gid in GLOBAL_GID:
                await self._on_download_error(f"Download with GID {gid} already in progress.")
                return
            GLOBAL_GID.add(gid)

        try:
            if name:
                self._listener.name = name
            elif self._listener.name == "":
                self._listener.name = media.file_name if hasattr(media, "file_name") and media.file_name else f"tg_{gid}"
            
            path += self._listener.name
            if caption:
                self._listener.caption = caption
            self._listener.size = media.file_size
            
            msg, button = await stop_duplicate_check(self._listener)
            if msg:
                await self._on_download_error(msg, button)
                return

            add_to_queue, event = await check_running_tasks(self._listener)
            if add_to_queue:
                LOGGER.info(f"Added to Queue/Download: {self._listener.name}")
                async with task_dict_lock:
                    task_dict[self._listener.mid] = QueueStatus(self._listener, gid, "dl")
                await self._listener.on_download_start()
                if self._listener.multi <= 1:
                    await send_status_message(self._listener.message)
                await event.wait()
                if self._listener.is_cancelled:
                    return

            await self._on_download_start(gid, add_to_queue)
            await self._download(message, path)
        finally:
            async with global_lock:
                GLOBAL_GID.discard(gid)

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling download on user request: name: {self._listener.name} id: {self._id}")
