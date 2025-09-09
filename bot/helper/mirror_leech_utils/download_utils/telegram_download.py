from asyncio import Lock, sleep
from time import time
from pyrogram.errors import FloodWait, RPCError

from bot import (LOGGER, task_dict, task_dict_lock, bot, user,
                 INCOMPLETE_TASK_NOTIFIER)
from bot.helper.ext_utils.bot_utils import MirrorStatus, get_readable_file_size, get_readable_time
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from bot.helper.mirror_leech_utils.status_utils.queue_status import QueueStatus
from bot.helper.mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage

global_lock = Lock()
GLOBAL_GID = set()

class TelegramDownloadHelper:
    def __init__(self, listener):
        self._processed_bytes = 0
        self._start_time = time()
        self.listener = listener
        self._id = ""
        self.session = ""

    @property
    def speed(self):
        return self._processed_bytes / (time() - self._start_time)

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def _onDownloadStart(self, file_id, from_queue):
        async with global_lock:
            GLOBAL_GID.add(file_id)
        self._id = file_id
        async with task_dict_lock:
            task_dict[self.listener.mid] = TelegramStatus(
                self.listener, self, file_id[:12], 'dl')
        if not from_queue and self.listener.multi <= 1:
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Download from Telegram: {self.listener.name}")
        if self.listener.isSuperGroup and INCOMPLETE_TASK_NOTIFIER:
            await DbManager().add_incomplete_task(self.listener.message.chat.id, self.listener.message.link, self.listener.tag)

    async def _onDownloadProgress(self, current, total):
        if self.listener.isCancelled:
            if self.session == "user":
                user.stop_transmission()
            else:
                bot.stop_transmission()
        self._processed_bytes = current

    async def _onDownloadError(self, error):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        if self.listener.isSuperGroup and INCOMPLETE_TASK_NOTIFIER:
            await DbManager().rm_complete_task(self.listener.message.link)
        await self.listener.onDownloadError(error)

    async def _onDownloadComplete(self):
        async with global_lock:
            GLOBAL_GID.remove(self._id)
        if self.listener.isSuperGroup and INCOMPLETE_TASK_NOTIFIER:
            await DbManager().rm_complete_task(self.listener.message.link)
        await self.listener.onDownloadComplete()

    async def _download(self, message, path):
        try:
            download = await message.download(file_name=path, progress=self._onDownloadProgress)
            if self.listener.isCancelled:
                await self._onDownloadError("Cancelled by user!")
                return
        except FloodWait as f:
            LOGGER.warning(str(f))
            await sleep(f.value)
        except Exception as e:
            LOGGER.error(str(e))
            await self._onDownloadError(str(e))
            return
        if download is not None:
            await self._onDownloadComplete()
        elif not self.listener.isCancelled:
            await self._onDownloadError('Internal error occurred')

    async def add_download(self, message, path, session, name="", caption=""):
        self.session = session
        if self.session != "user":
            self.session = "bot"
        if self.session == "user" and not self.listener.isSuperGroup:
            await self.listener.onDownloadError("Use SuperGroup to download with User Session!")
            return

        media = message.document or message.photo or message.video or message.audio or message.voice or message.video_note or message.sticker or message.animation or None

        if media is not None:
            async with global_lock:
                download = media.file_unique_id not in GLOBAL_GID
            if download:
                if name:
                    self.listener.name = name
                    path = path + self.listener.name
                elif self.listener.name == "":
                    self.listener.name = media.file_name if hasattr(
                        media, 'file_name') and media.file_name else "None"
                else:
                    path = path + self.listener.name
                if caption:
                    self.listener.caption = caption
                self.listener.size = media.file_size
                msg, button = await stop_duplicate_check(self.listener)
                if msg:
                    await self.listener.onDownloadError(msg, button)
                    return
                add_to_queue, event = await check_running_tasks()
                if add_to_queue:
                    LOGGER.info(f"Added to Queue/Download: {self.listener.name}")
                    async with task_dict_lock:
                        task_dict[self.listener.mid] = QueueStatus(
                            self.listener, media.file_unique_id, 'dl')
                    await self.listener.onDownloadStart()
                    if self.listener.multi <= 1:
                        await sendStatusMessage(self.listener.message)
                    await event.wait()
                    if self.listener.isCancelled:
                        async with global_lock:
                            if self._id in GLOBAL_GID:
                                GLOBAL_GID.remove(self._id)
                        return
                await self._onDownloadStart(media.file_unique_id, add_to_queue)
                await self._download(message, path)
            else:
                await self.listener.onDownloadError('File already being downloaded!')
        else:
            await self.listener.onDownloadError('No document in the replied message!')

    async def cancel_task(self):
        self.listener.isCancelled = True
        LOGGER.info(
            f"Cancelling download on user request: name: {self.listener.name} id: {self._id}")
