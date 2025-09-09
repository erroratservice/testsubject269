from asyncio import Lock, sleep, TimeoutError
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
        async with global_lock:
            GLOBAL_GID.add(file_id)
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
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        await self._listener.on_download_error(error)

    async def _on_download_complete(self):
        await self._listener.on_download_complete()
        async with global_lock:
            GLOBAL_GID.remove(self._id)

    async def _download(self, message, path):
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                LOGGER.info(f"Starting download attempt {retry_count + 1}/{max_retries + 1}: {self._listener.name}")
                
                download = await message.download(
                    file_name=path, progress=self._on_download_progress
                )
                
                if self._listener.is_cancelled:
                    await self._on_download_error("Cancelled by user!")
                    return
                
                if download is not None:
                    LOGGER.info(f"Download completed successfully: {self._listener.name}")
                    await self._on_download_complete()
                    return
                else:
                    error_msg = f"Download failed (None returned) - attempt {retry_count + 1}/{max_retries + 1}"
                    LOGGER.error(error_msg)
                    
                    if retry_count >= max_retries:
                        await self._on_download_error("Download failed: No data received after maximum retries")
                        return
                    else:
                        retry_count += 1
                        await sleep(5)
                        continue
                
            except FloodWait as f:
                LOGGER.warning(f"FloodWait encountered: sleeping for {f.value} seconds")
                await sleep(f.value)
                continue
                
            except TimeoutError as e:
                error_msg = f"TimeoutError during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                if retry_count >= max_retries:
                    await self._on_download_error(f"Download timed out after {max_retries + 1} attempts")
                    await self.cancel_task()
                    return
                else:
                    retry_count += 1
                    await sleep(10)
                    continue
                    
            except OSError as e:
                error_msg = f"Network error during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                if "Network is unreachable" in str(e) or "Connection reset" in str(e):
                    if retry_count >= max_retries:
                        await self._on_download_error(f"Network error after {max_retries + 1} attempts: {str(e)}")
                        return
                    else:
                        retry_count += 1
                        await sleep(15)
                        continue
                else:
                    await self._on_download_error(f"System error: {str(e)}")
                    return
                    
            except Exception as e:
                error_msg = f"Unexpected error during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                if retry_count >= max_retries:
                    await self._on_download_error(f"Download failed after {max_retries + 1} attempts: {str(e)}")
                    return
                else:
                    retry_count += 1
                    await sleep(5)
                    continue

    async def add_download(self, message, path, session):
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
        
        if media is not None:
            async with global_lock:
                download = media.file_unique_id not in GLOBAL_GID
            if download:
                if self._listener.name == "":
                    self._listener.name = (
                        media.file_name if hasattr(media, "file_name") else "None"
                    )
                else:
                    path = path + self._listener.name
                self._listener.size = media.file_size
                
                # Set TaskListener attributes for failure tracking
                self._listener.expected_size = media.file_size
                self._listener.file_unique_id = media.file_unique_id
                
                gid = media.file_unique_id
                msg, button = await stop_duplicate_check(self._listener)
                if msg:
                    await self._listener.on_download_error(msg, button)
                    return
                    
                add_to_queue, event = await check_running_tasks(self._listener)
                if add_to_queue:
                    LOGGER.info(f"Added to Queue/Download: {self._listener.name}")
                    async with task_dict_lock:
                        task_dict[self._listener.mid] = QueueStatus(
                            self._listener, gid, "dl"
                        )
                    await self._listener.on_download_start()
                    if self._listener.multi <= 1:
                        await send_status_message(self._listener.message)
                    await event.wait()
                    if self._listener.is_cancelled:
                        async with global_lock:
                            if self._id in GLOBAL_GID:
                                GLOBAL_GID.remove(self._id)
                        return
                        
                await self._on_download_start(gid, add_to_queue)
                await self._download(message, path)
            else:
                await self._on_download_error("File already being downloaded!")
        else:
            await self._on_download_error(
                "No document in the replied message! Use SuperGroup incase you are trying to download with User session!"
            )

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(
            f"Cancelling download on user request: name: {self._listener.name} id: {self._id}"
        )
