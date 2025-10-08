from asyncio import Lock, sleep, TimeoutError
from time import time
import os
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
        self._expected_size = 0
        self._download_start_size = 0
        self._last_progress_time = time()
        self._stall_timeout = 300  # 5 minutes without progress = stalled

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
        self._last_progress_time = time()
        
        # Log progress every 10MB for debugging
        if current > 0 and current % (10 * 1024 * 1024) == 0:
            progress_percent = (current / total * 100) if total > 0 else 0
            LOGGER.info(f"Download progress: {current}/{total} bytes ({progress_percent:.1f}%) - {self._listener.name}")

    async def _on_download_error(self, error):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        await self._listener.on_download_error(error)

    async def _on_download_complete(self):
        await self._listener.on_download_complete()
        async with global_lock:
            GLOBAL_GID.remove(self._id)

    def _validate_download_completion(self, downloaded_path, expected_size):
        """Validate that download completed successfully"""
        try:
            if not os.path.exists(downloaded_path):
                return False, "Downloaded file does not exist"
            
            actual_size = os.path.getsize(downloaded_path)
            
            # Allow 1% tolerance for size differences
            size_tolerance = max(1024, expected_size * 0.01)  # At least 1KB tolerance
            
            if abs(actual_size - expected_size) > size_tolerance:
                size_diff_mb = (expected_size - actual_size) / (1024 * 1024)
                return False, f"Size mismatch: Expected {expected_size} bytes, got {actual_size} bytes (missing {size_diff_mb:.2f}MB)"
            
            # Check if file is not empty
            if actual_size == 0:
                return False, "Downloaded file is empty"
            
            # Check if file size is suspiciously small compared to expected
            if actual_size < (expected_size * 0.5):
                return False, f"Download appears incomplete: {actual_size}/{expected_size} bytes ({actual_size/expected_size*100:.1f}%)"
            
            return True, "Download validation successful"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def _is_download_stalled(self):
        """Check if download has stalled (no progress for extended period)"""
        return (time() - self._last_progress_time) > self._stall_timeout

    async def _download(self, message, path):
        max_retries = 3
        retry_count = 0
        base_backoff = 10  # Start with 10 seconds backoff
        
        while retry_count <= max_retries:
            try:
                LOGGER.info(f"Starting download attempt {retry_count + 1}/{max_retries + 1}: {self._listener.name}")
                
                # Reset progress tracking
                self._processed_bytes = 0
                self._last_progress_time = time()
                
                # Store expected size for validation
                media = (message.document or message.photo or message.video or 
                        message.audio or message.voice or message.video_note or 
                        message.sticker or message.animation)
                
                if media:
                    self._expected_size = media.file_size
                    LOGGER.info(f"Expected file size: {self._expected_size} bytes ({self._expected_size/(1024*1024):.2f}MB)")
                
                # Start download with enhanced error handling
                download_start_time = time()
                try:
                    download_result = await message.download(
                        file_name=path, 
                        progress=self._on_download_progress
                    )
                except TimeoutError as timeout_err:
                    download_time = time() - download_start_time
                    raise TimeoutError(f"Download timeout after {download_time:.1f}s: {str(timeout_err)}")
                
                if self._listener.is_cancelled:
                    await self._on_download_error("Cancelled by user!")
                    return
                
                # CRITICAL: Validate download completion
                if download_result is not None:
                    # Check if download stalled
                    if self._is_download_stalled():
                        raise Exception(f"Download stalled - no progress for {self._stall_timeout}s")
                    
                    # Validate file integrity
                    is_valid, validation_msg = self._validate_download_completion(
                        download_result, self._expected_size
                    )
                    
                    if is_valid:
                        download_time = time() - download_start_time
                        avg_speed = self._expected_size / download_time / (1024 * 1024) if download_time > 0 else 0
                        LOGGER.info(f"Download completed and validated successfully: {self._listener.name} "
                                  f"({download_time:.1f}s, {avg_speed:.2f}MB/s)")
                        await self._on_download_complete()
                        return
                    else:
                        # Validation failed - remove partial file and retry
                        try:
                            if os.path.exists(download_result):
                                os.remove(download_result)
                                LOGGER.warning(f"Removed invalid download file: {download_result}")
                        except:
                            pass
                        
                        raise Exception(f"Download validation failed: {validation_msg}")
                else:
                    raise Exception("Download returned None - no data received")
                
            except FloodWait as f:
                wait_time = f.value + 5  # Add 5 seconds buffer
                LOGGER.warning(f"FloodWait: sleeping for {wait_time}s (attempt {retry_count + 1})")
                await sleep(wait_time)
                continue  # Don't count FloodWait as a retry
                
            except TimeoutError as e:
                error_msg = f"TimeoutError during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                if retry_count >= max_retries:
                    await self._on_download_error(f"Download timed out after {max_retries + 1} attempts")
                    return
                else:
                    retry_count += 1
                    backoff_time = base_backoff * (2 ** (retry_count - 1))  # Exponential backoff
                    LOGGER.info(f"Retrying after {backoff_time}s backoff...")
                    await sleep(backoff_time)
                    continue
                    
            except OSError as e:
                error_msg = f"Network error during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                # Handle specific network errors
                if any(err_type in str(e).lower() for err_type in 
                      ["network is unreachable", "connection reset", "connection timed out", "broken pipe"]):
                    if retry_count >= max_retries:
                        await self._on_download_error(f"Network error after {max_retries + 1} attempts: {str(e)}")
                        return
                    else:
                        retry_count += 1
                        backoff_time = base_backoff * (2 ** (retry_count - 1))
                        LOGGER.info(f"Network error - retrying after {backoff_time}s...")
                        await sleep(backoff_time)
                        continue
                else:
                    await self._on_download_error(f"System error: {str(e)}")
                    return
                    
            except Exception as e:
                error_msg = f"Unexpected error during download - attempt {retry_count + 1}/{max_retries + 1}: {str(e)}"
                LOGGER.error(error_msg)
                
                # Check for specific Pyrogram errors that indicate corruption
                if any(err_type in str(e).lower() for err_type in 
                      ["validation failed", "size mismatch", "appears incomplete", "download stalled"]):
                    if retry_count >= max_retries:
                        await self._on_download_error(f"Download validation failed after {max_retries + 1} attempts: {str(e)}")
                        return
                    else:
                        retry_count += 1
                        backoff_time = base_backoff * (2 ** (retry_count - 1))
                        LOGGER.info(f"Download corrupted - retrying after {backoff_time}s...")
                        await sleep(backoff_time)
                        continue
                else:
                    await self._on_download_error(f"Critical error: {str(e)}")
                    return

        # If we reach here, all retries exhausted
        await self._on_download_error(f"Download failed after {max_retries + 1} attempts")

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
                
                # Enhanced TaskListener attributes for failure tracking
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
