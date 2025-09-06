from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from bot import bot, user, DOWNLOAD_DIR, LOGGER, user_data, config_dict
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
from ..helper.mirror_leech_utils.download_utils.telegram_download import TelegramDownloadHelper
from ..helper.listeners.task_listener import TaskListener
import asyncio
import os
import re
import time
import traceback

def remove_emoji(text):
    """Remove emojis and special characters from text"""
    emoji_pattern = re.compile(
        '['
        '\U0001F600-\U0001F64F'  # emoticons
        '\U0001F300-\U0001F5FF'  # symbols & pictographs
        '\U0001F680-\U0001F6FF'  # transport & map symbols
        '\U0001F1E0-\U0001F1FF'  # flags (iOS)
        '\U00002702-\U000027B0'  # dingbats
        '\U000024C2-\U0001F251'  # enclosed characters
        '\U0001F900-\U0001F9FF'  # supplemental symbols
        '\U0001FA00-\U0001FA6F'  # chess symbols
        ']+', flags=re.UNICODE
    )
    return emoji_pattern.sub('', text)

def sanitize_filename(filename):
    """Sanitize filename for safe file system use"""
    filename = remove_emoji(filename)
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'[^\w\s.-]', '', filename)
    filename = re.sub(r'\s+', ' ', filename)
    filename = filename.strip()
    if len(filename) > 200:
        filename = filename[:200]
    return filename

class ConcurrentChannelLeech(TaskListener):
    """A dedicated, isolated TaskListener for a single concurrent download with debugging."""
    
    def __init__(self, main_listener, message_to_leech, file_info):
        # Copy essential properties from main listener
        self.message = main_listener.message
        self.client = main_listener.client
        self.user_dict = main_listener.user_dict
        self.use_caption_as_filename = main_listener.use_caption_as_filename
        self.channel_id = main_listener.channel_id
        
        # Store the specific message and file info for this task
        self.message_to_leech = message_to_leech
        self.file_info = file_info
        
        # Set leech mode and clear cloud paths
        self.is_leech = True
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        # Call parent constructor to get a NEW, UNIQUE mid/gid for this specific file
        super().__init__()
        
        # Apply the same user settings as main listener
        self._apply_concurrent_user_settings()
        
        # 🔍 DEBUG: Log task creation
        LOGGER.info(f"[CONCURRENT-DEBUG] Created task {self.mid} for MSG {message_to_leech.id}")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} user_id: {self.user_id}")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} original filename: {file_info['file_name']}")
    
    def _apply_concurrent_user_settings(self):
        """Applies user settings to this isolated instance with debugging."""
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} applying user settings...")
        
        if self.user_dict.get("split_size", False):
            self.split_size = self.user_dict["split_size"]
            LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} split_size from user: {self.split_size}")
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
            LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} split_size from config: {self.split_size}")
        
        if (self.user_dict.get("as_doc", False) or 
            "as_doc" not in self.user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} as_doc: {self.as_doc}")
            
        if self.user_dict.get("leech_dest", False):
            self.leech_dest = self.user_dict["leech_dest"]
        elif "leech_dest" not in self.user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} leech_dest: {self.leech_dest}")
        
        self.up_dest = self.leech_dest

    async def run(self):
        """Executes the download and upload for a single file with comprehensive debugging."""
        start_time = time.time()
        
        # 🔍 DEBUG: Log task start
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} starting execution")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} message ID: {self.message_to_leech.id}")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} channel ID: {self.channel_id}")
        
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        os.makedirs(download_path, exist_ok=True)
        
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} download path: {download_path}")
        
        # Generate unique filename
        final_filename, new_caption = self._generate_file_details(self.message_to_leech, self.file_info)
        self.name = final_filename
        
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} final filename: {final_filename}")
        
        telegram_helper = TelegramDownloadHelper(self)
        
        # 🔍 DEBUG: Log before download attempt
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} calling add_download with:")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid}   - message_id: {self.message_to_leech.id}")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid}   - download_path: {download_path}")
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid}   - user_id: {self.user_id}")
        
        try:
            # 🔧 FIX: Use correct session parameter - this was the main issue
            await telegram_helper.add_download(self.message_to_leech, download_path, self.user_id)
            
            end_time = time.time()
            duration = end_time - start_time
            
            LOGGER.info(f"[CONCURRENT-SUCCESS] Task {self.mid} completed in {duration:.2f}s: {final_filename}")
            return True
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            # 🔍 DEBUG: Comprehensive error logging
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} FAILED after {duration:.2f}s")
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} message_id: {self.message_to_leech.id}")
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} filename: {final_filename}")
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} error type: {type(e).__name__}")
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} error message: {str(e)}")
            
            # 🔍 DEBUG: Categorize error types
            error_str = str(e).lower()
            if "internal error" in error_str:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Telegram API internal error (likely rate limiting)")
            elif "session" in error_str or "auth" in error_str:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Session/authentication issue")
            elif "permission" in error_str or "forbidden" in error_str:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Permission/access issue")
            elif "timeout" in error_str:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Timeout issue")
            elif "flood" in error_str:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Rate limit/flood wait")
            else:
                LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} ANALYSIS: Unknown error type")
            
            # Full traceback for debugging
            LOGGER.error(f"[CONCURRENT-ERROR] Task {self.mid} full traceback:")
            LOGGER.error(traceback.format_exc())
            
            raise

    def _generate_file_details(self, message, file_info):
        """Generate filename and caption with debugging"""
        original_filename = file_info['file_name']
        final_filename = original_filename
        new_caption = None
        
        LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} generating filename from: {original_filename}")
        
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} caption first line: {first_line}")
            
            if first_line:
                clean_name = sanitize_filename(first_line)
                LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} cleaned name: {clean_name}")
                
                if clean_name and len(clean_name) >= 3:
                    original_extension = os.path.splitext(original_filename)[1]
                    
                    # Add unique message ID to prevent filename conflicts
                    final_filename = f"{clean_name}_{message.id}{original_extension}"
                    new_caption = os.path.splitext(final_filename)[0]
                    
                    LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} using caption-based filename: {final_filename}")
                else:
                    # Clean name too short, use original with message ID
                    base_name = os.path.splitext(original_filename)[0]
                    extension = os.path.splitext(original_filename)[1]
                    final_filename = f"{base_name}_{message.id}{extension}"
                    LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} caption too short, using: {final_filename}")
            else:
                # Empty caption, use original with message ID
                base_name = os.path.splitext(original_filename)[0]
                extension = os.path.splitext(original_filename)[1]
                final_filename = f"{base_name}_{message.id}{extension}"
                LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} empty caption, using: {final_filename}")
        else:
            # No caption mode, use original with message ID
            base_name = os.path.splitext(original_filename)[0]
            extension = os.path.splitext(original_filename)[1]
            final_filename = f"{base_name}_{message.id}{extension}"
            LOGGER.info(f"[CONCURRENT-DEBUG] Task {self.mid} no caption mode, using: {final_filename}")
        
        return final_filename, new_caption

class ChannelLeech(TaskListener):
    # Conservative settings for debugging
    CONCURRENCY = 1          # Start with 1 to isolate issues
    TASK_START_DELAY = 5     # 5 second delay between starts  
    BATCH_SIZE = 2           # Small batches
    BATCH_DELAY = 10         # Long delay between batches
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.concurrent_enabled = True
        
        # Concurrent debugging counters
        self.concurrent_attempts = 0
        self.concurrent_successes = 0
        self.concurrent_failures = 0
        
        self.is_leech = True
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        super().__init__()
        self._apply_user_settings_with_fallbacks()
        
        # 🔍 DEBUG: Log main task creation
        LOGGER.info(f"[MAIN-DEBUG] Created main channel leech task {self.mid}")
        LOGGER.info(f"[MAIN-DEBUG] Concurrency settings: {self.CONCURRENCY} parallel, {self.TASK_START_DELAY}s delay")

    def _apply_user_settings_with_fallbacks(self):
        self.user_dict = user_data.get(self.message.from_user.id, {})
        LOGGER.info("=== APPLYING USER SETTINGS WITH FALLBACKS ===")
        LOGGER.info(f"Raw user_dict: {self.user_dict}")
        
        if self.user_dict.get("split_size", False):
            self.split_size = self.user_dict["split_size"]
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
            
        if (self.user_dict.get("as_doc", False) or "as_doc" not in self.user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
            
        if self.user_dict.get("leech_dest", False):
            self.leech_dest = self.user_dict["leech_dest"]
        elif "leech_dest" not in self.user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
            
        self.is_leech = True
        self.up_dest = self.leech_dest
        LOGGER.info("=== USER SETTINGS APPLIED WITH FALLBACKS ===")

    async def new_event(self):
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        
        if 'channel' not in args:
            await send_message(self.message, "Usage: /cleech -ch <channel_id> [--sequential] [--debug]")
            return
            
        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.concurrent_enabled = not args.get('sequential', False)
        
        if not user:
            await send_message(self.message, "User session is required!")
            return
            
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech"
        )
        
        mode = f"Concurrent ({self.CONCURRENCY} parallel)" if self.concurrent_enabled else "Sequential"
        
        self.status_message = await send_message(
            self.message, 
            f"🔍 **DEBUG MODE: Starting channel leech** `{str(self.mid)[:12]}`\n"
            f"**Mode:** {mode}\n"
            f"**Channel:** `{self.channel_id}`\n"
            f"**Settings:** {self.TASK_START_DELAY}s delay, batch size {self.BATCH_SIZE}\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )
        
        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"[MAIN-ERROR] Channel leech error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _process_channel(self):
        tasks = []
        semaphore = asyncio.Semaphore(self.CONCURRENCY)
        processed, downloaded, skipped, errors = 0, 0, 0, 0
        
        try:
            chat = await user.get_chat(self.channel_id)
            await edit_message(self.status_message, f"🔍 **Debugging channel:** {chat.title}")
            
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            batch_tasks = []
            
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("[MAIN-DEBUG] Channel leech cancelled by user")
                    break
                    
                processed += 1
                
                # 🔍 DEBUG: Log message processing
                if processed <= 10:  # Only log first 10 to avoid spam
                    LOGGER.info(f"[MAIN-DEBUG] Processing message {processed}: {message.id}")
                
                await channel_status.update_operation(self.operation_key, processed=processed)
                
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                    
                if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                    continue
                    
                if await database.check_file_exists(file_info.get('file_unique_id')):
                    skipped += 1
                    await channel_status.update_operation(self.operation_key, skipped=skipped)
                    continue

                # 🔍 DEBUG: Log file processing
                LOGGER.info(f"[MAIN-DEBUG] Processing file: {file_info['file_name']} from MSG {message.id}")
                
                concurrent_listener = ConcurrentChannelLeech(self, message, file_info)
                
                if self.concurrent_enabled:
                    self.concurrent_attempts += 1
                    task = asyncio.create_task(self._concurrent_worker(concurrent_listener, semaphore))
                    batch_tasks.append({'task': task, 'message_id': message.id, 'file_info': file_info})
                    
                    # Process batch when full
                    if len(batch_tasks) >= self.BATCH_SIZE:
                        LOGGER.info(f"[BATCH-DEBUG] Processing batch of {len(batch_tasks)} tasks")
                        
                        task_list = [task_info['task'] for task_info in batch_tasks]
                        results = await asyncio.gather(*task_list, return_exceptions=True)
                        
                        batch_successes = 0
                        batch_failures = 0
                        
                        for i, result in enumerate(results):
                            task_info = batch_tasks[i]
                            if isinstance(result, Exception):
                                batch_failures += 1
                                errors += 1
                                LOGGER.error(f"[BATCH-ERROR] Task for MSG {task_info['message_id']} failed: {result}")
                            elif result is True:
                                batch_successes += 1
                                downloaded += 1
                                # Add to database after successful completion
                                await database.add_file_entry(
                                    self.channel_id, task_info['message_id'], task_info['file_info']
                                )
                                LOGGER.info(f"[BATCH-SUCCESS] Task for MSG {task_info['message_id']} completed")
                            else:
                                batch_failures += 1
                                errors += 1
                        
                        self.concurrent_successes += batch_successes
                        self.concurrent_failures += batch_failures
                        
                        LOGGER.info(f"[BATCH-DEBUG] Batch results: {batch_successes} successes, {batch_failures} failures")
                        
                        # Update status
                        await edit_message(
                            self.status_message,
                            f"🔍 **Debug Progress**\n"
                            f"Processed: {processed}\n" 
                            f"Downloaded: {downloaded}\n"
                            f"Skipped: {skipped}\n"
                            f"Errors: {errors}\n"
                            f"Success Rate: {(self.concurrent_successes/max(self.concurrent_attempts,1)*100):.1f}%"
                        )
                        
                        batch_tasks = []
                        LOGGER.info(f"[BATCH-DEBUG] Sleeping {self.BATCH_DELAY}s before next batch")
                        await asyncio.sleep(self.BATCH_DELAY)
                        
                else:
                    # Sequential mode
                    try:
                        success = await self._concurrent_worker(concurrent_listener, semaphore)
                        if success:
                            downloaded += 1
                            await database.add_file_entry(self.channel_id, message.id, file_info)
                        else:
                            errors += 1
                    except Exception as e:
                        errors += 1
                        LOGGER.error(f"[SEQUENTIAL-ERROR] Task for message {message.id} failed: {e}")

            # Process remaining batch tasks
            if self.concurrent_enabled and batch_tasks:
                LOGGER.info(f"[FINAL-DEBUG] Processing final batch of {len(batch_tasks)} tasks")
                task_list = [task_info['task'] for task_info in batch_tasks]
                results = await asyncio.gather(*task_list, return_exceptions=True)
                
                for i, result in enumerate(results):
                    task_info = batch_tasks[i]
                    if isinstance(result, Exception):
                        errors += 1
                    elif result is True:
                        downloaded += 1
                        await database.add_file_entry(
                            self.channel_id, task_info['message_id'], task_info['file_info']
                        )

            # Final statistics
            success_rate = (self.concurrent_successes / max(self.concurrent_attempts, 1) * 100) if self.concurrent_attempts > 0 else 0
            
            final_text = (
                f"🔍 **Debug Channel Leech Completed!**\n\n"
                f"**Processed:** {processed}\n"
                f"**Downloaded:** {downloaded}\n" 
                f"**Skipped:** {skipped}\n"
                f"**Errors:** {errors}\n\n"
                f"**Concurrent Stats:**\n"
                f"Attempts: {self.concurrent_attempts}\n"
                f"Successes: {self.concurrent_successes}\n"
                f"Failures: {self.concurrent_failures}\n"
                f"Success Rate: {success_rate:.1f}%\n\n"
                f"**Check logs for detailed error analysis**"
            )
            
            await edit_message(self.status_message, final_text)
            
            # 🔍 DEBUG: Final summary
            LOGGER.info(f"[FINAL-DEBUG] Channel leech completed")
            LOGGER.info(f"[FINAL-DEBUG] Total files processed: {processed}")
            LOGGER.info(f"[FINAL-DEBUG] Concurrent attempts: {self.concurrent_attempts}")
            LOGGER.info(f"[FINAL-DEBUG] Concurrent success rate: {success_rate:.1f}%")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _concurrent_worker(self, listener, semaphore):
        """Worker with detailed debugging and error handling"""
        async with semaphore:
            if self.is_cancelled:
                return False
            
            # 🔍 DEBUG: Log worker start
            LOGGER.info(f"[WORKER-DEBUG] Starting worker for task {listener.mid}, MSG {listener.message_to_leech.id}")
            
            if self.TASK_START_DELAY > 0:
                LOGGER.info(f"[WORKER-DEBUG] Task {listener.mid} waiting {self.TASK_START_DELAY}s...")
                await asyncio.sleep(self.TASK_START_DELAY)
            
            try:
                result = await listener.run()
                LOGGER.info(f"[WORKER-SUCCESS] Task {listener.mid} completed successfully")
                return result
                
            except Exception as e:
                LOGGER.error(f"[WORKER-ERROR] Task {listener.mid} failed: {str(e)}")
                return False

    def _parse_arguments(self, args):
        parsed = {}
        i = 0
        while i < len(args):
            if args[i] == '-ch' and i + 1 < len(args):
                parsed['channel'] = args[i + 1]
                i += 2
            elif args[i] == '-f' and i + 1 < len(args):
                parsed['filter'] = args[i + 1].split()
                i += 2
            elif args[i] == '--no-caption':
                parsed['no_caption'] = True
                i += 1
            elif args[i] == '--sequential':
                parsed['sequential'] = True
                i += 1
            else:
                i += 1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True
        LOGGER.info(f"[MAIN-DEBUG] Channel leech task cancelled for {self.channel_id}")

@new_task
async def channel_leech_cmd(client, message):
    """Handle /cleech command with comprehensive debugging"""
    await ChannelLeech(client, message).new_event()

# Register handler
bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
