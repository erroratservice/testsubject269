from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from bot import bot, user, DOWNLOAD_DIR, LOGGER, user_data
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.ext_utils.files_utils import clean_download
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
from ..helper.mirror_leech_utils.download_utils.telegram_download import TelegramDownloadHelper
from ..helper.listeners.task_listener import TaskListener
import asyncio
import os
import re
from aiofiles.os import path as aiopath, remove, makedirs

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

class ChannelLeech(TaskListener):
    CONCURRENCY = 4
    TASK_START_DELAY = 2

    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.counters_lock = asyncio.Lock()
        self.processed = 0
        self.downloaded = 0
        self.skipped = 0
        self.errors = 0
        self.is_leech = True
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        super().__init__()
        self._apply_user_settings_with_fallbacks()

    def _apply_user_settings_with_fallbacks(self):
        from bot import config_dict
        user_dict = getattr(self, 'user_dict', {})
        LOGGER.info("=== APPLYING USER SETTINGS WITH FALLBACKS ===")
        if user_dict.get("split_size", False):
            self.split_size = user_dict["split_size"]
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
        if (user_dict.get("as_doc", False) or "as_doc" not in user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        if user_dict.get("leech_dest", False):
            self.leech_dest = user_dict["leech_dest"]
        elif "leech_dest" not in user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        self.is_leech = True
        self.up_dest = self.leech_dest
        LOGGER.info("=== USER SETTINGS APPLIED WITH FALLBACKS ===")

    async def onUploadComplete(self, link, size, files, folders, mime_type, name, rclone_path=None):
        LOGGER.info(f"Channel Leech: Finished uploading {name}. Cleaning up individual file.")
        file_path = os.path.join(self.dir, name)
        if await aiopath.exists(file_path):
            try:
                await remove(file_path)
            except Exception as e:
                LOGGER.error(f"Failed to clean up file {file_path}: {e}")

    async def new_event(self):
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        if 'channel' not in args:
            await send_message(self.message, "Usage: /cleech -ch <channel_id> ...")
            return
        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        if not user:
            await send_message(self.message, "User session is required!")
            return
        self.operation_key = await channel_status.start_operation(self.message.from_user.id, self.channel_id, "channel_leech")
        self.status_message = await send_message(self.message, f"Starting channel leech `{str(self.mid)[:12]}`...")
        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"Channel leech error: {e}")
            await edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    def _generate_file_details(self, message, file_info):
        final_filename = file_info['file_name']
        new_caption = None
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            if first_line:
                clean_name = sanitize_filename(first_line)
                if clean_name:
                    original_extension = os.path.splitext(file_info['file_name'])[1]
                    final_filename = clean_name
                    if not final_filename.lower().endswith(original_extension.lower()):
                        final_filename += original_extension
                    new_caption = os.path.splitext(final_filename)[0]
        return final_filename, new_caption

    async def _worker(self, message, file_info, semaphore):
        async with semaphore:
            if self.is_cancelled:
                return
            
            # Stagger the start of each download task
            if self.TASK_START_DELAY > 0:
                await asyncio.sleep(self.TASK_START_DELAY)

            try:
                final_filename, new_caption = self._generate_file_details(message, file_info)
                
                # Create a fresh helper instance for each task to ensure state isolation
                telegram_helper = TelegramDownloadHelper(self)
                
                # Pass name and caption directly to avoid race conditions
                await telegram_helper.add_download(message, self.dir, self.user_id, name=final_filename, caption=new_caption)
                
                async with self.counters_lock:
                    self.downloaded += 1
                await channel_status.update_operation(self.operation_key, downloaded=self.downloaded)
                await database.add_file_entry(self.channel_id, message.id, file_info)

            except Exception as e:
                async with self.counters_lock:
                    self.errors += 1
                await channel_status.update_operation(self.operation_key, errors=self.errors)
                LOGGER.error(f"Leech failed for {file_info['file_name']}: {e}")

    async def _process_channel(self):
        tasks = []
        semaphore = asyncio.Semaphore(self.CONCURRENCY)
        self.dir = f"{DOWNLOAD_DIR}{self.mid}"
        await makedirs(self.dir, exist_ok=True)

        try:
            chat = await user.get_chat(self.channel_id)
            await edit_message(self.status_message, f"Processing channel: **{chat.title}** with **{self.CONCURRENCY}** concurrent downloads...")
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)

            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled: break
                self.processed += 1
                await channel_status.update_operation(self.operation_key, processed=self.processed)
                file_info = await scanner._extract_file_info(message)
                if not file_info: continue
                if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags): continue
                if await database.check_file_exists(file_info.get('file_unique_id')):
                    self.skipped += 1
                    await channel_status.update_operation(self.operation_key, skipped=self.skipped)
                    continue
                task = asyncio.create_task(self._worker(message, file_info, semaphore))
                tasks.append(task)
            
            if tasks: await asyncio.gather(*tasks)
            final_text = (f"**Channel leech completed!**\n\n"
                          f"**Processed:** {self.processed}, **Downloaded:** {self.downloaded}, "
                          f"**Skipped:** {self.skipped}, **Errors:** {self.errors}")
            await edit_message(self.status_message, final_text)

        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")
        finally:
            await clean_download(self.dir)

    def _parse_arguments(self, args):
        parsed = {}
        i = 0
        while i < len(args):
            if args[i] == '-ch' and i + 1 < len(args):
                parsed['channel'] = args[i + 1]; i += 2
            elif args[i] == '-f' and i + 1 < len(args):
                parsed['filter'] = args[i + 1].split(); i += 2
            elif args[i] == '--no-caption':
                parsed['no_caption'] = True; i += 1
            else: i += 1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True
        LOGGER.info(f"Channel leech task cancelled for {self.channel_id}")

# --- Channel Scanning System (Unchanged) ---
class ChannelScanListener(TaskListener):
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.scanner = None
        super().__init__()

    async def new_event(self):
        text = self.message.text.split()
        if len(text) < 2:
            await send_message(self.message, "Usage: /scan <channel_id> [filter]")
            return
        self.channel_id = text[1]
        self.filter_tags = text[2:] if len(text) > 2 else []
        if not user:
            await send_message(self.message, "User session is required!")
            return
        status_msg = await send_message(self.message, f"Starting scan `{str(self.mid)[:12]}`...")
        try:
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
        except Exception as e:
            await edit_message(status_msg, f"Scan failed: {str(e)}")

    def cancel_task(self):
        self.is_cancelled = True
        if self.scanner: self.scanner.running = False
        LOGGER.info(f"Channel scan cancelled for {self.channel_id}")

@new_task
async def channel_scan(client, message):
    await ChannelScanListener(user, message).new_event()

@new_task
async def channel_leech_cmd(client, message):
    await ChannelLeech(client, message).new_event()

bot.add_handler(MessageHandler(channel_scan, filters=command("scan") & CustomFilters.authorized))
bot.add_handler(MessageHandler(channel_leech_cmd, filters=command("cleech") & CustomFilters.authorized))
