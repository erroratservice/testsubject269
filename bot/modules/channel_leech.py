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
    filename = re.sub(r'[<>:"/\|?*]', '', filename)
    filename = re.sub(r'[^\w\s.-]', '', filename)
    filename = re.sub(r'\s+', ' ', filename)
    filename = filename.strip()
    
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename

class ConcurrentChannelLeech(TaskListener):
    """A dedicated, isolated TaskListener for a single concurrent download."""
    
    def __init__(self, main_listener, message_to_leech, file_info):
        # Set essential attributes BEFORE calling super().__init__()
        self.message = main_listener.message
        self.client = main_listener.client
        self.use_caption_as_filename = main_listener.use_caption_as_filename
        self.message_to_leech = message_to_leech
        self.file_info = file_info
        
        # CRITICAL: Set leech mode BEFORE parent initialization
        self.is_leech = True
        self.is_mirror = False
        self.is_clone = False
        
        # Clear any cloud upload paths
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        # Initialize parent class
        super().__init__()
        
        # Copy user settings from main listener using the same three-tier fallback
        self._copy_user_settings_from_main_listener(main_listener)
    
    def _copy_user_settings_from_main_listener(self, main_listener):
        """Copy user settings from main listener with proper three-tier fallback system"""
        user_dict = getattr(main_listener, 'user_dict', {})
        
        LOGGER.info(f"=== COPYING USER SETTINGS TO CONCURRENT WORKER ===")
        
        # Split size with fallback
        if user_dict.get("split_size", False):
            self.split_size = user_dict["split_size"]
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
        
        # Upload as document with fallback
        if (user_dict.get("as_doc", False) or 
            "as_doc" not in user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        
        # Leech destination with fallback
        if user_dict.get("leech_dest", False):
            self.leech_dest = user_dict["leech_dest"]
        elif "leech_dest" not in user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        
        # Media group with fallback
        if (user_dict.get("media_group", False) or 
            "media_group" not in user_dict and config_dict.get("MEDIA_GROUP", False)):
            self.media_group = True
        else:
            self.media_group = False
        
        # Equal splits with fallback
        if (user_dict.get("equal_splits", False) or 
            "equal_splits" not in user_dict and config_dict.get("EQUAL_SPLITS", False)):
            self.equal_splits = True
        else:
            self.equal_splits = False
        
        # Thumbnail with fallback
        thumbpath = f"Thumbnails/{self.user_id}.jpg"
        self.thumb = user_dict.get("thumb") or thumbpath
        
        # Leech prefix with fallback
        if user_dict.get("lprefix", False):
            self.lprefix = user_dict["lprefix"]
        elif "lprefix" not in user_dict and config_dict.get("LEECH_FILENAME_PREFIX"):
            self.lprefix = config_dict["LEECH_FILENAME_PREFIX"]
        else:
            self.lprefix = None
        
        # CRITICAL: Force leech mode and set upload destination correctly
        self.is_leech = True
        self.up_dest = self.leech_dest  # Set to leech destination, not None
        
        LOGGER.info(f"Concurrent worker settings - is_leech: {self.is_leech}, up_dest: {self.up_dest}, as_doc: {self.as_doc}")
    
    async def run(self):
        """Executes the download and upload for a single file."""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        os.makedirs(download_path, exist_ok=True)
        
        # Generate filename and caption
        final_filename, new_caption = self._generate_file_details(self.message_to_leech, self.file_info)
        
        # Set name and caption for the TaskListener pipeline
        self.name = final_filename
        self.caption = new_caption
        
        # Verify leech settings before download
        LOGGER.info(f"Starting concurrent download - is_leech: {self.is_leech}, name: {self.name}")
        
        # Start download using TelegramDownloadHelper
        telegram_helper = TelegramDownloadHelper(self)
        await telegram_helper.add_download(
            self.message_to_leech, 
            download_path, 
            'user',  # Use user session
            name=self.name, 
            caption=self.caption
        )
        return True
    
    def _generate_file_details(self, message, file_info):
        """Generate filename and caption based on user preferences"""
        final_filename = file_info['file_name']
        new_caption = None
        
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            if first_line:
                clean_name = sanitize_filename(first_line)
                if clean_name and len(clean_name) >= 3:
                    original_extension = os.path.splitext(file_info['file_name'])[1]
                    final_filename = clean_name
                    if not final_filename.lower().endswith(original_extension.lower()):
                        final_filename += original_extension
                    new_caption = os.path.splitext(final_filename)[0]
        
        return final_filename, new_caption

class ChannelLeech(TaskListener):
    CONCURRENCY = 4
    TASK_START_DELAY = 2
    
    def __init__(self, client, message):
        # Set attributes BEFORE calling super().__init__()
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.concurrent_enabled = True
        
        # Critical: Set is_leech BEFORE calling super().__init__()
        self.is_leech = True
        self.is_mirror = False
        self.is_clone = False
        
        # Clear cloud upload paths to force Telegram upload
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None  # Will be set by user settings fallback
        
        # Now call parent constructor
        super().__init__()
        
        # Apply user settings with proper three-tier fallback system
        self._apply_user_settings_with_fallbacks()
    
    def _apply_user_settings_with_fallbacks(self):
        """Apply user settings using the same three-tier fallback system as usersetting.py"""
        user_dict = getattr(self, 'user_dict', {})
        
        LOGGER.info("=== APPLYING USER SETTINGS WITH FALLBACKS ===")
        LOGGER.info(f"Raw user_dict: {user_dict}")
        
        # Split size with fallback
        if user_dict.get("split_size", False):
            self.split_size = user_dict["split_size"]
            LOGGER.info(f"Split size from user_dict: {self.split_size}")
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
            LOGGER.info(f"Split size from config fallback: {self.split_size}")
        
        # Upload as document with fallback
        if (user_dict.get("as_doc", False) or 
            "as_doc" not in user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        LOGGER.info(f"Upload as document: {self.as_doc}")
        
        # Leech destination with fallback
        if user_dict.get("leech_dest", False):
            self.leech_dest = user_dict["leech_dest"]
        elif "leech_dest" not in user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        LOGGER.info(f"Leech destination: {self.leech_dest}")
        
        # Media group with fallback
        if (user_dict.get("media_group", False) or 
            "media_group" not in user_dict and config_dict.get("MEDIA_GROUP", False)):
            self.media_group = True
        else:
            self.media_group = False
        LOGGER.info(f"Media group: {self.media_group}")
        
        # Equal splits with fallback
        if (user_dict.get("equal_splits", False) or 
            "equal_splits" not in user_dict and config_dict.get("EQUAL_SPLITS", False)):
            self.equal_splits = True
        else:
            self.equal_splits = False
        LOGGER.info(f"Equal splits: {self.equal_splits}")
        
        # Thumbnail with fallback
        thumbpath = f"Thumbnails/{self.user_id}.jpg"
        self.thumb = user_dict.get("thumb") or thumbpath
        LOGGER.info(f"Thumbnail path: {self.thumb}")
        
        # Leech prefix with fallback
        if user_dict.get("lprefix", False):
            self.lprefix = user_dict["lprefix"]
        elif "lprefix" not in user_dict and config_dict.get("LEECH_FILENAME_PREFIX"):
            self.lprefix = config_dict["LEECH_FILENAME_PREFIX"]
        else:
            self.lprefix = None
        LOGGER.info(f"Leech prefix: {self.lprefix}")
        
        # Force leech mode and ensure proper upload destination
        self.is_leech = True
        self.up_dest = self.leech_dest  # Set upload destination
        
        LOGGER.info("=== USER SETTINGS APPLIED WITH FALLBACKS ===")
    
    async def new_event(self):
        """Main channel leech event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        
        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption] [--sequential]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch -1001234567890 -f movie --no-caption`\n"
                "`/cleech -ch @channel --sequential`\n\n"
                "**Features:**\n"
                "• Concurrent downloads by default (4 parallel)\n"
                "• Files uploaded with caption first line as filename (emoji-free)\n"
                "• Use `--no-caption` to keep original filenames\n"
                "• Use `--sequential` for one-by-one processing\n"
                "• Respects all your user settings with proper fallbacks"
            )
            await send_message(self.message, usage_text)
            return
        
        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.concurrent_enabled = not args.get('sequential', False)
        
        if not user:
            await send_message(self.message, "User session is required for channel access!")
            return
        
        # Start operation tracking
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech"
        )
        
        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "caption as filename" if self.use_caption_as_filename else "original filenames"
        mode = f"Concurrent ({self.CONCURRENCY} parallel)" if self.concurrent_enabled else "Sequential"
        
        self.status_message = await send_message(
            self.message, 
            f"Starting channel leech `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Mode:** {mode}\n"
            f"**Filename mode:** {caption_mode}\n"
            f"**Split size:** {self.split_size} bytes\n"
            f"**As document:** {self.as_doc}\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )
        
        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"Channel leech error: {e}")
            await edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)
    
    async def _process_channel(self):
        """Process channel messages with concurrent or sequential downloads"""
        tasks = []
        semaphore = asyncio.Semaphore(self.CONCURRENCY)
        processed, downloaded, skipped, errors = 0, 0, 0, 0
        
        try:
            chat = await user.get_chat(self.channel_id)
            caption_info = "with clean caption filenames" if self.use_caption_as_filename else "with original filenames"
            mode_text = f"Concurrent ({self.CONCURRENCY} parallel)" if self.concurrent_enabled else "Sequential"
            
            await edit_message(
                self.status_message, 
                f"Processing channel: **{chat.title}**\n"
                f"Mode: **{mode_text}** {caption_info}\n"
                f"Scanning messages..."
            )
            
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("Channel leech cancelled by user")
                    break
                
                processed += 1
                await channel_status.update_operation(
                    self.operation_key, processed=processed
                )
                
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                # Apply filters if specified
                if self.filter_tags:
                    search_text = file_info['search_text'].lower()
                    if not all(tag.lower() in search_text for tag in self.filter_tags):
                        continue
                
                # Check for duplicates
                exists = await database.check_file_exists(
                    file_info.get('file_unique_id'),
                    file_info.get('file_hash'),
                    file_info.get('file_name')
                )
                if exists:
                    skipped += 1
                    await channel_status.update_operation(
                        self.operation_key, skipped=skipped
                    )
                    continue
                
                # Create concurrent listener instance
                concurrent_listener = ConcurrentChannelLeech(self, message, file_info)
                
                if self.concurrent_enabled:
                    # Concurrent mode
                    task = asyncio.create_task(
                        self._concurrent_worker(concurrent_listener, semaphore)
                    )
                    tasks.append((task, message, file_info))
                else:
                    # Sequential mode
                    try:
                        await self._concurrent_worker(concurrent_listener, semaphore)
                        downloaded += 1
                        await database.add_file_entry(
                            self.channel_id, message.id, file_info
                        )
                        await channel_status.update_operation(
                            self.operation_key, downloaded=downloaded
                        )
                    except Exception as e:
                        errors += 1
                        LOGGER.error(f"Sequential task for message {message.id} failed: {e}")
                        await channel_status.update_operation(
                            self.operation_key, errors=errors
                        )
            
            # Wait for concurrent tasks to complete
            if self.concurrent_enabled and tasks:
                LOGGER.info(f"Waiting for {len(tasks)} concurrent tasks to complete...")
                results = await asyncio.gather(*[task[0] for task in tasks], return_exceptions=True)
                
                for i, (result, message, file_info) in enumerate(zip(results, [t[1:] for t in tasks])):
                    if isinstance(result, Exception):
                        errors += 1
                        LOGGER.error(f"Concurrent task failed: {result}")
                    else:
                        downloaded += 1
                        await database.add_file_entry(
                            self.channel_id, message[0].id, file_info
                        )
                
                await channel_status.update_operation(
                    self.operation_key, downloaded=downloaded, errors=errors
                )
            
            final_text = (
                f"**Channel leech completed!**\n\n"
                f"**Total processed:** {processed}\n"
                f"**Downloaded:** {downloaded}\n"
                f"**Skipped (duplicates):** {skipped}\n"
                f"**Errors:** {errors}\n\n"
                f"**Channel:** `{self.channel_id}`\n"
                f"**Mode:** {mode_text}"
            )
            await edit_message(self.status_message, final_text)
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")
    
    async def _concurrent_worker(self, listener, semaphore):
        """Worker function for concurrent downloads"""
        async with semaphore:
            if self.is_cancelled:
                return
            
            if self.TASK_START_DELAY > 0:
                await asyncio.sleep(self.TASK_START_DELAY)
            
            # Execute download and upload through TaskListener pipeline
            await listener.run()
    
    def _parse_arguments(self, args):
        """Parse command arguments"""
        parsed = {}
        i = 0
        
        while i < len(args):
            if args[i] == '-ch' and i + 1 < len(args):
                parsed['channel'] = args[i + 1]
                i += 2
            elif args[i] == '-f' and i + 1 < len(args):
                filter_text = args[i + 1]
                if filter_text.startswith('"') and filter_text.endswith('"'):
                    filter_text = filter_text[1:-1]
                parsed['filter'] = filter_text.split()
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
        """Cancel the channel leech task"""
        self.is_cancelled = True
        LOGGER.info(f"Channel leech task cancelled for {self.channel_id}")

class ChannelScanListener(TaskListener):
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.scanner = None
        super().__init__()
    
    async def new_event(self):
        """Handle scan command with task ID assignment"""
        text = self.message.text.split()
        
        if len(text) < 2:
            usage_text = (
                "**Usage:** `/scan <channel_id> [filter]`\n\n"
                "**Examples:**\n"
                "`/scan @my_channel`\n"
                "`/scan -1001234567890`\n"
                "`/scan @movies_channel movie`\n\n"
                "**Purpose:** Build file database for duplicate detection"
            )
            await send_message(self.message, usage_text)
            return
        
        self.channel_id = text[1]
        self.filter_tags = text[2:] if len(text) > 2 else []
        
        if not user:
            await send_message(self.message, "User session is required for channel scanning!")
            return
        
        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        status_msg = await send_message(
            self.message, 
            f"Starting scan `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )
        
        try:
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
            
        except Exception as e:
            LOGGER.error(f"Channel scan error: {e}")
            await edit_message(status_msg, f"Scan failed: {str(e)}")
    
    def cancel_task(self):
        """Cancel the scan operation"""
        self.is_cancelled = True
        if self.scanner:
            self.scanner.running = False
        LOGGER.info(f"Channel scan cancelled for {self.channel_id}")

@new_task
async def channel_scan(client, message):
    """Handle /scan command with task ID support"""
    await ChannelScanListener(user, message).new_event()

@new_task
async def channel_leech_cmd(client, message):
    """Handle /cleech command - concurrent with proper user settings fallback"""
    await ChannelLeech(client, message).new_event()

# Register handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
