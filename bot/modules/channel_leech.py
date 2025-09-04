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
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'[^\w\s.-]', '', filename)
    filename = re.sub(r'\s+', ' ', filename)
    filename = filename.strip()
    
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename

class ChannelLeech(TaskListener):
    def __init__(self, client, message):
        # Set attributes BEFORE calling super().__init__()
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        
        # Critical: Set is_leech BEFORE calling super().__init__()
        self.is_leech = True
        
        # Clear cloud upload paths to force Telegram upload
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None  # Will be set by user settings fallback
        
        # Now call parent constructor - this loads user_dict properly
        super().__init__()
        
        # CRITICAL: Apply user settings with proper three-tier fallback system
        self._apply_user_settings_with_fallbacks()

    def _apply_user_settings_with_fallbacks(self):
        """Apply user settings using the same three-tier fallback system as usersetting.py"""
        user_dict = getattr(self, 'user_dict', {})
        
        LOGGER.info("=== APPLYING USER SETTINGS WITH FALLBACKS ===")
        LOGGER.info(f"Raw user_dict: {user_dict}")
        
        # Split size with fallback (same logic as usersetting.py)
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
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch -1001234567890 -f movie --no-caption`\n\n"
                "**Features:**\n"
                "• Files uploaded with caption first line as filename (emoji-free)\n"
                "• Use `--no-caption` to keep original filenames\n"
                "• Respects all your user settings with proper fallbacks"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)

        if not user:
            await send_message(self.message, "User session is required for channel access!")
            return

        # Start operation tracking
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "caption as filename" if self.use_caption_as_filename else "original filenames"
        
        self.status_message = await send_message(
            self.message, 
            f"Starting channel leech `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Upload:** TaskListener Pipeline (Unique Filenames)\n"
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
        """Process channel messages and download files for automatic upload"""
        downloaded = 0
        skipped = 0
        processed = 0
        errors = 0
        batch_count = 0
        batch_sleep = 3
        message_sleep = 0.1

        try:
            chat = await user.get_chat(self.channel_id)
            caption_info = "with unique caption filenames" if self.use_caption_as_filename else "with original filenames"
            
            await edit_message(
                self.status_message, 
                f"Processing channel: **{chat.title}**\n"
                f"Scanning messages...\n"
                f"Upload: **Unique Filename System** {caption_info}"
            )

            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)

            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("Channel leech cancelled by user")
                    break

                processed += 1
                batch_count += 1

                await channel_status.update_operation(
                    self.operation_key, processed=processed
                )

                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue

                if self.filter_tags:
                    search_text = file_info['search_text'].lower()
                    if not all(tag.lower() in search_text for tag in self.filter_tags):
                        continue

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

                try:
                    await self._download_file_tasklistener_pipeline(message, file_info)
                    downloaded += 1

                    await database.add_file_entry(
                        self.channel_id, message.id, file_info
                    )

                    await channel_status.update_operation(
                        self.operation_key, downloaded=downloaded
                    )

                except Exception as e:
                    errors += 1
                    LOGGER.error(f"Leech failed for {file_info['file_name']}: {e}")
                    await channel_status.update_operation(
                        self.operation_key, errors=errors
                    )

                await asyncio.sleep(message_sleep)

                if batch_count >= 20:
                    status_text = (
                        f"**Progress Update**\n"
                        f"Processed: {processed}\n"
                        f"Downloaded: {downloaded}\n"
                        f"Skipped: {skipped}\n"
                        f"Errors: {errors}\n"
                        f"Using: Unique Filename System"
                    )
                    await edit_message(self.status_message, status_text)
                    
                    LOGGER.info(f"Batch completed ({batch_count} messages), sleeping for {batch_sleep}s")
                    await asyncio.sleep(batch_sleep)
                    batch_count = 0

            final_text = (
                f"**Channel leech completed!**\n\n"
                f"**Total processed:** {processed}\n"
                f"**Downloaded:** {downloaded}\n"
                f"**Skipped (duplicates):** {skipped}\n"
                f"**Errors:** {errors}\n\n"
                f"**Channel:** `{self.channel_id}`\n"
                f"**System:** Unique Filenames Applied"
            )
            await edit_message(self.status_message, final_text)

        except FloodWait as e:
            LOGGER.warning(f"FloodWait during channel processing: {e.x}s")
            await edit_message(self.status_message, f"Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("Resuming channel processing after FloodWait")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _download_file_tasklistener_pipeline(self, message, file_info):
        """Download file with proper unique filename generation per file - WITH DEBUG"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        # 🔍 MINIMAL DEBUG: Log what we receive for this specific message
        LOGGER.info(f"[DEBUG] MSG_{message.id}: Original file_info name: {file_info['file_name']}")
        LOGGER.info(f"[DEBUG] MSG_{message.id}: Message caption: {getattr(message, 'caption', 'NO_CAPTION')}")
        
        # Generate completely fresh filename for EACH file
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            if first_line:
                # Start fresh - don't modify file_info directly
                clean_name = sanitize_filename(first_line)
                if clean_name and len(clean_name) >= 3:
                    # Get original extension
                    original_extension = os.path.splitext(file_info['file_name'])[1]
                    
                    # CRITICAL: Only add extension if not already present
                    if not clean_name.lower().endswith(original_extension.lower()):
                        clean_name = clean_name + original_extension
                    
                    # Add unique identifier to prevent collisions
                    unique_id = message.id
                    final_filename = f"{clean_name[:-len(original_extension)]}_{unique_id}{original_extension}"
                    
                    # 🔍 MINIMAL DEBUG: Log the generated filename for this message
                    LOGGER.info(f"[DEBUG] MSG_{message.id}: Generated final_filename: {final_filename}")
                    
                    LOGGER.info(f"Fresh filename: '{file_info['file_name']}' → '{final_filename}'")
                    
                    # Create a NEW file_info dict for this specific file
                    updated_file_info = file_info.copy()
                    updated_file_info['file_name'] = final_filename
                else:
                    # Use original name with unique ID
                    base_name = os.path.splitext(file_info['file_name'])[0]
                    extension = os.path.splitext(file_info['file_name'])[1]
                    updated_file_info = file_info.copy()
                    updated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
                    LOGGER.info(f"[DEBUG] MSG_{message.id}: Used original with ID: {updated_file_info['file_name']}")
            else:
                # Caption exists but first line is empty
                base_name = os.path.splitext(file_info['file_name'])[0]
                extension = os.path.splitext(file_info['file_name'])[1]
                updated_file_info = file_info.copy()
                updated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
                LOGGER.info(f"[DEBUG] MSG_{message.id}: Empty caption, using original with ID: {updated_file_info['file_name']}")
        else:
            # Use original filename with unique ID
            base_name = os.path.splitext(file_info['file_name'])[0]
            extension = os.path.splitext(file_info['file_name'])[1]
            updated_file_info = file_info.copy()
            updated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
            LOGGER.info(f"[DEBUG] MSG_{message.id}: No caption mode, using: {updated_file_info['file_name']}")
        
        # Create download directory
        os.makedirs(download_path, exist_ok=True)
        
        # Verify settings are still applied
        LOGGER.info(f"Pipeline settings check for {updated_file_info['file_name']}:")
        LOGGER.info(f"  is_leech: {self.is_leech}")
        LOGGER.info(f"  split_size: {self.split_size}")
        LOGGER.info(f"  as_doc: {getattr(self, 'as_doc', 'NOT_SET')}")
        LOGGER.info(f"  leech_dest: {getattr(self, 'leech_dest', 'NOT_SET')}")
        
        # Use TelegramDownloadHelper with updated file_info containing unique filename
        # This ensures each file gets its own unique name in the TaskListener pipeline
        telegram_helper = TelegramDownloadHelper(self)
        
        # Temporarily update the file info in the message for unique filename processing
        original_file_name = file_info['file_name']
        file_info['file_name'] = updated_file_info['file_name']
        
        try:
            await telegram_helper.add_download(user, message, download_path)
        finally:
            # Restore original filename to avoid affecting other operations
            file_info['file_name'] = original_file_name

    def _parse_arguments(self, args):
        """Parse command arguments including new --no-caption flag"""
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
    """Handle /cleech command - uses three-tier fallback system with unique filenames"""
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
