from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from bot import bot, user, DOWNLOAD_DIR, LOGGER, user_data

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

async def get_user_data(user_id):
    """Get user settings from the global user_data dictionary"""
    if user_id in user_data:
        return user_data[user_id]
    return {}

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
        
        # FORCE LEECH MODE - Always upload to Telegram with user settings
        self.is_leech = True
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        
        # Now call parent constructor
        super().__init__()

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
                "• Upload respects your user settings (thumbnails, destination, etc.)"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)

        if not user:
            await send_message(self.message, "❌ User session is required for channel access!")
            return

        # Start operation tracking
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "caption as filename" if self.use_caption_as_filename else "original filenames"
        
        self.status_message = await send_message(
            self.message, 
            f"🔄 **Starting channel leech** `{str(self.mid)[:12]}`\n"
            f"📋 **Channel:** `{self.channel_id}`{filter_text}\n"
            f"📤 **Upload to:** Telegram (with user settings)\n"
            f"📝 **Filename mode:** {caption_mode}\n"
            f"⏹️ **Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"Channel leech error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _process_channel(self):
        """Process channel messages and download files for Telegram upload"""
        downloaded = 0
        skipped = 0
        processed = 0
        errors = 0
        batch_count = 0
        batch_sleep = 3
        message_sleep = 0.1

        try:
            chat = await user.get_chat(self.channel_id)
            caption_info = "with caption filenames" if self.use_caption_as_filename else "with original filenames"
            
            await edit_message(
                self.status_message, 
                f"📋 Processing channel: **{chat.title}**\n"
                f"🔍 Scanning messages...\n"
                f"📤 Upload: **Telegram** {caption_info}"
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
                    await self._leech_file_with_user_settings(message, file_info)
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
                        f"📊 **Progress Update**\n"
                        f"📋 Processed: {processed}\n"
                        f"⬇️ Downloaded: {downloaded}\n"
                        f"⏭️ Skipped: {skipped}\n"
                        f"❌ Errors: {errors}\n"
                        f"📝 Using: {'Caption filenames' if self.use_caption_as_filename else 'Original filenames'}"
                    )
                    await edit_message(self.status_message, status_text)
                    
                    LOGGER.info(f"Batch completed ({batch_count} messages), sleeping for {batch_sleep}s")
                    await asyncio.sleep(batch_sleep)
                    batch_count = 0

            final_text = (
                f"✅ **Channel leech completed!**\n\n"
                f"📋 **Total processed:** {processed}\n"
                f"⬇️ **Downloaded:** {downloaded}\n"
                f"⏭️ **Skipped (duplicates):** {skipped}\n"
                f"❌ **Errors:** {errors}\n\n"
                f"🎯 **Channel:** `{self.channel_id}`\n"
                f"📝 **Filename mode:** {'Caption as filename' if self.use_caption_as_filename else 'Original filenames'}"
            )
            await edit_message(self.status_message, final_text)

        except FloodWait as e:
            LOGGER.warning(f"FloodWait during channel processing: {e.x}s")
            await edit_message(self.status_message, f"⏳ Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("Resuming channel processing after FloodWait")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _leech_file_with_user_settings(self, message, file_info):
        """Download file and apply user settings properly"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        LOGGER.info("=== LOADING USER SETTINGS (FIXED) ===")
        try:
            # Load user settings from global user_data dictionary
            user_settings = await get_user_data(self.message.from_user.id)
            LOGGER.info(f"User settings loaded: {user_settings}")
            
            if user_settings:
                LOGGER.info("=== USER SETTINGS BREAKDOWN ===")
                for key, value in user_settings.items():
                    LOGGER.info(f"  {key}: {value} (type: {type(value)})")
            else:
                LOGGER.warning("No user settings found - using defaults")
                
        except Exception as e:
            LOGGER.error(f"Error loading user settings: {e}")
            user_settings = {}
        
        # Apply user settings to TaskListener attributes
        LOGGER.info("=== APPLYING USER SETTINGS ===")
        
        # Force leech mode
        self.is_leech = True
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        
        # Apply user's split size (critical for preventing division by zero)
        if user_settings and 'split_size' in user_settings:
            self.split_size = user_settings['split_size']
            LOGGER.info(f"Applied user split_size: {self.split_size}")
        else:
            self.split_size = 2097152000  # 2GB default
            LOGGER.info(f"Applied default split_size: {self.split_size}")
        
        # Apply user's upload destination
        if user_settings and 'leech_dest' in user_settings:
            self.leech_dest = user_settings['leech_dest']
            LOGGER.info(f"Applied user leech_dest: {self.leech_dest}")
        
        # Apply user's document preference
        if user_settings and 'as_doc' in user_settings:
            self.upload_as_doc = user_settings['as_doc']
            LOGGER.info(f"Applied user as_doc: {self.upload_as_doc}")
        
        # Apply user's thumbnail
        if user_settings and 'thumb' in user_settings:
            self.thumb_path = user_settings['thumb']
            LOGGER.info(f"Applied user thumb: {self.thumb_path}")
        
        # Apply other user settings
        if user_settings:
            self.equal_splits = user_settings.get('equal_splits', False)
            self.media_group = user_settings.get('media_group', False)
            LOGGER.info(f"Applied equal_splits: {self.equal_splits}")
            LOGGER.info(f"Applied media_group: {self.media_group}")
        
        # Process caption-based filename if enabled
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            LOGGER.info("=== CAPTION FILENAME PROCESSING ===")
            first_line = message.caption.split('\n')[0].strip()
            LOGGER.info(f"Caption first line: '{first_line}'")
            
            if first_line:
                clean_name = sanitize_filename(first_line)
                LOGGER.info(f"Cleaned name: '{clean_name}'")
                
                if clean_name and len(clean_name) >= 3:
                    original_name = file_info['file_name']
                    if '.' in original_name:
                        extension = '.' + original_name.split('.')[-1]
                    else:
                        extension = ''
                    
                    new_filename = clean_name + extension
                    LOGGER.info(f"Filename change: '{original_name}' → '{new_filename}'")
                    file_info['file_name'] = new_filename
                    file_info['caption_first_line'] = first_line
        
        # Create download directory
        os.makedirs(download_path, exist_ok=True)
        
        # Final settings check
        LOGGER.info("=== FINAL SETTINGS VERIFICATION ===")
        LOGGER.info(f"split_size: {getattr(self, 'split_size', 'NOT_SET')}")
        LOGGER.info(f"is_leech: {getattr(self, 'is_leech', 'NOT_SET')}")
        LOGGER.info(f"leech_dest: {getattr(self, 'leech_dest', 'NOT_SET')}")
        LOGGER.info(f"upload_as_doc: {getattr(self, 'upload_as_doc', 'NOT_SET')}")
        
        LOGGER.info(f"Starting Telegram leech with proper user settings for: {file_info['file_name']}")
        
        # Use TelegramDownloadHelper with corrected settings
        telegram_helper = TelegramDownloadHelper(self)
        await telegram_helper.add_download(message, download_path, "user")

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
            await send_message(self.message, "❌ User session is required for channel scanning!")
            return

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        status_msg = await send_message(
            self.message, 
            f"🔍 **Starting scan** `{str(self.mid)[:12]}`\n"
            f"📋 **Channel:** `{self.channel_id}`{filter_text}\n"
            f"⏹️ **Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
            
        except Exception as e:
            LOGGER.error(f"Channel scan error: {e}")
            await edit_message(status_msg, f"❌ Scan failed: {str(e)}")

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
    """Handle /cleech command - Telegram leech with proper user settings"""
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
