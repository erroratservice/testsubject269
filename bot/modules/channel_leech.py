from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from copy import deepcopy
import asyncio
import os
import re

from bot import bot, user, DOWNLOAD_DIR, LOGGER, config_dict
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
from ..helper.mirror_leech_utils.download_utils.telegram_download import TelegramDownloadHelper
from ..helper.listeners.task_listener import TaskListener

def remove_emoji(text):
    """Remove emojis and special characters from text"""
    emoji_pattern = re.compile(
        '['
        '\U0001F600-\U0001F64F'
        '\U0001F300-\U0001F5FF'
        '\U0001F680-\U0001F6FF'
        '\U0001F1E0-\U0001F1FF'
        '\U00002702-\U000027B0'
        '\U000024C2-\U0001F251'
        '\U0001F900-\U0001F9FF'
        '\U0001FA00-\U0001FA6F'
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
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        
        # Set is_leech BEFORE calling super().__init__()
        self.is_leech = True
        
        # Clear cloud upload paths to force Telegram upload
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        # Call parent constructor
        super().__init__()
        
        # Apply user settings
        self._apply_user_settings()

    def _apply_user_settings(self):
        """Apply user settings with fallback system"""
        user_dict = getattr(self, 'user_dict', {})
        
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
        
        # Force leech mode
        self.is_leech = True
        self.up_dest = self.leech_dest

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
            f"**Upload:** TaskListener Pipeline\n"
            f"**Filename mode:** {caption_mode}\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error("Channel leech error: %s", e)
            await edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _process_channel(self):
        """Process channel messages and download files"""
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
                f"Upload: **TaskListener Pipeline** {caption_info}"
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
                    await self._download_file_simple(message, file_info)
                    downloaded += 1

                    await database.add_file_entry(
                        self.channel_id, message.id, file_info
                    )

                    await channel_status.update_operation(
                        self.operation_key, downloaded=downloaded
                    )

                except Exception as e:
                    errors += 1
                    LOGGER.error("Download failed for %s: %s", file_info['file_name'], e)
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
                        f"Errors: {errors}"
                    )
                    await edit_message(self.status_message, status_text)
                    
                    LOGGER.info("Batch completed (%s messages), sleeping for %s seconds", batch_count, batch_sleep)
                    await asyncio.sleep(batch_sleep)
                    batch_count = 0

            final_text = (
                f"**Channel leech completed!**\n\n"
                f"**Total processed:** {processed}\n"
                f"**Downloaded:** {downloaded}\n"
                f"**Skipped (duplicates):** {skipped}\n"
                f"**Errors:** {errors}\n\n"
                f"**Channel:** `{self.channel_id}`"
            )
            await edit_message(self.status_message, final_text)

        except FloodWait as e:
            LOGGER.warning("FloodWait during channel processing: %s seconds", e.x)
            await edit_message(self.status_message, f"Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("Resuming channel processing after FloodWait")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _download_file_simple(self, message, file_info):
        """Simple download with unique filename generation"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        # Create isolated copy for this file
        local_file_info = deepcopy(file_info)
        
        # Generate unique filename
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            if first_line and len(first_line) >= 3:
                clean_name = sanitize_filename(first_line)
                if clean_name and len(clean_name) >= 3:
                    original_extension = os.path.splitext(local_file_info['file_name'])[1]
                    unique_filename = f"{clean_name}_{message.id}{original_extension}"
                    local_file_info['file_name'] = unique_filename
                    LOGGER.info("Using caption filename: %s -> %s", file_info['file_name'], unique_filename)
                else:
                    # Use original with message ID
                    base_name = os.path.splitext(local_file_info['file_name'])[0]
                    extension = os.path.splitext(local_file_info['file_name'])[1]
                    local_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
            else:
                # Use original with message ID
                base_name = os.path.splitext(local_file_info['file_name'])[0]
                extension = os.path.splitext(local_file_info['file_name'])[1]
                local_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
        else:
            # No caption mode - use original filename with unique ID
            base_name = os.path.splitext(local_file_info['file_name'])[0]
            extension = os.path.splitext(local_file_info['file_name'])[1]
            local_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
        
        # Create download directory
        os.makedirs(download_path, exist_ok=True)
        
        # Create download helper
        telegram_helper = TelegramDownloadHelper(self)
        
        # Update file_info temporarily for download
        original_filename = file_info['file_name']
        file_info['file_name'] = local_file_info['file_name']
        
        try:
            # Simple download call
            await telegram_helper.add_download(user, message, download_path)
            LOGGER.info("Download completed: %s", local_file_info['file_name'])
            
        except Exception as e:
            LOGGER.error("Download failed: %s", e)
            raise
        finally:
            # Restore original filename
            file_info['file_name'] = original_filename

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
            else:
                i += 1
                
        return parsed

    def cancel_task(self):
        """Cancel the channel leech task"""
        self.is_cancelled = True
        LOGGER.info("Channel leech task cancelled for %s", self.channel_id)

@new_task
async def channel_leech_cmd(client, message):
    """Handle /cleech command - Simple working version"""
    await ChannelLeech(client, message).new_event()

# Register handlers
bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
