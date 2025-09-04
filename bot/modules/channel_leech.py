from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from copy import deepcopy
from aiofiles.os import path as aiopath
import asyncio
import os
import re

from bot import bot, user, DOWNLOAD_DIR, LOGGER, user_data, config_dict

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
        self.batch_counter = 0
        
        # ✅ Critical: Set is_leech BEFORE calling super().__init__()
        self.is_leech = True
        
        # ✅ Clear cloud upload paths to force Telegram upload
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        # Now call parent constructor - this loads user_dict properly
        super().__init__()
        
        # ✅ Apply user settings with proper three-tier fallback system
        self._apply_user_settings_with_fallbacks()

    def _apply_user_settings_with_fallbacks(self):
        """Apply user settings using the same three-tier fallback system as usersetting.py"""
        user_dict = getattr(self, 'user_dict', {})
        
        LOGGER.info("=== DEBUG: APPLYING USER SETTINGS WITH FALLBACKS ===")
        LOGGER.info(f"🔍 Raw user_dict: {user_dict}")
        
        # ✅ Split size with fallback (same logic as usersetting.py)
        if user_dict.get("split_size", False):
            self.split_size = user_dict["split_size"]
            LOGGER.info(f"📏 Split size from user_dict: {self.split_size}")
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
            LOGGER.info(f"📏 Split size from config fallback: {self.split_size}")
        
        # ✅ Upload as document with fallback
        if (user_dict.get("as_doc", False) or 
            "as_doc" not in user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        LOGGER.info(f"📄 Upload as document: {self.as_doc}")
        
        # ✅ Leech destination with fallback
        if user_dict.get("leech_dest", False):
            self.leech_dest = user_dict["leech_dest"]
        elif "leech_dest" not in user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        LOGGER.info(f"🎯 Leech destination: {self.leech_dest}")
        
        # ✅ Media group with fallback
        if (user_dict.get("media_group", False) or 
            "media_group" not in user_dict and config_dict.get("MEDIA_GROUP", False)):
            self.media_group = True
        else:
            self.media_group = False
        LOGGER.info(f"📦 Media group: {self.media_group}")
        
        # ✅ Equal splits with fallback
        if (user_dict.get("equal_splits", False) or 
            "equal_splits" not in user_dict and config_dict.get("EQUAL_SPLITS", False)):
            self.equal_splits = True
        else:
            self.equal_splits = False
        LOGGER.info(f"⚖️ Equal splits: {self.equal_splits}")
        
        # ✅ Thumbnail with fallback
        thumbpath = f"Thumbnails/{self.user_id}.jpg"
        self.thumb = user_dict.get("thumb") or thumbpath
        LOGGER.info(f"🖼️ Thumbnail path: {self.thumb}")
        
        # ✅ Leech prefix with fallback
        if user_dict.get("lprefix", False):
            self.lprefix = user_dict["lprefix"]
        elif "lprefix" not in user_dict and config_dict.get("LEECH_FILENAME_PREFIX"):
            self.lprefix = config_dict["LEECH_FILENAME_PREFIX"]
        else:
            self.lprefix = None
        LOGGER.info(f"🏷️ Leech prefix: {self.lprefix}")
        
        # ✅ Force leech mode and ensure proper upload destination
        self.is_leech = True
        self.up_dest = self.leech_dest
        
        LOGGER.info("=== DEBUG: USER SETTINGS APPLIED WITH FALLBACKS ===")

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
                "• Enhanced debugging enabled\n"
                "• Complete state isolation per file\n"
                "• Unique filename generation with message ID"
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
            self.message.from_user.id, self.channel_id, "channel_leech_debug"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "caption as filename" if self.use_caption_as_filename else "original filenames"
        
        self.status_message = await send_message(
            self.message, 
            f"🔄 **DEBUG: Starting channel leech** `{str(self.mid)[:12]}`\n"
            f"📋 **Channel:** `{self.channel_id}`{filter_text}\n"
            f"📤 **Upload:** Enhanced Debug Mode\n"
            f"📝 **Filename mode:** {caption_mode}\n"
            f"⚙️ **Split size:** {self.split_size} bytes\n"
            f"📄 **As document:** {self.as_doc}\n"
            f"🎯 **Destination:** {self.leech_dest}\n"
            f"⏹️ **Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"❌ Channel leech error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _process_channel(self):
        """Process channel messages with complete state isolation per file"""
        downloaded = 0
        skipped = 0
        processed = 0
        errors = 0
        batch_count = 0
        batch_sleep = 3
        message_sleep = 0.5  # Increased to prevent state bleeding

        try:
            chat = await user.get_chat(self.channel_id)
            caption_info = "with unique caption filenames + msgID" if self.use_caption_as_filename else "with original filenames + msgID"
            
            await edit_message(
                self.status_message, 
                f"📋 Processing channel: **{chat.title}**\n"
                f"🔍 Scanning messages...\n"
                f"📤 Upload: **DEBUG MODE - State Isolation** {caption_info}"
            )

            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("❌ Channel leech cancelled by user")
                    break

                processed += 1
                batch_count += 1
                self.batch_counter += 1

                LOGGER.info(f"🔄 BATCH #{self.batch_counter}: Starting processing message {message.id}")

                await channel_status.update_operation(
                    self.operation_key, processed=processed
                )

                try:
                    # ✅ Create completely isolated scanner for THIS message only
                    isolated_scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
                    isolated_file_info = await isolated_scanner._extract_file_info(message)
                    
                    if not isolated_file_info:
                        LOGGER.info(f"⏭️ MSG_{message.id}: No file info, skipping")
                        continue

                    LOGGER.info(f"🔍 MSG_{message.id}: File info extracted - {isolated_file_info.get('file_name', 'unknown')}")

                    # Apply filters to THIS message only
                    if self.filter_tags:
                        search_text = isolated_file_info['search_text'].lower()
                        if not all(tag.lower() in search_text for tag in self.filter_tags):
                            LOGGER.info(f"⏭️ MSG_{message.id}: Failed filter check, skipping")
                            continue

                    # Check duplicates for THIS message only
                    exists = await database.check_file_exists(
                        isolated_file_info.get('file_unique_id'),
                        isolated_file_info.get('file_hash'),
                        isolated_file_info.get('file_name')
                    )

                    if exists:
                        LOGGER.info(f"⏭️ MSG_{message.id}: Duplicate file, skipping")
                        skipped += 1
                        await channel_status.update_operation(
                            self.operation_key, skipped=skipped
                        )
                        continue

                    LOGGER.info(f"🎯 MSG_{message.id}: Starting isolated processing")

                    # ✅ Process THIS message with completely isolated state
                    await self._download_file_with_complete_isolation(message, isolated_file_info)
                    
                    downloaded += 1
                    LOGGER.info(f"✅ MSG_{message.id}: Completed isolated processing")

                    await database.add_file_entry(
                        self.channel_id, message.id, isolated_file_info
                    )

                    await channel_status.update_operation(
                        self.operation_key, downloaded=downloaded
                    )

                except Exception as e:
                    errors += 1
                    LOGGER.error(f"❌ MSG_{message.id}: Processing failed: {e}")
                    await channel_status.update_operation(
                        self.operation_key, errors=errors
                    )

                # ✅ Critical: Sleep between messages to prevent state bleeding
                await asyncio.sleep(message_sleep)

                if batch_count >= 10:
                    status_text = (
                        f"📊 **DEBUG Progress Update**\n"
                        f"📋 Processed: {processed}\n"
                        f"⬇️ Downloaded: {downloaded}\n"
                        f"⏭️ Skipped: {skipped}\n"
                        f"❌ Errors: {errors}\n"
                        f"🔧 Using: State Isolation Debug Mode"
                    )
                    await edit_message(self.status_message, status_text)
                    
                    LOGGER.info(f"🛑 Batch completed ({batch_count} messages), sleeping for {batch_sleep}s")
                    await asyncio.sleep(batch_sleep)
                    batch_count = 0

            final_text = (
                f"✅ **DEBUG: Channel leech completed!**\n\n"
                f"📋 **Total processed:** {processed}\n"
                f"⬇️ **Downloaded:** {downloaded}\n"
                f"⏭️ **Skipped (duplicates):** {skipped}\n"
                f"❌ **Errors:** {errors}\n\n"
                f"🎯 **Channel:** `{self.channel_id}`\n"
                f"🔧 **System:** Complete State Isolation Applied"
            )
            await edit_message(self.status_message, final_text)

        except FloodWait as e:
            LOGGER.warning(f"⏳ FloodWait during channel processing: {e.x}s")
            await edit_message(self.status_message, f"⏳ Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("🔄 Resuming channel processing after FloodWait")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _download_file_with_complete_isolation(self, message, file_info):
        """Download file with complete state isolation and enhanced debugging"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        LOGGER.info(f"🚀 MSG_{message.id}: === STARTING COMPLETE ISOLATION ===")
        
        # ✅ Create completely isolated copy of file_info for THIS file only
        isolated_file_info = deepcopy(file_info)
        LOGGER.info(f"🔄 MSG_{message.id}: Created isolated file_info copy")
        
        # ✅ Generate unique filename using
