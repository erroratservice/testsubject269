from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from copy import deepcopy
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
    def __init__(self, client, message, sync_db=True):
        # Set attributes BEFORE calling super().__init__()
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.batch_counter = 0
        
        # TESTING FLAG: Set to False to disable database syncing during testing
        self.sync_db = sync_db
        
        # Critical: Set is_leech BEFORE calling super().__init__()
        self.is_leech = True
        
        # Clear cloud upload paths to force Telegram upload
        self.rclone_path = None
        self.gdrive_id = None
        self.drive_id = None
        self.folder_id = None
        self.up_dest = None
        
        # Now call parent constructor - this loads user_dict properly
        super().__init__()
        
        # Apply user settings with proper three-tier fallback system
        self._apply_user_settings_with_fallbacks()

    def _apply_user_settings_with_fallbacks(self):
        """Apply user settings using the same three-tier fallback system as usersetting.py"""
        user_dict = getattr(self, 'user_dict', {})
        
        LOGGER.info("=== DEBUG: APPLYING USER SETTINGS WITH FALLBACKS ===")
        LOGGER.info("Raw user_dict: %s", user_dict)
        LOGGER.info("DB Sync Mode: %s", 'ENABLED' if self.sync_db else 'DISABLED (Testing)')
        
        # Split size with fallback (same logic as usersetting.py)
        if user_dict.get("split_size", False):
            self.split_size = user_dict["split_size"]
            LOGGER.info("Split size from user_dict: %s", self.split_size)
        else:
            self.split_size = config_dict.get("LEECH_SPLIT_SIZE", 2097152000)
            LOGGER.info("Split size from config fallback: %s", self.split_size)
        
        # Upload as document with fallback
        if (user_dict.get("as_doc", False) or 
            "as_doc" not in user_dict and config_dict.get("AS_DOCUMENT", True)):
            self.as_doc = True
        else:
            self.as_doc = False
        LOGGER.info("Upload as document: %s", self.as_doc)
        
        # Leech destination with fallback
        if user_dict.get("leech_dest", False):
            self.leech_dest = user_dict["leech_dest"]
        elif "leech_dest" not in user_dict and config_dict.get("LEECH_DUMP_CHAT"):
            self.leech_dest = config_dict["LEECH_DUMP_CHAT"]
        else:
            self.leech_dest = None
        LOGGER.info("Leech destination: %s", self.leech_dest)
        
        # Media group with fallback
        if (user_dict.get("media_group", False) or 
            "media_group" not in user_dict and config_dict.get("MEDIA_GROUP", False)):
            self.media_group = True
        else:
            self.media_group = False
        LOGGER.info("Media group: %s", self.media_group)
        
        # Equal splits with fallback
        if (user_dict.get("equal_splits", False) or 
            "equal_splits" not in user_dict and config_dict.get("EQUAL_SPLITS", False)):
            self.equal_splits = True
        else:
            self.equal_splits = False
        LOGGER.info("Equal splits: %s", self.equal_splits)
        
        # Thumbnail with fallback
        thumbpath = f"Thumbnails/{self.user_id}.jpg"
        self.thumb = user_dict.get("thumb") or thumbpath
        LOGGER.info("Thumbnail path: %s", self.thumb)
        
        # Leech prefix with fallback
        if user_dict.get("lprefix", False):
            self.lprefix = user_dict["lprefix"]
        elif "lprefix" not in user_dict and config_dict.get("LEECH_FILENAME_PREFIX"):
            self.lprefix = config_dict["LEECH_FILENAME_PREFIX"]
        else:
            self.lprefix = None
        LOGGER.info("Leech prefix: %s", self.lprefix)
        
        # Force leech mode and ensure proper upload destination
        self.is_leech = True
        self.up_dest = self.leech_dest
        
        LOGGER.info("=== DEBUG: USER SETTINGS APPLIED WITH FALLBACKS ===")

    async def new_event(self):
        """Main channel leech event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])

        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption] [--test]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch -1001234567890 -f movie --no-caption`\n"
                "`/cleech -ch @movies_channel --test` (testing mode - no DB sync)\n\n"
                "**Features:**\n"
                "• Enhanced debugging enabled\n"
                "• Complete state isolation per file\n"
                "• Unique filename generation with message ID\n"
                "• `--test` flag disables database syncing for repeated testing"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        
        # Check for test flag in arguments
        if args.get('test', False):
            self.sync_db = False
            LOGGER.info("TEST MODE: Database syncing DISABLED")

        if not user:
            await send_message(self.message, "User session is required for channel access!")
            return

        # Start operation tracking
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech_testing"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "caption as filename" if self.use_caption_as_filename else "original filenames"
        test_mode = " [TEST MODE - NO DB SYNC]" if not self.sync_db else ""
        
        self.status_message = await send_message(
            self.message, 
            f"**TESTING: Starting channel leech** `{str(self.mid)[:12]}`{test_mode}\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Upload:** TaskListener Pipeline (No Tag)\n"
            f"**Filename mode:** {caption_mode}\n"
            f"**Split size:** {self.split_size} bytes\n"
            f"**As document:** {self.as_doc}\n"
            f"**Destination:** {self.leech_dest}\n"
            f"**DB Sync:** {'DISABLED' if not self.sync_db else 'ENABLED'}\n"
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
        """Process channel messages with complete state isolation per file"""
        downloaded = 0
        skipped = 0
        processed = 0
        errors = 0
        batch_count = 0
        batch_sleep = 3
        message_sleep = 0.5

        try:
            chat = await user.get_chat(self.channel_id)
            caption_info = "with unique caption filenames + msgID" if self.use_caption_as_filename else "with original filenames + msgID"
            db_mode = "DB SYNC DISABLED" if not self.sync_db else "DB SYNC ENABLED"
            
            await edit_message(
                self.status_message, 
                f"Processing channel: **{chat.title}**\n"
                f"Scanning messages...\n"
                f"Upload: **TESTING MODE - {db_mode}** {caption_info}"
            )

            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("Channel leech cancelled by user")
                    break

                processed += 1
                batch_count += 1
                self.batch_counter += 1

                LOGGER.info("[BATCH] #%s: Starting processing message %s", self.batch_counter, message.id)

                await channel_status.update_operation(
                    self.operation_key, processed=processed
                )

                try:
                    # Create completely isolated scanner for THIS message only
                    isolated_scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
                    isolated_file_info = await isolated_scanner._extract_file_info(message)
                    
                    if not isolated_file_info:
                        LOGGER.info("[SKIP] MSG_%s: No file info, skipping", message.id)
                        continue

                    LOGGER.info("[FILE] MSG_%s: File info extracted - %s", message.id, isolated_file_info.get('file_name', 'unknown'))

                    # Apply filters to THIS message only
                    if self.filter_tags:
                        search_text = isolated_file_info['search_text'].lower()
                        if not all(tag.lower() in search_text for tag in self.filter_tags):
                            LOGGER.info("[FILTER] MSG_%s: Failed filter check, skipping", message.id)
                            continue

                    # Check duplicates ONLY if database sync is enabled
                    if self.sync_db:
                        exists = await database.check_file_exists(
                            isolated_file_info.get('file_unique_id'),
                            isolated_file_info.get('file_hash'),
                            isolated_file_info.get('file_name')
                        )

                        if exists:
                            LOGGER.info("[DUPLICATE] MSG_%s: Duplicate file, skipping", message.id)
                            skipped += 1
                            await channel_status.update_operation(
                                self.operation_key, skipped=skipped
                            )
                            continue
                    else:
                        LOGGER.info("[TEST] MSG_%s: TESTING MODE - Skipping duplicate check", message.id)

                    LOGGER.info("[PROCESS] MSG_%s: Starting isolated processing", message.id)

                    # Process THIS message with completely isolated state
                    await self._download_file_with_complete_isolation(message, isolated_file_info)
                    
                    downloaded += 1
                    LOGGER.info("[SUCCESS] MSG_%s: Completed isolated processing", message.id)

                    # Add to database ONLY if sync is enabled
                    if self.sync_db:
                        await database.add_file_entry(
                            self.channel_id, message.id, isolated_file_info
                        )
                        LOGGER.info("[DATABASE] MSG_%s: Added to database", message.id)
                    else:
                        LOGGER.info("[TEST] MSG_%s: TESTING MODE - Skipped database entry", message.id)

                    await channel_status.update_operation(
                        self.operation_key, downloaded=downloaded
                    )

                except Exception as e:
                    errors += 1
                    LOGGER.error("[ERROR] MSG_%s: Processing failed: %s", message.id, e)
                    await channel_status.update_operation(
                        self.operation_key, errors=errors
                    )

                # Critical: Sleep between messages to prevent state bleeding
                await asyncio.sleep(message_sleep)

                if batch_count >= 10:
                    status_text = (
                        f"**TESTING Progress Update**\n"
                        f"Processed: {processed}\n"
                        f"Downloaded: {downloaded}\n"
                        f"Skipped: {skipped}\n"
                        f"Errors: {errors}\n"
                        f"DB Sync: {'DISABLED' if not self.sync_db else 'ENABLED'}\n"
                        f"Using: No Tag Parameter (Testing)"
                    )
                    await edit_message(self.status_message, status_text)
                    
                    LOGGER.info("[BATCH] Completed (%s messages), sleeping for %s seconds", batch_count, batch_sleep)
                    await asyncio.sleep(batch_sleep)
                    batch_count = 0

            final_text = (
                f"**TESTING: Channel leech completed!**\n\n"
                f"**Total processed:** {processed}\n"
                f"**Downloaded:** {downloaded}\n"
                f"**Skipped (duplicates):** {skipped}\n"
                f"**Errors:** {errors}\n\n"
                f"**Channel:** `{self.channel_id}`\n"
                f"**DB Sync:** {'DISABLED (Testing)' if not self.sync_db else 'ENABLED'}\n"
                f"**System:** No Tag Parameter Applied"
            )
            await edit_message(self.status_message, final_text)

        except FloodWait as e:
            LOGGER.warning("FloodWait during channel processing: %s seconds", e.x)
            await edit_message(self.status_message, f"Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("Resuming channel processing after FloodWait")
            
        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _download_file_with_complete_isolation(self, message, file_info):
        """Download file with complete state isolation - TAG PARAMETER REMOVED FOR TESTING"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        LOGGER.info("[ISOLATION] MSG_%s: === STARTING COMPLETE ISOLATION ===", message.id)
        
        # Create completely isolated copy of file_info for THIS file only
        isolated_file_info = deepcopy(file_info)
        LOGGER.info("[COPY] MSG_%s: Created isolated file_info copy", message.id)
        
        # Generate unique filename using message-specific data only
        if self.use_caption_as_filename and hasattr(message, 'caption') and message.caption:
            first_line = message.caption.split('\n')[0].strip()
            LOGGER.info("[CAPTION] MSG_%s: Caption first line: %s", message.id, first_line)
            
            if first_line and len(first_line) >= 3:
                # Start completely fresh for THIS message
                clean_name = sanitize_filename(first_line)
                LOGGER.info("[CLEAN] MSG_%s: Cleaned caption: %s", message.id, clean_name)
                
                if clean_name and len(clean_name) >= 3:
                    # Get extension from THIS file only
                    original_extension = os.path.splitext(isolated_file_info['file_name'])[1]
                    
                    # Build unique filename with message ID to prevent ANY collisions
                    unique_filename = f"{clean_name}_{message.id}{original_extension}"
                    
                    # Update ONLY the isolated copy
                    isolated_file_info['file_name'] = unique_filename
                    
                    LOGGER.info("[FILENAME] MSG_%s: Isolated filename: %s -> %s", message.id, file_info['file_name'], unique_filename)
                else:
                    # Clean name too short, use original filename with unique suffix
                    base_name = os.path.splitext(isolated_file_info['file_name'])[0]
                    extension = os.path.splitext(isolated_file_info['file_name'])[1]
                    isolated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
                    LOGGER.info("[FILENAME] MSG_%s: Caption too short, using original with suffix: %s", message.id, isolated_file_info['file_name'])
            else:
                # Caption too short, use original with unique ID
                base_name = os.path.splitext(isolated_file_info['file_name'])[0]
                extension = os.path.splitext(isolated_file_info['file_name'])[1]
                isolated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
                LOGGER.info("[FILENAME] MSG_%s: Empty caption, using original with suffix: %s", message.id, isolated_file_info['file_name'])
        else:
            # No caption mode - use original filename with unique ID
            base_name = os.path.splitext(isolated_file_info['file_name'])[0]
            extension = os.path.splitext(isolated_file_info['file_name'])[1]
            isolated_file_info['file_name'] = f"{base_name}_{message.id}{extension}"
            LOGGER.info("[FILENAME] MSG_%s: No caption mode, using original with suffix: %s", message.id, isolated_file_info['file_name'])
        
        # Create download directory
        os.makedirs(download_path, exist_ok=True)
        
        # Log this specific file's processing with complete context
        LOGGER.info("[CONTEXT] MSG_%s: ISOLATION CONTEXT:", message.id)
        LOGGER.info("    [FILE] Final filename: %s", isolated_file_info['file_name'])
        LOGGER.info("    [SIZE] File size: %s", isolated_file_info.get('file_size', 'unknown'))
        LOGGER.info("    [LEECH] is_leech: %s", self.is_leech)
        LOGGER.info("    [SPLIT] split_size: %s", self.split_size)
        LOGGER.info("    [DOC] as_doc: %s", self.as_doc)
        LOGGER.info("    [DEST] leech_dest: %s", self.leech_dest)
        LOGGER.info("    [TEST] sync_db: %s", self.sync_db)
        
        # Create completely isolated download helper instance
        telegram_helper = TelegramDownloadHelper(self)
        
        # Process with isolated file info - TAG PARAMETER REMOVED FOR TESTING
        try:
            LOGGER.info("[DOWNLOAD] MSG_%s: Starting download with isolated context (NO TAG)", message.id)
            
            # Temporarily update the original file_info for download processing
            original_filename = file_info['file_name']
            file_info['file_name'] = isolated_file_info['file_name']
            
            # REMOVED TAG PARAMETER TO FIX TESTING ERRORS
            await telegram_helper.add_download(
                message=message,
                path=download_path
            )
            
            LOGGER.info("[SUCCESS] MSG_%s: Download completed successfully with isolated filename: %s", message.id, isolated_file_info['file_name'])
            
        except Exception as e:
            LOGGER.error("[ERROR] MSG_%s: Download failed: %s", message.id, e)
            raise
        finally:
            # Restore original filename to prevent affecting other operations
            file_info['file_name'] = original_filename
            LOGGER.info("[RESTORE] MSG_%s: Restored original filename for safety", message.id)

    def _parse_arguments(self, args):
        """Parse command arguments including --test flag for disabling DB sync"""
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
            elif args[i] == '--test':
                parsed['test'] = True
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
    """Handle /cleech command - TESTING VERSION with tag parameter removed"""
    # Check if --test flag is in the command to disable DB sync
    test_mode = '--test' in message.text
    await ChannelLeech(client, message, sync_db=not test_mode).new_event()

# Register handlers
bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
