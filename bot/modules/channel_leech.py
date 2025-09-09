from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from bot import bot, user, LOGGER
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
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
    """Sanitize filename by creating clean dot-separated names"""
    filename = remove_emoji(filename)
    
    # Replace underscores with spaces
    filename = re.sub(r'[_]+', ' ', filename)
    
    # Only remove truly problematic characters for filenames
    # Keep: letters, numbers, dots, spaces, hyphens, parentheses, brackets, colons
    filename = re.sub(r'[<>:"/\\|?*]', ' ', filename)  # Only remove filesystem-forbidden chars
    
    # Clean up dots and spaces
    filename = re.sub(r'\s*\.\s*', '.', filename)
    filename = re.sub(r'\.{2,}', '.', filename)  
    filename = re.sub(r'\s+', ' ', filename)
    filename = filename.strip(' .')
    filename = filename.replace(' ', '.')
    
    if not filename:
        filename = "file"
    
    return filename

class UniversalChannelLeechCoordinator(TaskListener):
    """Universal coordinator for channel leech operations"""
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.max_concurrent = 2
        self.check_interval = 10
        self.pending_files = []
        self.our_active_links = set()
        self.completed_count = 0
        self.total_files = 0
        self.link_to_file_mapping = {}  # Track file info for database save
        self._last_status_text = ""  # Track last status to prevent MESSAGE_NOT_MODIFIED
        super().__init__()

    async def new_event(self):
        """Main event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        
        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)

        if not user:
            await send_message(self.message, "User session required!")
            return

        try:
            chat = await user.get_chat(self.channel_id)
            self.channel_chat_id = chat.id
        except Exception as e:
            await send_message(self.message, f"Could not resolve channel: {e}")
            return

        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "universal_channel_leech"
        )

        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        self.status_message = await send_message(
            self.message,
            f"**Channel Leech Initializing...**\n"
            f"**Channel:** `{self.channel_id}` → `{self.channel_chat_id}`\n"
            f"**Filter:**{filter_text}"
        )

        try:
            await self._coordinate_universal_leech()
        except Exception as e:
            LOGGER.error(f"[cleech] Coordination Error: {e}")
            await edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _coordinate_universal_leech(self):
        """Main coordination with database link tracking and batch processing"""
        await edit_message(self.status_message, f"**Scanning Channel...**")
        scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
        
        # Batch processing configuration
        batch_size = 30
        batch_sleep = 2
        batch_count = 0
        processed_messages = 0
        skipped_duplicates = 0
        
        async for message in user.get_chat_history(self.channel_id):
            if self.is_cancelled: 
                break
                
            processed_messages += 1
            batch_count += 1
            
            file_info = await scanner._extract_file_info(message)
            if not file_info: 
                continue
                
            if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                continue
            
            # DEBUG: Check if file exists
            file_exists = await database.check_file_exists(file_info.get('file_unique_id'), file_info.get('file_hash'), file_info.get('file_name'))
            
            if file_exists:
                skipped_duplicates += 1
                LOGGER.info(f"[cleech-debug] Skipping duplicate: {file_info.get('file_name')}")
                continue
            else:
                LOGGER.info(f"[cleech-debug] New file found: {file_info.get('file_name')}")

            if str(self.channel_chat_id).startswith('-100'):
                message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{message.id}"
            else:
                message_link = f"https://t.me/{self.channel_id.replace('@', '')}/{message.id}"
            
            self.pending_files.append({
                'url': message_link,
                'filename': file_info['file_name'],
                'message_id': message.id,
                'file_info': file_info,
                'link': message_link
            })

            # Batch processing with status updates and sleep
            if batch_count >= batch_size:
                await self._handle_scan_batch_complete(processed_messages, len(self.pending_files), batch_count, skipped_duplicates)
                batch_count = 0
                
                # Sleep after each batch to respect rate limits
                await asyncio.sleep(batch_sleep)

        # Handle remaining messages if any
        if batch_count > 0:
            await self._handle_scan_batch_complete(processed_messages, len(self.pending_files), batch_count, skipped_duplicates)

        LOGGER.info(f"[cleech-debug] Scan complete - Total: {processed_messages}, New: {len(self.pending_files)}, Skipped: {skipped_duplicates}")

        self.total_files = len(self.pending_files)
        if self.total_files == 0:
            await edit_message(self.status_message, "**No new files to download!**")
            return

        await edit_message(self.status_message, f"**Found {self.total_files} files. Starting downloads...**")
        await self._process_with_link_tracking()

    async def _handle_scan_batch_complete(self, processed_messages, found_files, batch_count, skipped_duplicates):
        """Handle scan batch completion with status update"""
        try:
            status_text = (
                f"**Scanning Channel...**\n\n"
                f"**Scanned:** {processed_messages} messages\n"
                f"**Found:** {found_files} new files\n"
                f"**Skipped:** {skipped_duplicates} duplicates\n"
                f"**Batch:** {batch_count} messages processed"
            )
            
            # Only update if content has changed
            if self._last_status_text != status_text:
                await edit_message(self.status_message, status_text)
                self._last_status_text = status_text
                
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"[cleech] Scan status update error: {e}")

    async def _process_with_link_tracking(self):
        """Process files using database link tracking"""
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
            
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            await self._update_progress()
            await asyncio.sleep(self.check_interval)
            
            completed_links = await self._check_completed_via_database()
            for _ in completed_links:
                if self.pending_files and len(self.our_active_links) < self.max_concurrent:
                    await self._start_next_download()
        
        await self._show_final_results()

    async def _start_next_download(self):
        """Start next download and capture the actual command message ID for tracking"""
        if not self.pending_files: 
            return
            
        file_item = self.pending_files.pop(0)
        
        try:
            COMMAND_CHANNEL_ID = -1001791052293
            clean_name = self._generate_clean_filename(file_item['file_info'], file_item['message_id'])
            leech_cmd = f'/leech {file_item["url"]} -n {clean_name}'
            
            command_message = await user.send_message(chat_id=COMMAND_CHANNEL_ID, text=leech_cmd)
            command_msg_id = command_message.id
            
            actual_stored_url = f"https://t.me/c/{str(COMMAND_CHANNEL_ID)[4:]}/{command_msg_id}"
            
            await asyncio.sleep(5)
            
            self.our_active_links.add(actual_stored_url)
            
            # Store mapping for database save when completed
            self.link_to_file_mapping[actual_stored_url] = file_item
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error starting download: {e}")
            self.pending_files.insert(0, file_item)

    async def _check_completed_via_database(self):
        """Check completion using the actual stored URLs"""
        completed_links = []
        try:
            if database._return:
                LOGGER.info(f"[cleech-debug] Database not available, skipping completion check")
                return completed_links
            
            from bot import BOT_ID
            
            current_incomplete_links = set()
            
            if await database._db.tasks[BOT_ID].find_one():
                rows = database._db.tasks[BOT_ID].find({})
                async for row in rows:
                    current_incomplete_links.add(row["_id"])
            
            LOGGER.info(f"[cleech-debug] Current incomplete tasks: {len(current_incomplete_links)}")
            LOGGER.info(f"[cleech-debug] Our tracked links: {len(self.our_active_links)}")
            
            for tracked_link in list(self.our_active_links):
                if tracked_link not in current_incomplete_links:
                    LOGGER.info(f"[cleech-debug] Detected completion for: {tracked_link}")
                    
                    # Save completed file to database to prevent re-downloading
                    await self._save_completed_file(tracked_link)
                    
                    self.our_active_links.remove(tracked_link)
                    completed_links.append(tracked_link)
                    self.completed_count += 1
                    
        except Exception as e:
            LOGGER.error(f"[cleech] Error checking completion: {e}")
            import traceback
            LOGGER.error(f"[cleech] Traceback: {traceback.format_exc()}")
            
        return completed_links

    async def _save_completed_file(self, completed_link):
        """Save completed file to database using the same format as channel scanner"""
        try:
            LOGGER.info(f"[cleech-debug] Attempting to save completed file: {completed_link}")
            
            if completed_link in self.link_to_file_mapping:
                file_item = self.link_to_file_mapping[completed_link]
                
                LOGGER.info(f"[cleech-debug] File mapping found for: {completed_link}")
                LOGGER.info(f"[cleech-debug] File info: {file_item['file_info'].get('file_name')}")
                LOGGER.info(f"[cleech-debug] File unique ID: {file_item['file_info'].get('file_unique_id')}")
                
                # Use the same database.add_file_entry method as channel scanner
                await database.add_file_entry(
                    self.channel_chat_id,  # channel_id  
                    file_item['message_id'],  # message_id from original channel
                    file_item['file_info']  # file_info with all the scanner fields
                )
                
                LOGGER.info(f"[cleech-debug] Successfully saved file to database: {file_item['file_info'].get('file_name')}")
                
                # Clean up the mapping
                del self.link_to_file_mapping[completed_link]
                
            else:
                LOGGER.error(f"[cleech-debug] No file mapping found for completed link: {completed_link}")
                LOGGER.error(f"[cleech-debug] Available mappings: {list(self.link_to_file_mapping.keys())}")
                
        except Exception as e:
            LOGGER.error(f"[cleech] Error saving completed file: {e}")
            import traceback
            LOGGER.error(f"[cleech] Traceback: {traceback.format_exc()}")

    async def _update_progress(self):
        if not self.status_message: 
            return
            
        try:
            progress = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
            text = (
                f"**Channel Leech in Progress**\n\n"
                f"**Progress:** {self.completed_count}/{self.total_files} ({progress:.1f}%)\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n\n"
                f"**Cancel:** `/cancel {str(self.mid)[:12]}`"
            )
            
            # Only update if content has changed
            if self._last_status_text != text:
                await edit_message(self.status_message, text)
                self._last_status_text = text
                
        except Exception as e:
            # Still ignore MESSAGE_NOT_MODIFIED errors as backup
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"[cleech] Progress update error: {e}")

    async def _show_final_results(self):
        success_rate = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
        text = (
            f"**Channel Leech Completed!**\n\n"
            f"**Total:** {self.total_files} | **Downloaded:** {self.completed_count} | **Success:** {success_rate:.1f}%"
        )
        await edit_message(self.status_message, text)

    def _generate_clean_filename(self, file_info, message_id):
        original_filename = file_info['file_name']
        base_name = original_filename
        
        if self.use_caption_as_filename and file_info.get('caption_first_line'):
            base_name = file_info['caption_first_line'].strip()
        
        clean_base = sanitize_filename(base_name)
        original_ext = os.path.splitext(original_filename)[1]
        
        # Media file extensions commonly found in channels
        media_extensions = {
            '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',  # Video
            '.mp3', '.flac', '.wav', '.aac', '.m4a', '.ogg',                 # Audio  
            '.zip', '.rar', '.7z', '.tar', '.gz'                            # Archives
        }
        
        # Only add extension if it's a valid media extension and not already present
        if original_ext.lower() in media_extensions and not clean_base.lower().endswith(original_ext.lower()):
            clean_base += original_ext
        
        return clean_base

    def _parse_arguments(self, args):
        parsed, i = {}, 0
        while i < len(args):
            if args[i] == '-ch': 
                parsed['channel'] = args[i+1]; i+=2
            elif args[i] == '-f':
                text = args[i+1]
                parsed['filter'] = text[1:-1].split() if text.startswith('"') else text.split(); i+=2
            elif args[i] == '--no-caption': 
                parsed['no_caption'] = True; i+=1
            else: 
                i+=1
        return parsed

    def cancel_task(self):
        self.is_cancelled = True

class ChannelScanListener(TaskListener):
    """Simple channel scanner for database building"""
    
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

        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
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
            LOGGER.error(f"[CHANNEL-SCANNER] Error: {e}")
            await edit_message(status_msg, f"Scan failed: {str(e)}")

    def cancel_task(self):
        """Cancel the scan operation"""
        self.is_cancelled = True
        if self.scanner:
            self.scanner.running = False

@new_task
async def channel_scan(client, message):
    """Handle /scan command with task ID support"""
    await ChannelScanListener(user, message).new_event()

@new_task
async def universal_channel_leech_cmd(client, message):
    """Handle /cleech with database link tracking"""
    await UniversalChannelLeechCoordinator(client, message).new_event()

# Register BOTH handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    universal_channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
