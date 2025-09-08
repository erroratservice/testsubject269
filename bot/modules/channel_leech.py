from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from bot import bot, user, LOGGER, user_data
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
import time
import hashlib

def generate_universal_telegram_gid(chat_id: int, message_id: int) -> str:
    """
    Generate the EXACT same deterministic GID used by TelegramDownloadHelper
    This ensures perfect prediction and tracking
    """
    combined = f"tg_{chat_id}_{message_id}"
    hash_obj = hashlib.sha256(combined.encode('utf-8'))
    return hash_obj.hexdigest()[:12]

def extract_telegram_url_components(telegram_url: str) -> tuple:
    """Extract components from Telegram URL for GID generation"""
    # Pattern for different Telegram URL formats
    patterns = [
        r't\.me/c/(\d+)/(\d+)',           # Private: t.me/c/123456789/456
        r't\.me/([^/\s]+)/(\d+)',         # Public: t.me/channel_name/456
    ]
    
    for pattern in patterns:
        match = re.search(pattern, telegram_url)
        if match:
            chat_identifier = match.group(1)
            message_id = int(match.group(2))
            
            if chat_identifier.isdigit():
                # Private channel format: convert to full chat_id
                chat_id = -int(f"100{chat_identifier}")
            else:
                # Public channel: will be resolved at runtime
                return chat_identifier, message_id, 'username'
            
            return chat_id, message_id, 'chat_id'
    
    return None, None, None

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
    # Remove emojis first
    filename = remove_emoji(filename)
    
    # Replace underscores with spaces
    filename = re.sub(r'[_]+', ' ', filename)
    
    # Replace unwanted characters with spaces (not dots)
    filename = re.sub(r'[^\w\d\.\s]', ' ', filename)
    
    # Remove spaces around existing dots
    filename = re.sub(r'\s*\.\s*', '.', filename)
    
    # Replace multiple dots with single dot
    filename = re.sub(r'\.{2,}', '.', filename)
    
    # Replace multiple spaces with single space
    filename = re.sub(r'\s+', ' ', filename)
    
    # Trim leading/trailing dots and spaces
    filename = filename.strip(' .')
    
    # Finally replace remaining spaces with dots
    filename = filename.replace(' ', '.')
    
    # Ensure we don't end up with empty filename
    if not filename:
        filename = "file"
    
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename

class UniversalChannelLeechCoordinator(TaskListener):
    """Universal coordinator with perfect GID prediction for all Telegram downloads"""
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None  # Resolved numeric chat_id
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        
        # Smart queue configuration
        self.max_concurrent = 2           # Max active downloads
        self.check_interval = 5           # Check every 5 seconds
        
        # Track our downloads using perfect GID prediction
        self.pending_files = []           # Files waiting to start
        self.our_active_gids = set()      # Predicted GIDs we're tracking
        self.completed_count = 0
        self.total_files = 0
        
        super().__init__()
        
        LOGGER.info(f"[UNIVERSAL-LEECH] Created coordinator {self.mid} with perfect GID prediction")

    async def new_event(self):
        """Main event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])

        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n\n"
                "**Features:**\n"
                "• Universal GID system for perfect tracking\n"
                "• Works with ALL Telegram downloads\n"
                "• Smart concurrency management (max 2 concurrent)\n"
                "• Real-time progress via INCOMPLETE_TASK_NOTIFIER\n"
                "• Clean dot-separated filenames"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)

        if not user:
            await send_message(self.message, "User session required!")
            return

        # Resolve channel to numeric chat_id for perfect GID prediction
        try:
            chat = await user.get_chat(self.channel_id)
            self.channel_chat_id = chat.id
            LOGGER.info(f"[UNIVERSAL-LEECH] Resolved channel {self.channel_id} to chat_id: {self.channel_chat_id}")
        except Exception as e:
            await send_message(self.message, f"❌ Could not resolve channel: {e}")
            return

        # Start operation
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "universal_channel_leech"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        
        self.status_message = await send_message(
            self.message, 
            f"🌟 **Universal Channel Leech** `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}` → `{self.channel_chat_id}`\n"
            f"{filter_text}\n"
            f"**System:** Universal Telegram GID prediction\n"
            f"**Max concurrent:** {self.max_concurrent} downloads\n"
            f"**Cancel:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._coordinate_universal_leech()
        except Exception as e:
            LOGGER.error(f"[UNIVERSAL-LEECH] Error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _coordinate_universal_leech(self):
        """Main coordination with universal GID system"""
        try:
            # Get channel info and scan
            chat = await user.get_chat(self.channel_id)
            await edit_message(self.status_message, f"🔍 **Scanning:** {chat.title}")
            
            # Build file list
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    break
                
                # Extract and filter files
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                if self.filter_tags:
                    search_text = file_info['search_text'].lower()
                    if not all(tag.lower() in search_text for tag in self.filter_tags):
                        continue
                
                # Check duplicates
                exists = await database.check_file_exists(
                    file_info.get('file_unique_id'),
                    file_info.get('file_hash'),
                    file_info.get('file_name')
                )
                
                if exists:
                    continue
                
                # Generate message link
                if str(self.channel_chat_id).startswith('-100'):
                    clean_channel_id = str(self.channel_chat_id)[4:]
                    message_link = f"https://t.me/c/{clean_channel_id}/{message.id}"
                else:
                    channel_username = self.channel_id.replace('@', '')
                    message_link = f"https://t.me/{channel_username}/{message.id}"
                
                # Pre-calculate the GID that will be generated
                predicted_gid = generate_universal_telegram_gid(self.channel_chat_id, message.id)
                
                # Add to pending queue
                file_item = {
                    'url': message_link,
                    'filename': file_info['file_name'],
                    'message_id': message.id,
                    'file_info': file_info,
                    'predicted_gid': predicted_gid  # Store predicted GID
                }
                
                self.pending_files.append(file_item)
            
            self.total_files = len(self.pending_files)
            
            if self.total_files == 0:
                await edit_message(self.status_message, "✅ No new files to download!")
                return
            
            LOGGER.info(f"[UNIVERSAL-LEECH] Found {self.total_files} files with perfect GID prediction")
            
            # Start universal processing
            await self._process_with_universal_gid_tracking()
            
        except Exception as e:
            LOGGER.error(f"[UNIVERSAL-LEECH] Error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")

    async def _process_with_universal_gid_tracking(self):
        """Process files using universal GID tracking"""
        
        # Start initial downloads (up to max_concurrent)
        while len(self.our_active_gids) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        
        # Monitor loop - check INCOMPLETE_TASK_NOTIFIER for completions
        while (self.our_active_gids or self.pending_files) and not self.is_cancelled:
            
            # Check which of our predicted GIDs completed
            completed_gids = await self._check_completed_via_incomplete_task_notifier()
            
            # Start new downloads for completed slots
            for _ in range(len(completed_gids)):
                if self.pending_files and len(self.our_active_gids) < self.max_concurrent:
                    await self._start_next_download()
            
            # Update progress
            await self._update_progress()
            
            # Wait before next check
            await asyncio.sleep(self.check_interval)
        
        # Show final results
        await self._show_final_results()

    async def _start_next_download(self):
        """Start next download with perfect GID prediction"""
        if not self.pending_files:
            return
        
        file_item = self.pending_files.pop(0)
        
        try:
            # Generate leech command
            if self.use_caption_as_filename:
                clean_name = self._generate_clean_filename(
                    file_item['file_info'], file_item['message_id']
                )
                leech_cmd = f"/leech {file_item['url']} -n \"{clean_name}\""
            else:
                leech_cmd = f"/leech {file_item['url']}"
            
            # Send the leech command
            sent_message = await self.client.send_message(
                chat_id=self.message.chat.id,
                text=leech_cmd
            )
            
            # Use the pre-calculated predicted GID
            predicted_gid = file_item['predicted_gid']
            
            # Add predicted GID to our tracking set
            self.our_active_gids.add(predicted_gid)
            
            LOGGER.info(f"[UNIVERSAL-LEECH] Started download with predicted GID {predicted_gid}: {file_item['filename'][:50]}...")
            
            # Small delay for bot to process
            await asyncio.sleep(1)
            
        except Exception as e:
            LOGGER.error(f"[UNIVERSAL-LEECH] Error starting download: {e}")
            # Add back to queue for retry
            self.pending_files.append(file_item)

    async def _check_completed_via_incomplete_task_notifier(self):
        """Check completion using our perfectly predicted GIDs"""
        completed_gids = []
        
        try:
            # Get all current incomplete tasks
            current_incomplete_tasks = await database.get_incomplete_tasks()
            
            # Extract all current incomplete GIDs
            current_incomplete_gids = set()
            for cid_data in current_incomplete_tasks.values():
                for tag_data in cid_data.values():
                    current_incomplete_gids.update(tag_data)
            
            # Check which of our predicted GIDs are no longer incomplete
            for gid in list(self.our_active_gids):
                if gid not in current_incomplete_gids:
                    # GID completed!
                    self.our_active_gids.remove(gid)
                    completed_gids.append(gid)
                    self.completed_count += 1
                    
                    LOGGER.info(f"[UNIVERSAL-LEECH] Completed predicted GID: {gid}")
        
        except Exception as e:
            LOGGER.error(f"[UNIVERSAL-LEECH] Error checking completion: {e}")
        
        return completed_gids

    async def _update_progress(self):
        """Update user with current progress"""
        try:
            active_count = len(self.our_active_gids)
            pending_count = len(self.pending_files)
            progress_percentage = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
            
            progress_text = (
                f"🌟 **Universal Channel Leech Progress**\n\n"
                f"**Progress:** {self.completed_count}/{self.total_files} ({progress_percentage:.1f}%)\n"
                f"**🔄 Active:** {active_count}/{self.max_concurrent}\n"
                f"**⏳ Pending:** {pending_count}\n"
                f"**✅ Completed:** {self.completed_count}\n\n"
                f"**System:** Universal Telegram GID prediction\n"
            )
            
            if self.our_active_gids:
                progress_text += "**Active Downloads:**\n"
                for i, gid in enumerate(list(self.our_active_gids)):
                    if i < 3:  # Show max 3 active
                        progress_text += f"📥 `{gid}`\n"
            
            await edit_message(self.status_message, progress_text)
            
        except Exception as e:
            LOGGER.error(f"[UNIVERSAL-LEECH] Error updating progress: {e}")

    async def _show_final_results(self):
        """Show final results"""
        success_rate = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
        
        final_text = (
            f"✅ **Universal Channel Leech Completed!**\n\n"
            f"**Total Files:** {self.total_files}\n"
            f"**✅ Downloaded:** {self.completed_count}\n"
            f"**📊 Success Rate:** {success_rate:.1f}%\n\n"
            f"**System:** Universal Telegram GID prediction\n"
            f"**Max Concurrent:** {self.max_concurrent}\n\n"
            f"Perfect tracking with universal GID system! 🚀"
        )
        
        await edit_message(self.status_message, final_text)

    def _generate_clean_filename(self, file_info, message_id):
        """Generate clean filename"""
        original_filename = file_info['file_name']
        
        if self.use_caption_as_filename:
            caption_text = file_info.get('search_text', original_filename)
            first_line = caption_text.split('\n')[0].strip()
            
            if first_line and len(first_line) >= 3:
                clean_name = sanitize_filename(first_line)
                if clean_name:
                    original_extension = os.path.splitext(original_filename)[1]
                    if not clean_name.lower().endswith(original_extension.lower()):
                        clean_name = clean_name + original_extension
                    
                    name_part = os.path.splitext(clean_name)[0]
                    ext_part = os.path.splitext(clean_name)[1]
                    return f"{name_part}.{message_id}{ext_part}"
        
        clean_name = sanitize_filename(original_filename)
        name_part = os.path.splitext(clean_name)[0]
        ext_part = os.path.splitext(clean_name)[1]
        return f"{name_part}.{message_id}{ext_part}"

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
        """Cancel the channel leech coordination"""
        self.is_cancelled = True
        LOGGER.info(f"[UNIVERSAL-LEECH] Task cancelled for {self.channel_id}")

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

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        status_msg = await send_message(
            self.message, 
            f"🔍 Starting scan `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
            
        except Exception as e:
            LOGGER.error(f"[CHANNEL-SCANNER] Error: {e}")
            await edit_message(status_msg, f"❌ Scan failed: {str(e)}")

    def cancel_task(self):
        """Cancel the scan operation"""
        self.is_cancelled = True
        if self.scanner:
            self.scanner.running = False
        LOGGER.info(f"[CHANNEL-SCANNER] Scan cancelled for {self.channel_id}")

@new_task
async def channel_scan(client, message):
    """Handle /scan command with task ID support"""
    await ChannelScanListener(user, message).new_event()

@new_task
async def universal_channel_leech_cmd(client, message):
    """Handle /cleech with universal GID tracking"""
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
