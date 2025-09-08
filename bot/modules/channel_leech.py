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
    import re
    
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

class SmartChannelLeechCoordinator(TaskListener):
    """Uses existing INCOMPLETE_TASK_NOTIFIER system for perfect tracking"""
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        
        # Simple concurrency management
        self.max_concurrent = 2           # Max active downloads
        self.check_interval = 5           # Check every 5 seconds
        
        # Track our downloads using existing INCOMPLETE_TASK_NOTIFIER
        self.pending_files = []           # Files waiting to start
        self.our_active_gids = set()      # GIDs we started
        self.completed_count = 0
        self.total_files = 0
        
        super().__init__()
        
        LOGGER.info(f"[SMART-LEECH] Using INCOMPLETE_TASK_NOTIFIER - max {self.max_concurrent} concurrent")

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
                "• Uses existing INCOMPLETE_TASK_NOTIFIER system\n"
                "• Perfect integration with your bot's infrastructure\n"
                "• Clean dot-separated filenames\n"
                "• Real-time progress tracking via existing database"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)

        if not user:
            await send_message(self.message, "User session required!")
            return

        # Start operation
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "smart_channel_leech"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        
        self.status_message = await send_message(
            self.message, 
            f"🧠 **Smart Channel Leech** `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**System:** INCOMPLETE_TASK_NOTIFIER tracking\n"
            f"**Max concurrent:** {self.max_concurrent} downloads\n"
            f"**Cancel:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._coordinate_smart_leech()
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _coordinate_smart_leech(self):
        """Main coordination using INCOMPLETE_TASK_NOTIFIER"""
        processed = 0
        skipped = 0
        
        try:
            # Get channel info and scan
            chat = await user.get_chat(self.channel_id)
            await edit_message(self.status_message, f"🔍 **Scanning:** {chat.title}")
            
            # Build file list
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    break
                
                processed += 1
                
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
                    skipped += 1
                    continue
                
                # Generate message link
                if str(self.channel_id).startswith('-100'):
                    clean_channel_id = str(self.channel_id)[4:]
                else:
                    clean_channel_id = str(self.channel_id).replace('@', '')
                
                message_link = f"https://t.me/{clean_channel_id}/{message.id}"
                
                # Add to pending queue
                file_item = {
                    'url': message_link,
                    'filename': file_info['file_name'],
                    'message_id': message.id,
                    'file_info': file_info
                }
                
                self.pending_files.append(file_item)
            
            self.total_files = len(self.pending_files)
            
            if self.total_files == 0:
                await edit_message(self.status_message, "✅ No new files to download!")
                return
            
            LOGGER.info(f"[SMART-LEECH] Found {self.total_files} files, using INCOMPLETE_TASK_NOTIFIER for tracking")
            
            # Start smart processing
            await self._process_with_incomplete_task_tracking()
            
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")

    async def _process_with_incomplete_task_tracking(self):
        """Process files using INCOMPLETE_TASK_NOTIFIER for completion detection"""
        
        # Start initial downloads (up to max_concurrent)
        while len(self.our_active_gids) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        
        # Monitor loop - check INCOMPLETE_TASK_NOTIFIER for completions
        while (self.our_active_gids or self.pending_files) and not self.is_cancelled:
            
            # Check which of our GIDs completed by querying INCOMPLETE_TASK_NOTIFIER
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
        """Start next download and track with our GID set"""
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
            
            # Execute leech command
            gid = await self._execute_leech_command(leech_cmd)
            
            if gid:
                # Add GID to our tracking set
                self.our_active_gids.add(gid)
                
                LOGGER.info(f"[SMART-LEECH] Started {gid}: {file_item['filename'][:50]}... (tracking via INCOMPLETE_TASK_NOTIFIER)")
            else:
                # Failed to start - add back to queue
                self.pending_files.append(file_item)
                LOGGER.warning(f"[SMART-LEECH] Failed to start: {file_item['filename']}")
        
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Error starting download: {e}")
            # Add back to queue for retry
            self.pending_files.append(file_item)

    async def _check_completed_via_incomplete_task_notifier(self):
        """Check which of our GIDs completed by querying INCOMPLETE_TASK_NOTIFIER"""
        completed_gids = []
        
        try:
            # Get all current incomplete tasks from your existing system
            current_incomplete_tasks = await database.get_incomplete_tasks()
            
            # Extract all current incomplete GIDs from the notifier
            current_incomplete_gids = set()
            for cid_data in current_incomplete_tasks.values():
                for tag_data in cid_data.values():
                    current_incomplete_gids.update(tag_data)
            
            # Check which of our GIDs are no longer in the incomplete tasks
            for gid in list(self.our_active_gids):
                if gid not in current_incomplete_gids:
                    # GID no longer in INCOMPLETE_TASK_NOTIFIER = completed!
                    self.our_active_gids.remove(gid)
                    completed_gids.append(gid)
                    self.completed_count += 1
                    
                    LOGGER.info(f"[SMART-LEECH] Completed {gid} (removed from INCOMPLETE_TASK_NOTIFIER)")
        
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Error checking INCOMPLETE_TASK_NOTIFIER: {e}")
        
        return completed_gids

    async def _update_progress(self):
        """Update user with current progress"""
        try:
            active_count = len(self.our_active_gids)
            pending_count = len(self.pending_files)
            progress_percentage = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
            
            progress_text = (
                f"🧠 **Smart Channel Leech Progress**\n\n"
                f"**Progress:** {self.completed_count}/{self.total_files} ({progress_percentage:.1f}%)\n"
                f"**🔄 Active:** {active_count}/{self.max_concurrent}\n"
                f"**⏳ Pending:** {pending_count}\n"
                f"**✅ Completed:** {self.completed_count}\n\n"
                f"**System:** INCOMPLETE_TASK_NOTIFIER tracking\n"
            )
            
            if self.our_active_gids:
                progress_text += "**Active Downloads:**\n"
                for i, gid in enumerate(list(self.our_active_gids)):
                    if i < 3:  # Show max 3 active
                        progress_text += f"📥 `{gid[:12]}...`\n"
            
            await edit_message(self.status_message, progress_text)
            
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Error updating progress: {e}")

    async def _show_final_results(self):
        """Show final results"""
        success_rate = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
        
        final_text = (
            f"✅ **Smart Channel Leech Completed!**\n\n"
            f"**Total Files:** {self.total_files}\n"
            f"**✅ Downloaded:** {self.completed_count}\n"
            f"**📊 Success Rate:** {success_rate:.1f}%\n\n"
            f"**System:** INCOMPLETE_TASK_NOTIFIER tracking\n"
            f"**Max Concurrent:** {self.max_concurrent}\n\n"
            f"Perfect integration with existing bot infrastructure!"
        )
        
        await edit_message(self.status_message, final_text)

    async def _execute_leech_command(self, leech_command):
        """Execute leech command and return GID"""
        try:
            from ..modules.mirror_leech import mirror_leech_cmd
            
            fake_message = type('obj', (object,), {
                'text': leech_command,
                'from_user': self.message.from_user,
                'chat': self.message.chat,
                'reply_to_message': None,
            })
            
            # Execute and get task listener with GID
            task_listener = await mirror_leech_cmd(self.client, fake_message)
            return getattr(task_listener, 'mid', None) if task_listener else None
            
        except Exception as e:
            LOGGER.error(f"[SMART-LEECH] Failed to execute: {e}")
            return None

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
        LOGGER.info(f"[SMART-LEECH] Task cancelled for {self.channel_id}")

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
async def smart_channel_leech_cmd(client, message):
    """Handle /cleech with INCOMPLETE_TASK_NOTIFIER tracking"""
    await SmartChannelLeechCoordinator(client, message).new_event()

# Register BOTH handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    smart_channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
