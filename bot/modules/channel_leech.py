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

class ChannelLeechCoordinator(TaskListener):
    """Smart coordinator that scans channels and feeds links to internal leech system"""
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.batch_size = 3           # Links per batch
        self.batch_delay = 5          # Seconds between batches
        self.link_delay = 2           # Seconds between individual links
        
        # Inherit from TaskListener for proper user context
        super().__init__()
        
        LOGGER.info(f"[CHANNEL-COORDINATOR] Created coordinator task {self.mid}")

    async def new_event(self):
        """Main channel leech coordinator event handler"""
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
                "• Uses internal leech system for reliable downloads\n"
                "• Clean dot-separated filenames\n"
                "• All user settings automatically applied\n"
                "• Each file gets individual progress tracking\n"
                "• Built-in concurrency and error handling"
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
            self.message.from_user.id, self.channel_id, "channel_leech_coordinator"
        )

        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        caption_mode = "clean dot-separated filenames" if self.use_caption_as_filename else "original filenames"
        
        self.status_message = await send_message(
            self.message, 
            f"🚀 **Channel Leech Coordinator** `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**System:** Internal Leech Pipeline\n"
            f"**Filename mode:** {caption_mode}\n"
            f"**Batch size:** {self.batch_size} files\n"
            f"**Cancel with:** `/cancel {str(self.mid)[:12]}`"
        )

        try:
            await self._coordinate_channel_leech()
        except Exception as e:
            LOGGER.error(f"[CHANNEL-COORDINATOR] Error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _coordinate_channel_leech(self):
        """Coordinate channel scanning and link generation"""
        processed = 0
        queued = 0
        skipped = 0
        
        try:
            # Get channel info
            chat = await user.get_chat(self.channel_id)
            await edit_message(
                self.status_message,
                f"🔍 **Scanning channel:** {chat.title}\n"
                f"Filtering and generating message links..."
            )
            
            # Initialize scanner
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            message_links = []
            
            # Scan channel and collect matching files
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    LOGGER.info("[CHANNEL-COORDINATOR] Operation cancelled by user")
                    break
                
                processed += 1
                
                if processed % 50 == 0:  # Update every 50 messages
                    await channel_status.update_operation(
                        self.operation_key, processed=processed
                    )
                
                # Extract file info
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue
                
                # Apply filters
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
                    continue
                
                # Generate message link
                # Handle both private channels (-100) and public channels
                if str(self.channel_id).startswith('-100'):
                    # Private channel: use full ID without -100 prefix
                    clean_channel_id = str(self.channel_id)[4:]  # Remove -100 prefix
                else:
                    # Public channel: use as-is (username or clean ID)
                    clean_channel_id = str(self.channel_id).replace('@', '')
                
                message_link = f"https://t.me/{clean_channel_id}/{message.id}"
                
                # Prepare link with metadata
                link_data = {
                    'url': message_link,
                    'filename': file_info['file_name'],
                    'message_id': message.id,
                    'file_info': file_info
                }
                
                message_links.append(link_data)
                queued += 1
                
                LOGGER.info(f"[CHANNEL-COORDINATOR] Queued: {file_info['file_name']} -> {message_link}")
            
            # Send collected links to internal leech system in batches
            if message_links:
                await self._send_links_to_leech_system(message_links)
            
            # Final status update
            final_text = (
                f"✅ **Channel Leech Coordinator Completed!**\n\n"
                f"**Scanned messages:** {processed}\n"
                f"**Files queued for download:** {queued}\n"
                f"**Skipped (duplicates):** {skipped}\n\n"
                f"**Channel:** `{self.channel_id}`\n"
                f"**System:** Files sent to internal leech system\n"
                f"**Note:** Each file now has its own download task with individual progress tracking"
            )
            await edit_message(self.status_message, final_text)
            
            LOGGER.info(f"[CHANNEL-COORDINATOR] Completed - {queued} files queued")
            
        except FloodWait as e:
            LOGGER.warning(f"[CHANNEL-COORDINATOR] FloodWait: {e.x}s")
            await edit_message(self.status_message, f"⏱️ Rate limited, waiting {e.x} seconds...")
            await asyncio.sleep(e.x + 1)
            LOGGER.info("[CHANNEL-COORDINATOR] Resumed after FloodWait")
            
        except Exception as e:
            LOGGER.error(f"[CHANNEL-COORDINATOR] Processing error: {str(e)}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")

    async def _send_links_to_leech_system(self, message_links):
        """Send message links to internal leech system in controlled batches"""
        total_links = len(message_links)
        sent_count = 0
        
        LOGGER.info(f"[CHANNEL-COORDINATOR] Sending {total_links} links to internal leech system")
        
        # Process in batches
        for i in range(0, total_links, self.batch_size):
            if self.is_cancelled:
                break
                
            batch = message_links[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_links + self.batch_size - 1) // self.batch_size
            
            LOGGER.info(f"[CHANNEL-COORDINATOR] Processing batch {batch_num}/{total_batches}")
            
            # Update status
            await edit_message(
                self.status_message,
                f"🔄 **Sending files to leech system...**\n"
                f"**Progress:** {sent_count}/{total_links} files\n"
                f"**Batch:** {batch_num}/{total_batches}\n"
                f"**Current batch:** {len(batch)} files"
            )
            
            # Send each link in the batch
            for link_data in batch:
                if self.is_cancelled:
                    break
                
                try:
                    # Generate leech command with cleaned filename parameter
                    if self.use_caption_as_filename:
                        # Generate clean filename
                        clean_name = self._generate_clean_filename(
                            link_data['file_info'], link_data['message_id']
                        )
                        # Use -n parameter to specify custom filename
                        leech_cmd = f"/leech {link_data['url']} -n \"{clean_name}\""
                    else:
                        # Use original filename (internal system handles)
                        leech_cmd = f"/leech {link_data['url']}"
                    
                    # Send to internal leech system
                    await self._execute_internal_leech_command(leech_cmd)
                    
                    # Add to database to track as processed
                    await database.add_file_entry(
                        self.channel_id, 
                        link_data['message_id'], 
                        link_data['file_info']
                    )
                    
                    sent_count += 1
                    
                    LOGGER.info(f"[CHANNEL-COORDINATOR] Queued: {leech_cmd}")
                    
                    # Small delay between individual links
                    await asyncio.sleep(self.link_delay)
                    
                except Exception as e:
                    LOGGER.error(f"[CHANNEL-COORDINATOR] Failed to send {link_data['url']}: {e}")
            
            # Delay between batches
            if i + self.batch_size < total_links and not self.is_cancelled:
                LOGGER.info(f"[CHANNEL-COORDINATOR] Waiting {self.batch_delay}s before next batch...")
                await asyncio.sleep(self.batch_delay)
        
        LOGGER.info(f"[CHANNEL-COORDINATOR] Finished sending {sent_count}/{total_links} links")

    async def _execute_internal_leech_command(self, leech_command):
        """Execute leech command through internal bot system"""
        try:
            # Import your bot's leech handler
            from ..modules.mirror_leech import mirror_leech_cmd
            
            # Create simulated message object with the leech command
            fake_message = type('obj', (object,), {
                'text': leech_command,
                'from_user': self.message.from_user,
                'chat': self.message.chat,
                'reply_to_message': None,
            })
            
            # Execute leech command through internal system
            await mirror_leech_cmd(self.client, fake_message)
            
        except Exception as e:
            LOGGER.error(f"[CHANNEL-COORDINATOR] Internal leech execution failed: {e}")
            # Alternative: You might need to implement alternative method based on your bot architecture

    def _generate_clean_filename(self, file_info, message_id):
        """Generate clean filename with proper extension handling"""
        original_filename = file_info['file_name']
        
        if self.use_caption_as_filename:
            # Try to extract filename from caption or search text
            caption_text = file_info.get('search_text', original_filename)
            first_line = caption_text.split('\n')[0].strip()
            
            if first_line and len(first_line) >= 3:
                # Clean the caption text - creates clean dot-separated names
                clean_name = sanitize_filename(first_line)
                if clean_name:
                    # Get extension from original filename
                    original_extension = os.path.splitext(original_filename)[1]
                    
                    # Add extension if not present
                    if not clean_name.lower().endswith(original_extension.lower()):
                        clean_name = clean_name + original_extension
                    
                    # Add message ID for uniqueness (before extension)
                    name_part = os.path.splitext(clean_name)[0]
                    ext_part = os.path.splitext(clean_name)[1]
                    return f"{name_part}.{message_id}{ext_part}"
        
        # Fallback to sanitized original filename
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
        LOGGER.info(f"[CHANNEL-COORDINATOR] Task cancelled for {self.channel_id}")

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
async def channel_leech_cmd(client, message):
    """Handle /cleech command - uses internal leech system via message links"""
    await ChannelLeechCoordinator(client, message).new_event()

# Register handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
