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
    """Universal coordinator for channel leech operations with failed download handling"""
    
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
        self.failed_count = 0
        self.total_files = 0
        self.link_to_file_mapping = {}
        self._last_status_text = ""
        self.max_retries = 3
        self.retry_failed_only = False
        super().__init__()

    async def new_event(self):
        """Main event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])
        
        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption] [--retry-failed]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch @movies_channel --retry-failed`"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])
        self.use_caption_as_filename = not args.get('no_caption', False)
        self.retry_failed_only = args.get('retry_failed', False)

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
        mode_text = " (Retry Failed Only)" if self.retry_failed_only else ""
        self.status_message = await send_message(
            self.message,
            f"**Enhanced Channel Leech Initializing...**{mode_text}\n"
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
        """Enhanced coordination with failed file handling"""
        
        if self.retry_failed_only:
            # Only retry previously failed files
            await edit_message(self.status_message, f"**Retrying Failed Files...**")
            await self._retry_failed_files()
            await self._process_pending_files()
        else:
            # Normal processing with retry integration
            await edit_message(self.status_message, f"**Starting Enhanced Channel Leech...**")
            
            # First, retry any previously failed files
            await self._retry_failed_files()
            
            # Then process new files
            await self._scan_and_process_channel()

    async def _scan_and_process_channel(self):
        """Scan channel and process files in batches"""
        scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
        
        # Batch configuration
        batch_size = 30
        batch_sleep = 2
        processed_messages = 0
        skipped_duplicates = 0
        total_downloaded = 0
        
        # Get message iterator
        message_iterator = user.get_chat_history(self.channel_id)
        current_batch = []
        
        try:
            async for message in message_iterator:
                if self.is_cancelled:
                    break
                    
                processed_messages += 1
                current_batch.append(message)
                
                # Process batch when it reaches batch_size
                if len(current_batch) >= batch_size:
                    batch_downloaded, batch_skipped = await self._process_and_download_batch(current_batch, scanner, processed_messages)
                    total_downloaded += batch_downloaded
                    skipped_duplicates += batch_skipped
                    
                    # Clear batch and sleep
                    current_batch = []
                    await asyncio.sleep(batch_sleep)
                    
            # Process remaining messages in the last batch
            if current_batch and not self.is_cancelled:
                batch_downloaded, batch_skipped = await self._process_and_download_batch(current_batch, scanner, processed_messages)
                total_downloaded += batch_downloaded
                skipped_duplicates += batch_skipped
                
            # Show final results
            await self._show_batch_final_results(processed_messages, total_downloaded, skipped_duplicates)
            
        except Exception as e:
            LOGGER.error(f"[cleech] Batch processing error: {e}")
            raise

    async def _process_and_download_batch(self, message_batch, scanner, processed_so_far):
        """Process a batch of messages, download immediately, and wait for completion"""
        batch_files = []
        batch_skipped = 0
        
        # Step 1: Scan batch and identify new files
        for message in message_batch:
            if self.is_cancelled:
                break
                
            file_info = await scanner._extract_file_info(message)
            if not file_info:
                continue
                
            if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                continue
            
            # Check if file exists using proper keyword arguments
            file_exists = False
            if file_info.get('file_unique_id'):
                # Check both completed and failed files
                file_exists = await database.check_file_exists(file_unique_id=file_info.get('file_unique_id'))
                
                # Also check if it's in failed files and hasn't exceeded retry limit
                if not file_exists:
                    failed_info = await database.get_failed_file_info(file_info.get('file_unique_id'))
                    if failed_info and failed_info.get('retry_count', 0) >= self.max_retries:
                        file_exists = True  # Don't retry files that have exceeded max retries
                        LOGGER.info(f"[cleech-debug] File exceeded max retries: {file_info.get('file_name')}")
                
                LOGGER.info(f"[cleech-debug] Checking by unique_id: {file_info.get('file_unique_id')} - Found: {file_exists}")
            elif file_info.get('file_hash'):
                file_exists = await database.check_file_exists(file_hash=file_info.get('file_hash'))
                LOGGER.info(f"[cleech-debug] Checking by hash: {file_info.get('file_hash')} - Found: {file_exists}")
            elif file_info.get('file_name'):
                file_exists = await database.check_file_exists(file_name=file_info.get('file_name'))
                LOGGER.info(f"[cleech-debug] Checking by name: {file_info.get('file_name')} - Found: {file_exists}")
            
            if file_exists:
                batch_skipped += 1
                LOGGER.info(f"[cleech-debug] Skipping duplicate: {file_info.get('file_name')}")
                continue
            else:
                LOGGER.info(f"[cleech-debug] New file found: {file_info.get('file_name')}")

            if str(self.channel_chat_id).startswith('-100'):
                message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{message.id}"
            else:
                message_link = f"https://t.me/{self.channel_id.replace('@', '')}/{message.id}"
            
            batch_files.append({
                'url': message_link,
                'filename': file_info['file_name'],
                'message_id': message.id,
                'file_info': file_info,
                'link': message_link
            })
        
        # Step 2: Update progress
        await self._update_batch_progress(processed_so_far, len(batch_files), batch_skipped)
        
        # Step 3: Download all files in this batch and wait for completion
        batch_downloaded = 0
        if batch_files:
            batch_downloaded = await self._download_batch_and_wait(batch_files)
        
        return batch_downloaded, batch_skipped

    async def _retry_failed_files(self):
        """Retry files that failed to download completely"""
        try:
            # Get failed files that haven't exceeded retry limit
            failed_files = await database.get_failed_files_for_retry(self.channel_chat_id, self.max_retries)
            
            if failed_files:
                LOGGER.info(f"[cleech-debug] Found {len(failed_files)} failed files to retry")
                
                for failed_file in failed_files:
                    if self.is_cancelled:
                        break
                    
                    # Add back to pending files for retry
                    message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{failed_file['message_id']}"
                    
                    # Reconstruct file_info from failed file data
                    file_info = {
                        'file_name': failed_file['file_name'],
                        'file_unique_id': failed_file['file_unique_id'],
                        'file_size': failed_file['expected_size'],
                        'caption_first_line': failed_file.get('caption_first_line', ''),
                        'file_hash': failed_file.get('file_hash', ''),
                        'search_text': failed_file.get('search_text', '')
                    }
                    
                    self.pending_files.append({
                        'url': message_link,
                        'filename': failed_file['file_name'],
                        'message_id': failed_file['message_id'],
                        'file_info': file_info,
                        'link': message_link,
                        'is_retry': True,
                        'retry_count': failed_file.get('retry_count', 0)
                    })
                    
                    # Update retry count
                    await database.update_failed_file_retry(failed_file['file_unique_id'])
                    
        except Exception as e:
            LOGGER.error(f"[cleech] Error retrying failed files: {e}")

    async def _process_pending_files(self):
        """Process all pending files (used for retry-only mode)"""
        if not self.pending_files:
            await edit_message(self.status_message, "**No failed files to retry!**")
            return
            
        self.total_files = len(self.pending_files)
        await edit_message(self.status_message, f"**Processing {self.total_files} failed files for retry...**")
        
        # Start downloads up to max concurrent
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        
        # Wait for all downloads to complete
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            await self._update_progress()
            await asyncio.sleep(self.check_interval)
            
            completed_links = await self._check_completed_via_database()
            for _ in completed_links:
                if self.pending_files and len(self.our_active_links) < self.max_concurrent:
                    await self._start_next_download()
        
        await self._show_final_results()

    async def _download_batch_and_wait(self, batch_files):
        """Download all files in a batch and wait for completion before returning"""
        self.pending_files.extend(batch_files)
        downloaded_count = 0
        
        # Start downloads up to max concurrent
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        
        # Wait for all batch downloads to complete
        while self.our_active_links and not self.is_cancelled:
            await asyncio.sleep(5)  # Check every 5 seconds
            
            completed_links = await self._check_completed_via_database()
            downloaded_count += len(completed_links)
            
            # Start new downloads as slots become available
            for _ in completed_links:
                if self.pending_files and len(self.our_active_links) < self.max_concurrent:
                    await self._start_next_download()
        
        return downloaded_count

    async def _start_next_download(self):
        """Start next download - TaskListener will handle failure tracking"""
        if not self.pending_files: 
            return
            
        file_item = self.pending_files.pop(0)
        
        try:
            COMMAND_CHANNEL_ID = -1001791052293
            clean_name = self._generate_clean_filename(file_item['file_info'], file_item['message_id'])
            
            if file_item.get('is_retry', False):
                retry_count = file_item.get('retry_count', 0)
                clean_name = f"RETRY{retry_count}_{clean_name}"
            
            leech_cmd = f'/leech {file_item["url"]} -n {clean_name}'
            
            command_message = await user.send_message(chat_id=COMMAND_CHANNEL_ID, text=leech_cmd)
            command_msg_id = command_message.id
            
            actual_stored_url = f"https://t.me/c/{str(COMMAND_CHANNEL_ID)[4:]}/{command_msg_id}"
            
            await asyncio.sleep(5)
            
            self.our_active_links.add(actual_stored_url)
            self.link_to_file_mapping[actual_stored_url] = file_item
            
            LOGGER.info(f"[cleech-debug] Started download with tracking: {clean_name}")
            
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
                    
                    # Save completed file - TaskListener handles failure tracking
                    success = await self._save_completed_file(tracked_link)
                    
                    self.our_active_links.remove(tracked_link)
                    completed_links.append(tracked_link)
                    
                    if success:
                        self.completed_count += 1
                    else:
                        self.failed_count += 1
                    
        except Exception as e:
            LOGGER.error(f"[cleech] Error checking completion: {e}")
            import traceback
            LOGGER.error(f"[cleech] Traceback: {traceback.format_exc()}")
            
        return completed_links

    async def _save_completed_file(self, completed_link):
        """Save completed file to database - TaskListener handles failure tracking"""
        try:
            if completed_link not in self.link_to_file_mapping:
                LOGGER.error(f"[cleech-debug] No file mapping found for completed link: {completed_link}")
                return False
            
            file_item = self.link_to_file_mapping[completed_link]
            file_info = file_item['file_info']
            
            # Only save successful completions - TaskListener handles failures
            await database.add_file_entry(
                self.channel_chat_id,
                file_item['message_id'],
                file_info
            )
            
            LOGGER.info(f"[cleech-debug] Successfully saved completed file: {file_info.get('file_name')}")
            del self.link_to_file_mapping[completed_link]
            return True
            
        except Exception as e:
            LOGGER.error(f"[cleech] Error saving completed file: {e}")
            return False

    async def _update_batch_progress(self, processed_messages, found_files, skipped_duplicates):
        """Update progress during batch processing"""
        try:
            active_downloads = len(self.our_active_links)
            status_text = (
                f"**Enhanced Channel Leech**\n\n"
                f"**Scanned:** {processed_messages} messages\n"
                f"**Found:** {found_files} new files | **Skipped:** {skipped_duplicates} duplicates\n"
                f"**Active Downloads:** {active_downloads}/{self.max_concurrent}\n"
                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
            )
            
            if self._last_status_text != status_text:
                await edit_message(self.status_message, status_text)
                self._last_status_text = status_text
                
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"[cleech] Batch progress update error: {e}")

    async def _update_progress(self):
        if not self.status_message: 
            return
            
        try:
            progress = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
            text = (
                f"**Enhanced Channel Leech in Progress**\n\n"
                f"**Progress:** {self.completed_count}/{self.total_files} ({progress:.1f}%)\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent} | **Pending:** {len(self.pending_files)}\n"
                f"**Failed:** {self.failed_count}\n\n"
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

    async def _show_batch_final_results(self, processed_messages, total_downloaded, skipped_duplicates):
        """Show final results for batch processing"""
        success_rate = (total_downloaded / processed_messages * 100) if processed_messages > 0 else 0
        text = (
            f"**Channel Leech Completed!**\n\n"
            f"**Scanned:** {processed_messages} messages\n"
            f"**Downloaded:** {total_downloaded} | **Skipped:** {skipped_duplicates}\n"
            f"**Failed:** {self.failed_count} | **Success Rate:** {success_rate:.1f}%\n\n"
            f"Use `/cleech -ch {self.channel_id} --retry-failed` to retry failed downloads"
        )
        await edit_message(self.status_message, text)

    async def _show_final_results(self):
        success_rate = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
        failed_rate = (self.failed_count / self.total_files * 100) if self.total_files > 0 else 0
        text = (
            f"**Enhanced Channel Leech Completed!**\n\n"
            f"**Total:** {self.total_files} | **Success:** {self.completed_count} ({success_rate:.1f}%)\n"
            f"**Failed:** {self.failed_count} ({failed_rate:.1f}%)\n\n"
            f"Use `/cleech -ch {self.channel_id} --retry-failed` to retry failed downloads"
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
            elif args[i] == '--retry-failed':
                parsed['retry_failed'] = True; i+=1
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

class FailedFileManager(TaskListener):
    """Manager for failed file operations"""
    
    def __init__(self, client, message):
        self.client = client
        self.message = message
        super().__init__()

    async def new_event(self):
        """Handle failed file management commands"""
        text = self.message.text.split()
        
        if len(text) < 2:
            usage_text = (
                "**Usage:** `/failed <command> [channel_id]`\n\n"
                "**Commands:**\n"
                "`/failed stats` - Show failed files statistics\n"
                "`/failed stats @channel` - Show stats for specific channel\n"
                "`/failed list @channel` - List failed files for channel\n"
                "`/failed clear @channel` - Clear failed files for channel"
            )
            await send_message(self.message, usage_text)
            return

        command = text[1].lower()
        channel_id = text[2] if len(text) > 2 else None

        if command == "stats":
            await self._show_failed_stats(channel_id)
        elif command == "list" and channel_id:
            await self._list_failed_files(channel_id)
        elif command == "clear" and channel_id:
            await self._clear_failed_files(channel_id)
        else:
            await send_message(self.message, "Invalid command or missing channel ID")

    async def _show_failed_stats(self, channel_id):
        """Show failed files statistics"""
        try:
            if channel_id:
                chat = await user.get_chat(channel_id)
                channel_chat_id = chat.id
                stats = await database.get_failed_files_stats(channel_chat_id)
                title = f"**Failed Files Stats - {channel_id}**"
            else:
                stats = await database.get_failed_files_stats()
                title = "**Global Failed Files Stats**"
            
            if not stats or stats.get('total_failed', 0) == 0:
                await send_message(self.message, f"{title}\n\nNo failed files found.")
                return
            
            text = f"{title}\n\n"
            text += f"**Total Failed:** {stats.get('total_failed', 0)}\n\n"
            
            if stats.get('by_failure_reason'):
                text += "**By Failure Reason:**\n"
                for reason in stats['by_failure_reason']:
                    text += f"• {reason['_id']}: {reason['count']}\n"
                text += "\n"
            
            if stats.get('by_retry_count'):
                text += "**By Retry Count:**\n"
                for retry in stats['by_retry_count']:
                    text += f"• {retry['_id']} retries: {retry['count']}\n"
            
            await send_message(self.message, text)
            
        except Exception as e:
            await send_message(self.message, f"Error getting stats: {str(e)}")

    async def _list_failed_files(self, channel_id):
        """List recent failed files for a channel"""
        try:
            chat = await user.get_chat(channel_id)
            channel_chat_id = chat.id
            
            failed_files = await database.get_failed_files_for_retry(channel_chat_id, max_retries=10)
            
            if not failed_files:
                await send_message(self.message, f"No failed files found for {channel_id}")
                return
            
            text = f"**Recent Failed Files - {channel_id}**\n\n"
            for i, f in enumerate(failed_files[:10]):  # Show only first 10
                text += f"{i+1}. {f['file_name']}\n"
                text += f"   Retries: {f.get('retry_count', 0)} | Reason: {f.get('failure_reason', 'unknown')}\n\n"
            
            if len(failed_files) > 10:
                text += f"... and {len(failed_files) - 10} more"
            
            await send_message(self.message, text)
            
        except Exception as e:
            await send_message(self.message, f"Error listing failed files: {str(e)}")

    async def _clear_failed_files(self, channel_id):
        """Clear failed files for a channel"""
        try:
            chat = await user.get_chat(channel_id)
            channel_chat_id = chat.id
            
            # Get count before clearing
            stats = await database.get_failed_files_stats(channel_chat_id)
            count = stats.get('total_failed', 0)
            
            if count == 0:
                await send_message(self.message, f"No failed files found for {channel_id}")
                return
            
            # Clear failed files (would need to implement this method in database)
            # For now, just show what would be cleared
            await send_message(self.message, f"Would clear {count} failed files for {channel_id}\n(Clear method not implemented yet)")
            
        except Exception as e:
            await send_message(self.message, f"Error clearing failed files: {str(e)}")

@new_task
async def channel_scan(client, message):
    """Handle /scan command with task ID support"""
    await ChannelScanListener(user, message).new_event()

@new_task
async def universal_channel_leech_cmd(client, message):
    """Handle /cleech with enhanced batch processing and failed file handling"""
    await UniversalChannelLeechCoordinator(client, message).new_event()

@new_task
async def failed_file_manager_cmd(client, message):
    """Handle /failed command for failed file management"""
    await FailedFileManager(client, message).new_event()

# Register ALL handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    universal_channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    failed_file_manager_cmd,
    filters=command("failed") & CustomFilters.authorized
))
