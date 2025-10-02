from pyrogram.filters import command, chat, document, video
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.types import Message
from bot import bot, user, LOGGER, config_dict, user_data
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
from datetime import datetime, timezone

def sanitize_filename(filename):
    """Clean filename sanitization with proper dot conversion"""
    import re
    emoji_pattern = re.compile(
        r'['
        r'\U0001F600-\U0001F64F'
        r'\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF'
        r'\U00002702-\U000027B0'
        r'\U000024C2-\U0001F251'
        r'\U0001F900-\U0001F9FF'
        r'\U0001FA00-\U0001FA6F'
        r'\U0001F1E0-\U0001F1FF'
        r'\u2600-\u26FF\u2700-\u27BF'
        r']+', flags=re.UNICODE
    )
    filename = emoji_pattern.sub('', filename)
    filename = filename.replace('+', '.')
    filename = filename.replace('_', '.')
    filename = filename.replace(' ', '.')
    filename = re.sub(r'[\[\]\(\)\{\}]', '', filename)
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'\.{2,}', '.', filename)
    filename = filename.strip('.')
    if not filename:
        filename = "file"
    return filename

class DestinationWatcher:
    """
    Monitors the leech destination for new files using a specific MessageHandler
    to reliably capture new uploads without stressing the user session.
    """
    def __init__(self, client_session, destination_id, start_time):
        self.client = client_session
        self.destination_id = destination_id
        self.start_time = start_time
        self.verified_files = set()
        # Use a specific MessageHandler for reliability on the BOT session
        filters = (document | video) & chat(self.destination_id)
        self.handler = MessageHandler(self._on_new_message, filters=filters)
        self._is_active = False

    async def _on_new_message(self, client, message: Message):
        """Callback for new document or video messages."""
        if message.date < self.start_time:
            return

        sanitized_name = None
        # Prioritize caption for filename, as per uploader's behavior
        if message.caption:
            caption_first_line = message.caption.split('\n')[0].strip()
            sanitized_name = sanitize_filename(caption_first_line)
        
        if not sanitized_name:
            file_name = getattr(message.document or message.video, 'file_name', None)
            if file_name:
                sanitized_name = sanitize_filename(file_name)

        if sanitized_name:
            LOGGER.info(f"[Watcher] Detected new file in destination: {sanitized_name}")
            self.verified_files.add(sanitized_name)

    def start(self):
        """Starts the watcher on the specified client session."""
        if not self._is_active:
            self.client.add_handler(self.handler)
            self._is_active = True
            LOGGER.info(f"[Watcher] Started listening for media in destination chat {self.destination_id} using BOT session.")

    def stop(self):
        """Stops the watcher."""
        if self._is_active:
            self.client.remove_handler(self.handler)
            self._is_active = False
            LOGGER.info(f"[Watcher] Stopped listening in destination chat: {self.destination_id}")

    def is_verified(self, sanitized_name):
        return sanitized_name in self.verified_files

    def start(self):
        """Starts the watcher's background polling task."""
        if not self._is_active:
            self._is_active = True
            self._task = asyncio.create_task(self._monitor_destination())
            session_name = "BOT" if hasattr(self.client, 'me') and self.client.me.is_bot else "USER"
            LOGGER.info(f"[Watcher] Started actively polling destination {self.destination_id} using {session_name} session.")

    def stop(self):
        """Stops the watcher's background task."""
        if self._is_active:
            self._is_active = False
            if self._task:
                self._task.cancel()
            LOGGER.info(f"[Watcher] Stopped polling destination: {self.destination_id}")

    def is_verified(self, sanitized_name):
        return sanitized_name in self.verified_files

class SimpleChannelLeechCoordinator(TaskListener):
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.channel_chat_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        self.use_caption_as_filename = True
        self.max_concurrent = 5
        self.check_interval = 15
        self.pending_files = []
        self.pending_file_ids = set()
        self.pending_sanitized_names = set()
        self.our_active_links = set()
        self.completed_count = 0
        self.failed_count = 0
        self.total_files = 0
        self.link_to_file_mapping = {}
        self._last_status_text = ""
        self.resume_mode = False
        self.resume_from_msg_id = None
        self.scanned_message_ids = set()
        self.last_success_msg_id = None
        self.start_time = datetime.now(timezone.utc)
        self.watcher = None
        super().__init__()

    async def new_event(self):
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

        if not await self._initialize_watcher():
            return

        progress = await database.get_leech_progress(self.message.from_user.id, self.channel_id)
        if progress:
            self.resume_mode = True
            self.scanned_message_ids = set(progress.get("scanned_message_ids", []))
            
            db_resume_id = progress.get("last_success_msg_id")
            LOGGER.info(f"[cleech_debug] Value of 'last_success_msg_id' from database: {db_resume_id}")

            if db_resume_id and isinstance(db_resume_id, int):
                self.resume_from_msg_id = db_resume_id
                await send_message(self.message, f"⏸️ Resuming from last successful download at message {self.resume_from_msg_id}.")
            elif self.scanned_message_ids:
                self.resume_from_msg_id = min(self.scanned_message_ids)
                await send_message(self.message, f"⏸️ No downloads finished. Resuming scan from last checked message {self.resume_from_msg_id}.")
            else:
                self.resume_from_msg_id = 0
                await send_message(self.message, "⚠️ Could not find any valid resume point. Starting scan from the newest message.")
            
            LOGGER.info(f"[cleech_debug] Final 'resume_from_msg_id' to be used: {self.resume_from_msg_id}")
        else:
            self.scanned_message_ids = set()
            self.resume_from_msg_id = None

        try:
            chat = await user.get_chat(self.channel_id)
            self.channel_chat_id = chat.id
        except Exception as e:
            await send_message(self.message, f"Could not resolve channel: {e}")
            self.watcher.stop()
            return

        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "simple_channel_leech"
        )

        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        self.status_message = await send_message(
            self.message,
            f"**Channel Leech Starting{' (Resumed)' if self.resume_mode else ''}...**\n"
            f"**Channel:** `{self.channel_id}` → `{self.channel_chat_id}`\n"
            f"**Filter:**{filter_text}"
        )

        try:
            await self._coordinate_simple_leech()
        except Exception as e:
            LOGGER.error(f"[cleech] Coordination Error: {e}")
            await edit_message(self.status_message, f"Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)
            if self.watcher:
                self.watcher.stop()
            await database.clear_leech_progress(self.message.from_user.id, self.channel_id)

    async def _initialize_watcher(self):
        """Determines destination and starts the watcher exclusively on the bot session."""
        destination_id = await self._get_leech_destination()
        if not destination_id:
            await send_message(self.message, "❌ Could not determine leech destination. Please set `LEECH_DUMP_CHAT` or your personal `leech_dest`.")
            return False

        try:
            # Check if the bot is a member. This is a mandatory requirement now.
            await bot.get_chat_member(destination_id, bot.me.id)
            LOGGER.info(f"[cleech] Bot has access to destination {destination_id}. Using BOT session for watcher.")
            self.watcher = DestinationWatcher(bot, destination_id, self.start_time)
            self.watcher.start()
            return True
        except UserNotParticipant:
            await send_message(self.message, f"❌ Bot is not a member of the destination channel ({destination_id}). Please add it and try again.")
            return False
        except Exception as e:
            LOGGER.error(f"[cleech] Error initializing watcher: {e}")
            await send_message(self.message, f"❌ An error occurred while setting up the destination watcher: {e}")
            return False

    async def _get_leech_destination(self):
        """Determines the correct leech destination chat ID in line with db_handler.py."""
        try:
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            bot_settings = await database._db.settings.config.find_one({"_id": bot_token_first_half})
            if bot_settings and bot_settings.get('LEECH_DUMP_CHAT'):
                dump_chat_id = bot_settings['LEECH_DUMP_CHAT']
                if isinstance(dump_chat_id, str) and dump_chat_id.strip():
                    LOGGER.info(f"[cleech] Using global LEECH_DUMP_CHAT: {dump_chat_id}")
                    return int(dump_chat_id)

            if user:
                user_session_id = user.me.id
                data = user_data.get(user_session_id, {})
                user_dest_id = data.get('leech_dest')
                if isinstance(user_dest_id, str) and user_dest_id.strip():
                    LOGGER.info(f"[cleech] Using user-specific leech_destination: {user_dest_id}")
                    return int(user_dest_id)
            
            LOGGER.warning("[cleech] No leech destination configured.")
            return None
        except Exception as e:
            LOGGER.error(f"Error getting leech destination: {e}")
            return None

    async def _coordinate_simple_leech(self):
        await edit_message(self.status_message, f"**Starting Channel Leech...**")
        scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
        batch_size = 30
        batch_sleep = 2
        processed_messages = 0
        skipped_duplicates = 0
        completion_task_started = False
        completion_task = None

        if self.resume_mode:
            await self._restore_resume_state(scanner)
            LOGGER.info(f"[cleech] Restored {len(self.pending_files)} pending, {len(self.our_active_links)} active")
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            if self.our_active_links or self.pending_files:
                LOGGER.info(f"[cleech] Starting completion check task immediately")
                completion_task = asyncio.create_task(self._wait_for_completion())
                completion_task_started = True

        offset_id = 0
        if self.resume_from_msg_id:
            offset_id = self.resume_from_msg_id
            LOGGER.info(f"[cleech] Resuming scan from message ID: {offset_id}")
        
        # --- CORRECTED EFFICIENT SCANNING LOGIC ---
        from pyrogram.enums import MessagesFilter

        # This tells Pyrogram to only fetch messages that are documents OR videos by passing a list
        message_iterator = user.search_messages(
            chat_id=self.channel_id,
            offset=offset_id,
            filter=[MessagesFilter.DOCUMENT, MessagesFilter.VIDEO] # Corrected syntax
        )
        # --- END OF CORRECTION ---

        current_batch = []
        skip_count = 0

        try:
            async for message in message_iterator:
                if message.id in self.scanned_message_ids:
                    skip_count += 1
                    if skip_count % 100 == 0:
                        LOGGER.info(f"[cleech] Skipped {skip_count} already-scanned media messages...")
                    continue
                if self.is_cancelled:
                    break
                if skip_count > 0:
                    LOGGER.info(f"[cleech] Finished skipping {skip_count} media messages, processing new ones")
                    skip_count = 0
                processed_messages += 1
                current_batch.append(message)
                self.scanned_message_ids.add(message.id)
                if not completion_task_started and (self.our_active_links or self.pending_files):
                    LOGGER.info(f"[cleech] Starting completion check task")
                    completion_task = asyncio.create_task(self._wait_for_completion())
                    completion_task_started = True
                if len(current_batch) >= batch_size:
                    batch_skipped = await self._process_batch(current_batch, scanner, processed_messages)
                    skipped_duplicates += batch_skipped
                    current_batch = []
                    await asyncio.sleep(batch_sleep)
                    await self._save_progress()
            if current_batch and not self.is_cancelled:
                batch_skipped = await self._process_batch(current_batch, scanner, processed_messages)
                skipped_duplicates += batch_skipped
                await self._save_progress()
            if completion_task_started:
                await completion_task
            elif self.our_active_links or self.pending_files:
                await self._wait_for_completion()
            await self._show_final_results(processed_messages, skipped_duplicates)
        except Exception as e:
            LOGGER.error(f"[cleech] Processing error: {e}")
            await self._save_progress(interrupted=True)
            raise

    async def _restore_resume_state(self, scanner):
        try:
            progress = await database.get_leech_progress(self.message.from_user.id, self.channel_id)
            if not progress:
                return
            pending_msg_ids = progress.get("pending_files", [])
            for msg_id in pending_msg_ids:
                try:
                    message = await user.get_messages(self.channel_id, msg_id)
                    file_info = await scanner._extract_file_info(message)
                    if file_info:
                        if str(self.channel_chat_id).startswith('-100'):
                            message_link = f"https://t.me/c/{str(self.channel_chat_id)[4:]}/{msg_id}"
                        else:
                            message_link = f"https://t.me/{self.channel_id.replace('@', '')}/{msg_id}"
                        self.pending_files.append({
                            'url': message_link,
                            'filename': file_info['file_name'],
                            'message_id': msg_id,
                            'file_info': file_info,
                            'link': message_link
                        })
                except Exception as e:
                    LOGGER.warning(f"[cleech] Could not restore pending file {msg_id}: {e}")
            
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    command_url = row["_id"]
                    self.our_active_links.add(command_url)
                    LOGGER.info(f"[cleech] Restored active download: {command_url}")
        except Exception as e:
            LOGGER.error(f"[cleech] Error restoring resume state: {e}")

    async def _process_batch(self, message_batch, scanner, processed_so_far):
        batch_skipped = 0
        for message in message_batch:
            if self.is_cancelled:
                break
            file_info = await scanner._extract_file_info(message)
            if not file_info:
                continue
            if self.filter_tags and not all(tag.lower() in file_info['search_text'].lower() for tag in self.filter_tags):
                continue
            file_unique_id = file_info.get('file_unique_id')
            sanitized_name = self._generate_clean_filename(file_info, message.id)
            file_exists_in_db = await database.check_file_exists(
                file_unique_id=file_unique_id,
                file_hash=file_info.get('file_hash'),
                file_info=file_info
            )
            if file_exists_in_db:
                batch_skipped += 1
                continue
            if sanitized_name in self.pending_sanitized_names:
                continue
            if file_unique_id and file_unique_id in self.pending_file_ids:
                continue
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
            self.pending_sanitized_names.add(sanitized_name)
            if file_unique_id:
                self.pending_file_ids.add(file_unique_id)
        while len(self.our_active_links) < self.max_concurrent and self.pending_files:
            await self._start_next_download()
        await self._update_progress(processed_so_far, batch_skipped)
        return batch_skipped

    async def _start_next_download(self):
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
            self.link_to_file_mapping[actual_stored_url] = file_item
        except Exception as e:
            LOGGER.error(f"[cleech] Error starting download: {e}")
            self.pending_files.insert(0, file_item)

    async def _wait_for_completion(self):
        self.total_files = self.completed_count + len(self.pending_files) + len(self.our_active_links)
        while (self.our_active_links or self.pending_files) and not self.is_cancelled:
            await self._check_and_verify_tasks()
            while len(self.our_active_links) < self.max_concurrent and self.pending_files:
                await self._start_next_download()
            await asyncio.sleep(self.check_interval)

    async def _check_and_verify_tasks(self):
        try:
            bot_token_first_half = config_dict['BOT_TOKEN'].split(':')[0]
            active_db_tasks = set()
            if await database._db.tasks[bot_token_first_half].find_one():
                rows = database._db.tasks[bot_token_first_half].find({})
                async for row in rows:
                    active_db_tasks.add(row["_id"])
            potentially_completed_links = self.our_active_links - active_db_tasks
            if not potentially_completed_links:
                return
            verification_coros = [self._verify_upload_in_destination(link) for link in potentially_completed_links]
            results = await asyncio.gather(*verification_coros)
            for link, is_success in zip(potentially_completed_links, results):
                self.our_active_links.remove(link)
                if is_success:
                    self.completed_count += 1
                    file_item = self.link_to_file_mapping.get(link)
                    if file_item:
                        self.last_success_msg_id = file_item["message_id"]
                        await database.add_file_entry(self.channel_chat_id, file_item['message_id'], file_item['file_info'])
                        del self.link_to_file_mapping[link]
                    await self._save_progress()
                else:
                    self.failed_count += 1
                    LOGGER.warning(f"[cleech] Verification failed for {link}. Marked as failed.")
        except Exception as e:
            LOGGER.error(f"[cleech] CRITICAL ERROR in task completion check: {e}")

    async def _verify_upload_in_destination(self, completed_link):
        file_item = self.link_to_file_mapping.get(completed_link)
        if not file_item:
            return False
        
        sanitized_name_to_check = self._generate_clean_filename(file_item['file_info'], file_item['message_id'])
        
        # --- VERIFICATION DEBUGGING ---
        LOGGER.info(f"[cleech_debug] Verifying file: '{sanitized_name_to_check}'")
        if self.watcher:
             LOGGER.info(f"[cleech_debug] Watcher's current verified list: {self.watcher.verified_files}")
        # --- END DEBUGGING ---

        if self.watcher and self.watcher.is_verified(sanitized_name_to_check):
            LOGGER.info(f"[cleech] VERIFIED upload for: {sanitized_name_to_check}")
            return True
        else:
            await asyncio.sleep(10)
            if self.watcher and self.watcher.is_verified(sanitized_name_to_check):
                LOGGER.info(f"[cleech] VERIFIED upload for (after delay): {sanitized_name_to_check}")
                return True

        LOGGER.warning(f"[cleech] FAILED verification for: {sanitized_name_to_check}. Not found in destination.")
        return False

    async def _save_progress(self, interrupted=False):
        progress = {
            "user_id": self.message.from_user.id,
            "channel_id": self.channel_id,
            "filter_tags": self.filter_tags,
            "last_success_msg_id": self.last_success_msg_id,
            "scanned_message_ids": list(self.scanned_message_ids),
            "pending_files": [f['message_id'] for f in self.pending_files],
            "timestamp": datetime.utcnow().isoformat(),
            "interrupted": interrupted
        }
        await database.save_leech_progress(self.message.from_user.id, self.channel_id, progress)

    async def _update_progress(self, processed_messages, skipped_duplicates):
        try:
            status_text = (
                f"**Channel Leech Progress**\n\n"
                f"**Scanned:** {processed_messages} messages\n"
                f"**Skipped:** {skipped_duplicates} duplicates\n"
                f"**Active:** {len(self.our_active_links)}/{self.max_concurrent}\n"
                f"**Pending:** {len(self.pending_files)}\n"
                f"**Completed:** {self.completed_count} | **Failed:** {self.failed_count}"
            )
            if self._last_status_text != status_text:
                await edit_message(self.status_message, status_text)
                self._last_status_text = status_text
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                LOGGER.error(f"[cleech] Progress update error: {e}")

    async def _show_final_results(self, processed_messages, skipped_duplicates):
        self.total_files = self.completed_count + self.failed_count
        success_rate = (self.completed_count / self.total_files * 100) if self.total_files > 0 else 0
        text = (
            f"**Channel Leech Completed!**\n\n"
            f"**Scanned:** {processed_messages} messages\n"
            f"**Skipped:** {skipped_duplicates} duplicates\n"
            f"**Downloaded:** {self.completed_count} | **Failed:** {self.failed_count}\n"
            f"**Success Rate:** {success_rate:.1f}%\n\n"
            f"Run `/cleech -ch {self.channel_id}` again to retry failed downloads."
        )
        await edit_message(self.status_message, text)

    def _generate_clean_filename(self, file_info, message_id):
        original_filename = file_info.get('file_name', '')
        base_name = original_filename
        if self.use_caption_as_filename and file_info.get('caption_first_line'):
            base_name = file_info['caption_first_line'].strip()
        clean_base = sanitize_filename(base_name)
        original_ext = os.path.splitext(original_filename)[1]
        media_extensions = {
            '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
            '.mp3', '.flac', '.wav', '.aac', '.m4a', '.ogg',
            '.zip', '.rar', '.7z', '.tar', '.gz'
        }
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
    def __init__(self, client, message):
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.scanner = None
        super().__init__()

    async def new_event(self):
        text = self.message.text.split()
        if len(text) < 2:
            usage_text = (
                "**Usage:** `/scan <channel_id> [filter]`\n\n"
                "**Examples:**\n"
                "`/scan @my_channel`\n"
                "`/scan @movies_channel movie`\n\n"
                "**Purpose:** Build file database for duplicate detection"
            )
            await send_message(self.message, usage_text)
            return
        self.channel_id = text[1]
        self.filter_tags = text[2:] if len(text) > 2 else []
        if not user:
            await send_message(self.message, "User session required!")
            return
        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        status_msg = await send_message(
            self.message, 
            f"Starting scan `{str(self.mid)[:12]}`\n"
            f"**Channel:** `{self.channel_id}`{filter_text}\n"
            f"**Cancel:** `/cancel {str(self.mid)[:12]}`"
        )
        try:
            self.scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            self.scanner.listener = self
            await self.scanner.scan(status_msg)
        except Exception as e:
            LOGGER.error(f"[CHANNEL-SCANNER] Error: {e}")
            await edit_message(status_msg, f"Scan failed: {str(e)}")

    def cancel_task(self):
        self.is_cancelled = True
        if self.scanner:
            self.scanner.running = False

@new_task
async def channel_scan(client, message):
    """Handle /scan command"""
    await ChannelScanListener(user, message).new_event()

@new_task
async def simple_channel_leech_cmd(client, message):
    """Handle /cleech with simple failure prevention"""
    await SimpleChannelLeechCoordinator(client, message).new_event()

bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))
bot.add_handler(MessageHandler(
    simple_channel_leech_cmd,
    filters=command("cleech") & CustomFilters.authorized
))
