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
    # Remove emojis first
    filename = remove_emoji(filename)
    
    # Replace underscores with spaces
    filename = re.sub(r'[_]+', ' ', filename)
    
    # Replace unwanted characters with spaces (not dots)
    filename = re.sub(r'[^\w\d\.\s-]', ' ', filename)
    
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

class ChannelScanListener(TaskListener):
    def __init__(self, client, message, channel_id, filter_tags, use_caption_as_filename=True, max_concurrent=2, check_interval=5):
        self.client = client
        self.message = message
        self.channel_id = channel_id
        self.filter_tags = filter_tags
        self.use_caption_as_filename = use_caption_as_filename
        self.max_concurrent = max_concurrent
        self.check_interval = check_interval
        self.status_message = None
        self.operation_key = None
        self.is_cancelled = False
        
        super().__init__()

    async def new_event(self):
        """Main event handler for channel scanning"""
        if not user:
            await send_message(self.message, "User session required!")
            return

        try:
            chat = await user.get_chat(self.channel_id)
        except Exception as e:
            await send_message(self.message, f"❌ Could not resolve channel: {e}")
            return

        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_scan"
        )

        filter_text = f" with filter: `{' '.join(self.filter_tags)}`" if self.filter_tags else ""
        
        self.status_message = await send_message(
            self.message,
            f"🔍 **Scanning Channel:** `{self.channel_id}`{filter_text}"
        )

        try:
            await self._scan_and_leech()
        except Exception as e:
            LOGGER.error(f"Error during channel scan: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _scan_and_leech(self):
        """Scans the channel and initiates leech for new files."""
        try:
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)
            files_to_download = []
            
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    break
                    
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
                    continue
                
                files_to_download.append(file_info)

            total_files = len(files_to_download)
            if total_files == 0:
                await edit_message(self.status_message, "✅ No new files to download!")
                return
            
            await edit_message(self.status_message, f"✅ Found {total_files} new files. Starting download...")
            
            for i, file_info in enumerate(files_to_download):
                if self.is_cancelled:
                    break

                # The fix for the duplicate filename is here
                if self.use_caption_as_filename and file_info.get('caption'):
                    filename = sanitize_filename(file_info['caption'])
                    original_ext = os.path.splitext(file_info['file_name'])[1]
                    if not filename.endswith(original_ext):
                        filename += original_ext
                else:
                    filename = sanitize_filename(file_info['file_name'])

                leech_cmd = f"/leech {file_info['message_link']} -n \"{filename}\""
                
                # Send the leech command
                await self.client.send_message(chat_id=self.message.chat.id, text=leech_cmd)
                
                # Small delay to avoid flooding
                await asyncio.sleep(2)
                
                # Update status
                await edit_message(self.status_message, f"📥 Downloading {i+1}/{total_files}: `{file_info['file_name']}`")

            await edit_message(self.status_message, "✅ All download commands have been sent.")

        except Exception as e:
            LOGGER.error(f"Error in _scan_and_leech: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")

    def cancel_task(self):
        self.is_cancelled = True
        # LOGGER.info(f"Task cancelled for {self.channel_id}")


@new_task
async def channel_leech(client, message):
    """Handler for the /cleech command"""
    text = message.text.split()
    args = _parse_arguments(text[1:])

    if 'channel' not in args:
        usage_text = (
            "**Usage:** `/cleech -ch <channel_id> [-f filter_text] [--no-caption]`\n\n"
            "**Examples:**\n"
            "`/cleech -ch @movies_channel`\n"
            "`/cleech -ch @movies_channel -f 2024 BluRay`"
        )
        await send_message(message, usage_text)
        return

    channel_id = args['channel']
    filter_tags = args.get('filter', [])
    use_caption = not args.get('no_caption', False)

    listener = ChannelScanListener(client, message, channel_id, filter_tags, use_caption_as_filename=use_caption)
    await listener.new_event()


def _parse_arguments(args):
    """Helper to parse command arguments"""
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

# Register the handler
bot.add_handler(MessageHandler(channel_leech, filters=command("cleech") & CustomFilters.authorized))
