from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from bot import bot, user, DOWNLOAD_DIR, LOGGER

from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.db_handler import database
from ..helper.telegram_helper.message_utils import send_message, edit_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_scanner import ChannelScanner
from ..helper.mirror_leech_utils.channel_status import channel_status
from ..helper.mirror_leech_utils.download_utils.telegram_download import TelegramDownloadHelper
from ..helper.listeners.task_listener import TaskListener

class ChannelLeech(TaskListener):
    def __init__(self, client, message):
        # Set attributes BEFORE calling super().__init__()
        self.client = client
        self.message = message
        self.channel_id = None
        self.filter_tags = []
        self.status_message = None
        self.operation_key = None
        
        # Now call parent constructor
        super().__init__()

    async def new_event(self):
        """Main channel leech event handler"""
        text = self.message.text.split()
        args = self._parse_arguments(text[1:])

        if 'channel' not in args:
            usage_text = (
                "**Usage:** `/cleech -ch <channel_id> [-f filter_text]`\n\n"
                "**Examples:**\n"
                "`/cleech -ch @movies_channel`\n"
                "`/cleech -ch @movies_channel -f 2024 BluRay`\n"
                "`/cleech -ch -1001234567890 -f movie`"
            )
            await send_message(self.message, usage_text)
            return

        self.channel_id = args['channel']
        self.filter_tags = args.get('filter', [])

        if not user:
            await send_message(self.message, "❌ User session is required for channel access!")
            return

        # Start operation tracking
        self.operation_key = await channel_status.start_operation(
            self.message.from_user.id, self.channel_id, "channel_leech"
        )

        # Send initial status
        filter_text = f" with filter: {' '.join(self.filter_tags)}" if self.filter_tags else ""
        self.status_message = await send_message(
            self.message, 
            f"🔄 Starting channel leech from `{self.channel_id}`{filter_text}"
        )

        try:
            await self._process_channel()
        except Exception as e:
            LOGGER.error(f"Channel leech error: {e}")
            await edit_message(self.status_message, f"❌ Error: {str(e)}")
        finally:
            if self.operation_key:
                await channel_status.stop_operation(self.operation_key)

    async def _process_channel(self):
        """Process channel messages and download files"""
        downloaded = 0
        skipped = 0
        processed = 0
        errors = 0

        try:
            # Get channel info
            chat = await user.get_chat(self.channel_id)
            await edit_message(
                self.status_message, 
                f"📋 Processing channel: **{chat.title}**\n🔍 Scanning messages..."
            )

            # Create scanner instance
            scanner = ChannelScanner(user, self.channel_id, filter_tags=self.filter_tags)

            # Process messages
            async for message in user.get_chat_history(self.channel_id):
                if self.is_cancelled:
                    break

                processed += 1

                # Update operation stats
                await channel_status.update_operation(
                    self.operation_key, processed=processed
                )

                # Extract file info
                file_info = await scanner._extract_file_info(message)
                if not file_info:
                    continue

                # Apply filter (change to 'any' for OR logic instead of 'all' for AND logic)
                if self.filter_tags:
                    search_text = file_info['search_text'].lower()
                    if not all(tag.lower() in search_text for tag in self.filter_tags):
                        continue

                # Check if file exists in database
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

                # Download file
                try:
                    await self._download_file(message, file_info)
                    downloaded += 1

                    # Add to database after successful download
                    await database.add_file_entry(
                        self.channel_id, message.id, file_info
                    )

                    await channel_status.update_operation(
                        self.operation_key, downloaded=downloaded
                    )

                except Exception as e:
                    errors += 1
                    LOGGER.error(f"Download failed for {file_info['file_name']}: {e}")
                    await channel_status.update_operation(
                        self.operation_key, errors=errors
                    )

                # Update status every 20 messages
                if processed % 20 == 0:
                    status_text = (
                        f"📊 **Progress Update**\n"
                        f"📋 Processed: {processed}\n"
                        f"⬇️ Downloaded: {downloaded}\n"
                        f"⏭️ Skipped: {skipped}\n"
                        f"❌ Errors: {errors}"
                    )
                    await edit_message(self.status_message, status_text)

            # Final status
            final_text = (
                f"✅ **Channel leech completed!**\n\n"
                f"📋 **Total processed:** {processed}\n"
                f"⬇️ **Downloaded:** {downloaded}\n"
                f"⏭️ **Skipped (duplicates):** {skipped}\n"
                f"❌ **Errors:** {errors}\n\n"
                f"🎯 **Channel:** `{self.channel_id}`"
            )
            await edit_message(self.status_message, final_text)

        except Exception as e:
            await self.on_download_error(f"Channel processing error: {str(e)}")

    async def _download_file(self, message, file_info):
        """Download individual file"""
        download_path = f"{DOWNLOAD_DIR}{self.mid}/"
        
        # Use existing telegram download helper
        telegram_helper = TelegramDownloadHelper(self)
        await telegram_helper.add_download(message, download_path, "user")

    def _parse_arguments(self, args):
        """Parse command arguments"""
        parsed = {}
        i = 0
        
        while i < len(args):
            if args[i] == '-ch' and i + 1 < len(args):
                parsed['channel'] = args[i + 1]
                i += 2
            elif args[i] == '-f' and i + 1 < len(args):
                # Handle quoted filter text
                filter_text = args[i + 1]
                if filter_text.startswith('"') and filter_text.endswith('"'):
                    filter_text = filter_text[1:-1]
                parsed['filter'] = filter_text.split()
                i += 2
            else:
                i += 1
                
        return parsed

@new_task
async def channel_scan(_, message):
    """Handle /scan command for building file database"""
    args = message.text.split()
    
    if len(args) < 2:
        usage_text = (
            "**Usage:** `/scan <channel_id> [filter]`\n\n"
            "**Examples:**\n"
            "`/scan @my_channel`\n"
            "`/scan -1001234567890`\n"
            "`/scan @movies_channel movie`"
        )
        await send_message(message, usage_text)
        return

    channel_id = args[1]
    filter_tags = args[2:] if len(args) > 2 else []

    if not user:
        await send_message(message, "❌ User session is required for channel scanning!")
        return

    # Start scanning
    status_msg = await send_message(message, f"🔍 Starting scan of `{channel_id}`...")
    
    scanner = ChannelScanner(user, channel_id, filter_tags=filter_tags)
    await scanner.scan(status_msg)

@new_task
async def channel_leech_cmd(client, message):
    """Handle /cleech command"""
    await ChannelLeech(client, message).new_event()

# Register handlers
bot.add_handler(MessageHandler(
    channel_scan,
    filters=command("scan") & CustomFilters.authorized
))

bot.add_handler(MessageHandler(
    channel_leech_cmd, 
    filters=command("cleech") & CustomFilters.authorized
))
