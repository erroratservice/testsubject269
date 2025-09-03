import asyncio
import time
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import RPCError
from pyrogram.types import Message

from bot import bot, userbot, LOGGER, config_dict
from bot.helper.ext_utils.bot_utils import get_readable_time, new_task, sync_to_async
from bot.helper.ext_utils.db_handler import db_handler
from bot.helper.telegram_helper.message_utils import send_message, edit_message, send_status_message
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.mirror_leech_utils.status_utils.scan_status import ScanStatus

@new_task
async def scan_channel(_, message: Message):
    if len(message.command) < 2:
        await send_message(message, f"<b>Usage:</b> /{BotCommands.ScanCommand} &lt;channel_id&gt;")
        return

    try:
        channel_id = int(message.command[1])
    except ValueError:
        channel_id = message.command[1]

    try:
        if userbot:
            await userbot.get_chat(channel_id)
            client = userbot
        else:
            await bot.get_chat(channel_id)
            client = bot
    except Exception as e:
        await send_message(message, f"<b>Error:</b> {e}")
        return

    status_message = await send_message(message, "<i>Scanning channel...</i>")
    start_time = time.time()
    
    try:
        total_messages = await client.get_chat_history_count(channel_id)
    except Exception as e:
        await edit_message(status_message, f"<b>Error getting message count:</b> {e}")
        return

    scan_status = ScanStatus(status_message, total_messages, 0, start_time)
    await send_status_message(message)
    
    processed_messages = 0
    added_files = 0
    
    async for msg in client.get_chat_history(channel_id):
        processed_messages += 1
        if msg.caption:
            file_name = msg.caption.split('\n')[0].strip()
            if await db_handler.add_scanned_file(file_name):
                added_files += 1

        if processed_messages % 200 == 0:
            scan_status.processed = processed_messages
            # No need for a separate updater task, we can update directly in the loop
            # This is more efficient than creating a separate async task for it.
            
    elapsed = time.time() - start_time
    await edit_message(status_message, f"<b>Scan completed in {get_readable_time(elapsed)}.</b>\n\n"
                                       f"<b>Total messages scanned:</b> {processed_messages}\n"
                                       f"<b>New files added to database:</b> {added_files}")
    scan_status.cancel() # To remove it from the status message

bot.add_handler(MessageHandler(scan_channel, filters=filters.command(BotCommands.ScanCommand) & (CustomFilters.owner | CustomFilters.sudo)))
