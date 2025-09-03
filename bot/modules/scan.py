import asyncio
import time
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import RPCError
from pyrogram.types import Message

from bot import bot, userbot, LOGGER, config_dict, task_manager
from bot.helper.ext_utils.bot_utils import get_readable_time, new_task
from bot.helper.ext_utils.db_handler import db_handler
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, sendStatusMessage
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.mirror_leech_utils.status_utils.scan_status import ScanStatus

@new_task
async def scan_channel(_, message: Message):
    if len(message.command) < 2:
        await sendMessage(message, f"<b>Usage:</b> /{BotCommands.ScanCommand} &lt;channel_id&gt;")
        return

    try:
        # Allow both channel username and ID
        channel_id = message.command[1]
        if channel_id.startswith('@'):
            channel_id = channel_id.replace('@', '')
        else:
            channel_id = int(channel_id)
    except (ValueError, IndexError):
        await sendMessage(message, "<b>Invalid channel ID or username.</b>")
        return

    try:
        if userbot:
            await userbot.get_chat(channel_id)
            client = userbot
        else:
            await sendMessage(message, "<b>USER_SESSION_STRING is not defined.</b>")
            return
    except Exception as e:
        await sendMessage(message, f"<b>Error:</b> {e}")
        return

    status_message = await sendMessage(message, "<i>Scanning channel...</i>")
    
    try:
        total_messages = await client.get_chat_history_count(channel_id)
    except Exception as e:
        await editMessage(status_message, f"<b>Error getting message count:</b> {e}")
        return

    scan_status = ScanStatus(status_message, total_messages, 0, time.time())
    await sendStatusMessage(message)
    task_manager.add_task(scan_status)
    
    processed_messages = 0
    added_files = 0
    
    async for msg in client.get_chat_history(channel_id):
        processed_messages += 1
        scan_status.processed = processed_messages
        if msg.caption:
            file_name = msg.caption.split('\n')[0].strip()
            if await db_handler.add_scanned_file(file_name):
                added_files += 1

    elapsed = time.time() - scan_status.start_time()
    task_manager.remove_task(scan_status.gid())
    await editMessage(status_message, f"<b>Scan completed in {get_readable_time(elapsed)}.</b>\n\n"
                                       f"<b>Total messages scanned:</b> {processed_messages}\n"
                                       f"<b>New files added to database:</b> {added_files}")

bot.add_handler(MessageHandler(scan_channel, filters=filters.command(BotCommands.ScanCommand) & CustomFilters.authorized))
