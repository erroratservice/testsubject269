from pyrogram.filters import command
from pyrogram.handlers import MessageHandler
from bot import bot
import time

from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.status_utils import get_readable_time
from ..helper.telegram_helper.message_utils import send_message
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.mirror_leech_utils.channel_status import channel_status

@new_task
async def channel_status_cmd(_, message):
    """Show channel operation status"""
    user_id = message.from_user.id
    operations = await channel_status.get_user_operations(user_id)
    
    if not operations:
        await send_message(message, "📋 No active channel operations.")
        return

    status_text = "📊 **Active Channel Operations:**\n\n"
    
    for i, op in enumerate(operations, 1):
        elapsed = int(time.time() - op['start_time'])
        status_text += (
            f"**Operation {i}:**\n"
            f"🔹 **Type:** {op['type'].replace('_', ' ').title()}\n"
            f"🔹 **Channel:** `{op['channel_id']}`\n"
            f"🔹 **Status:** {op['status'].title()}\n"
            f"🔹 **Processed:** {op['processed']}\n"
            f"🔹 **Downloaded:** {op['downloaded']}\n"
            f"🔹 **Skipped:** {op['skipped']}\n"
            f"🔹 **Errors:** {op['errors']}\n"
            f"🔹 **Runtime:** {get_readable_time(elapsed)}\n\n"
        )

    await send_message(message, status_text)

# Register handler
bot.add_handler(MessageHandler(
    channel_status_cmd,
    filters=command("cstatus") & CustomFilters.authorized
))
