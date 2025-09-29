from asyncio import sleep
from pyrogram.errors import FloodWait
from re import match as re_match
from time import time

from bot import config_dict, LOGGER, status_dict, task_dict_lock, intervals, bot, user
from ..ext_utils.bot_utils import SetInterval
from ..ext_utils.exceptions import TgLinkException
from ..ext_utils.status_utils import get_readable_message


async def send_message(message, text, buttons=None, block=True):
    try:
        return await message.reply(
            text=text,
            quote=True,
            disable_web_page_preview=True,
            disable_notification=True,
            reply_markup=buttons,
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        if block:
            await sleep(f.value * 1.2)
            return await send_message(message, text, buttons)
        return str(f)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def edit_message(message, text, buttons=None, block=True):
    try:
        return await message.edit(
            text=text,
            disable_web_page_preview=True,
            reply_markup=buttons,
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        if block:
            await sleep(f.value * 1.2)
            return await edit_message(message, text, buttons)
        return str(f)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def send_file(message, file, caption=""):
    try:
        return await message.reply_document(
            document=file, quote=True, caption=caption, disable_notification=True
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        await sleep(f.value * 1.2)
        return await send_file(message, file, caption)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def send_rss(text, chat_id, thread_id):
    try:
        app = user or bot
        return await app.send_message(
            chat_id=chat_id,
            text=text,
            disable_web_page_preview=True,
            message_thread_id=thread_id,
            disable_notification=True,
        )
    except (FloodWait) as f:
        LOGGER.warning(str(f))
        await sleep(f.value * 1.2)
        return await send_rss(text)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def delete_message(message):
    try:
        await message.delete()
    except Exception as e:
        LOGGER.error(str(e))


async def auto_delete_message(cmd_message=None, bot_message=None):
    await sleep(60)
    if cmd_message is not None:
        await delete_message(cmd_message)
    if bot_message is not None:
        await delete_message(bot_message)


async def delete_status():
    async with task_dict_lock:
        for key, data in list(status_dict.items()):
            try:
                await delete_message(data["message"])
                del status_dict[key]
            except Exception as e:
                LOGGER.error(str(e))

async def get_tg_link_message(link):
    message = None
    links = []
    if link.startswith("https://t.me/"):
        private = False
        msg = re_match(
            r"https://t.me/(?:c/)?([^/]+)(?:/[^/]+)?/([0-9-]+)", link
        )
    else:
        private = True
        msg = re_match(
            r"tg://openmessage?user_id=([0-9]+)&message_id=([0-9-]+)", link
        )
        if not user:
            raise TgLinkException("USER_SESSION_STRING required for this private link!")

    chat = msg[1]
    msg_id = msg[2]
    
    # Handle message ID ranges
    if "-" in msg_id:
        start_id, end_id = msg_id.split("-")
        msg_id = start_id = int(start_id)
        end_id = int(end_id)
        btw = end_id - start_id
        if private:
            link = link.split("&message_id=")[0]
            links.append(f"{link}&message_id={start_id}")
            for _ in range(btw):
                start_id += 1
                links.append(f"{link}&message_id={start_id}")
        else:
            link = link.rsplit("/", 1)[0]
            links.append(f"{link}/{start_id}")
            for _ in range(btw):
                start_id += 1
                links.append(f"{link}/{start_id}")
    else:
        msg_id = int(msg_id)

    # Improved chat ID handling with multiple format attempts
    chat_ids_to_try = []
    
    if chat.isdigit():
        if private:
            # For private chats, try multiple formats
            chat_ids_to_try = [
                int(chat),
                f"-100{chat}",
                f"-{chat}"
            ]
        else:
            # For public channels, try different formats
            chat_ids_to_try = [
                int(f"-100{chat}"),
                int(chat),
                f"-{chat}",
                chat  # Keep original as string for username resolution
            ]
    else:
        # Username format
        chat_ids_to_try = [chat]

    # Try bot client first for public channels
    if not private:
        for chat_id in chat_ids_to_try:
            try:
                message = await bot.get_messages(chat_id=chat_id, message_ids=msg_id)
                if message and not message.empty:
                    return (links, "bot") if links else (message, "bot")
            except Exception as e:
                LOGGER.warning(f"Bot client failed for chat_id {chat_id}: {e}")
                continue
        
        # If bot fails, mark as private and try user client
        private = True

    # Try user client for private channels or when bot fails
    if private and user:
        # Refresh dialogs for better reliability
        try:
            async for _ in user.get_dialogs(limit=100):
                pass
        except Exception as e:
            LOGGER.warning(f"Dialog refresh failed: {e}")
        
        for chat_id in chat_ids_to_try:
            try:
                user_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)
                if user_message and not user_message.empty:
                    return (links, "user") if links else (user_message, "user")
            except Exception as e:
                LOGGER.warning(f"User client failed for chat_id {chat_id}: {e}")
                continue
        
        # Final attempt with original chat format
        try:
            # Try to resolve chat first
            resolved_chat = await user.get_chat(chat)
            user_message = await user.get_messages(chat_id=resolved_chat.id, message_ids=msg_id)
            if user_message and not user_message.empty:
                return (links, "user") if links else (user_message, "user")
        except Exception as e:
            LOGGER.error(f"Final resolution attempt failed: {e}")
            raise TgLinkException(f"You don't have access to this chat! ERROR: {e}") from e
    
    # If no user client available for private channels
    if private and not user:
        raise TgLinkException("USER_SESSION_STRING required for this private link!")
    
    raise TgLinkException("Unable to access the chat with any available method!")


async def update_status_message(sid, force=False):
    if intervals["stopAll"]:
        return
    async with task_dict_lock:
        if not status_dict.get(sid):
            if obj := intervals["status"].get(sid):
                obj.cancel()
                del intervals["status"][sid]
            return
        if not force and time() - status_dict[sid]["time"] < 3:
            return
        status_dict[sid]["time"] = time()
        page_no = status_dict[sid]["page_no"]
        status = status_dict[sid]["status"]
        is_user = status_dict[sid]["is_user"]
        page_step = status_dict[sid]["page_step"]
        text, buttons = await get_readable_message(
            sid, is_user, page_no, status, page_step
        )
        if text is None:
            del status_dict[sid]
            if obj := intervals["status"].get(sid):
                obj.cancel()
                del intervals["status"][sid]
            return
        if text != status_dict[sid]["message"].text:
            message = await edit_message(
                status_dict[sid]["message"], text, buttons, block=False
            )
            if isinstance(message, str):
                if message.startswith("Telegram says: [400"):
                    del status_dict[sid]
                    if obj := intervals["status"].get(sid):
                        obj.cancel()
                        del intervals["status"][sid]
                else:
                    LOGGER.error(
                        f"Status with id: {sid} haven't been updated. Error: {message}"
                    )
                return
            status_dict[sid]["message"].text = text
            status_dict[sid]["time"] = time()


async def send_status_message(msg, user_id=0):
    if intervals["stopAll"]:
        return
    async with task_dict_lock:
        sid = user_id or msg.chat.id
        is_user = bool(user_id)
        if sid in list(status_dict.keys()):
            page_no = status_dict[sid]["page_no"]
            status = status_dict[sid]["status"]
            page_step = status_dict[sid]["page_step"]
            text, buttons = await get_readable_message(
                sid, is_user, page_no, status, page_step
            )
            if text is None:
                del status_dict[sid]
                if obj := intervals["status"].get(sid):
                    obj.cancel()
                    del intervals["status"][sid]
                return
            message = status_dict[sid]["message"]
            await delete_message(message)
            message = await send_message(msg, text, buttons, block=False)
            if isinstance(message, str):
                LOGGER.error(
                    f"Status with id: {sid} haven't been sent. Error: {message}"
                )
                return
            message.text = text
            status_dict[sid].update({"message": message, "time": time()})
        else:
            text, buttons = await get_readable_message(sid, is_user)
            if text is None:
                return
            message = await send_message(msg, text, buttons, block=False)
            if isinstance(message, str):
                LOGGER.error(
                    f"Status with id: {sid} haven't been sent. Error: {message}"
                )
                return
            message.text = text
            status_dict[sid] = {
                "message": message,
                "time": time(),
                "page_no": 1,
                "page_step": 1,
                "status": "All",
                "is_user": is_user,
            }
    if not intervals["status"].get(sid) and not is_user:
        intervals["status"][sid] = SetInterval(
            config_dict["STATUS_UPDATE_INTERVAL"], update_status_message, sid
        )
