from aiofiles.os import remove, path as aiopath
from asyncio import sleep
from time import time

from bot import (
    task_dict,
    task_dict_lock,
    intervals,
    qbittorrent_client,
    config_dict,
    qb_torrents,
    qb_listener_lock,
    LOGGER,
)
from ..ext_utils.bot_utils import new_task, sync_to_async
from ..ext_utils.files_utils import clean_unwanted
from ..ext_utils.status_utils import get_readable_time, get_task_by_gid
from ..ext_utils.task_manager import stop_duplicate_check
from ..mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ..telegram_helper.message_utils import update_status_message


async def _remove_torrent(hash_, _tag=None):
    await sync_to_async(
        qbittorrent_client.torrents_delete, torrent_hashes=hash_, delete_files=True
    )
    async with qb_listener_lock:
        if hash_ in qb_torrents:
            del qb_torrents[hash_]
    # No tag deletion since we track by hash


@new_task
async def _on_download_error(err, tor, button=None):
    LOGGER.info(f"Cancelling Download: {tor.name}")
    ext_hash = tor.hash
    if task := await get_task_by_gid(ext_hash[:12]):
        await task.listener.on_download_error(err, button)
    await sync_to_async(qbittorrent_client.torrents_pause, torrent_hashes=ext_hash)
    await sleep(0.3)
    await _remove_torrent(ext_hash, None)


@new_task
async def _on_seed_finish(tor):
    ext_hash = tor.hash
    LOGGER.info(f"Cancelling Seed: {tor.name}")
    if task := await get_task_by_gid(ext_hash[:12]):
        msg = f"Seeding stopped with Ratio: {round(tor.ratio, 3)} and Time: {get_readable_time(tor.seeding_time)}"
        await task.listener.on_upload_error(msg)
    await _remove_torrent(ext_hash, None)


@new_task
async def _stop_duplicate(tor):
    if task := await get_task_by_gid(tor.hash[:12]):
        if task.listener.stop_duplicate:
            task.listener.name = tor.content_path.rsplit("/", 1)[-1].rsplit(".!qB", 1)[
                0
            ]
            msg, button = await stop_duplicate_check(task.listener)
            if msg:
                _on_download_error(msg, tor, button)


@new_task
async def _on_download_complete(tor):
    ext_hash = tor.hash
    if task := await get_task_by_gid(ext_hash[:12]):
        if not task.listener.seed:
            await sync_to_async(
                qbittorrent_client.torrents_pause, torrent_hashes=ext_hash
            )
        if task.listener.select:
            await clean_unwanted(task.listener.dir)
            path = tor.content_path.rsplit("/", 1)[0]
            res = await sync_to_async(
                qbittorrent_client.torrents_files, torrent_hash=ext_hash
            )
            for f in res:
                if f.priority == 0 and await aiopath.exists(f"{path}/{f.name}"):
                    try:
                        await remove(f"{path}/{f.name}")
                    except:
                        pass
        await task.listener.on_download_complete()
        if intervals["stopAll"]:
            return
        if task.listener.seed and not task.listener.is_cancelled:
            async with task_dict_lock:
                if task.listener.mid in task_dict:
                    removed = False
                    task_dict[task.listener.mid] = QbittorrentStatus(
                        task.listener, True, torrent_hash=ext_hash
                    )
                else:
                    removed = True
            if removed:
                await _remove_torrent(ext_hash, None)
                return
            async with qb_listener_lock:
                if ext_hash in qb_torrents:
                    qb_torrents[ext_hash]["seeding"] = True
                else:
                    return
            await update_status_message(task.listener.message.chat.id)
            LOGGER.info(f"Seeding started: {tor.name} - Hash: {ext_hash}")
        else:
            await _remove_torrent(ext_hash, None)
    else:
        await _remove_torrent(ext_hash, None)


@new_task
async def _qb_listener():
    while True:
        async with qb_listener_lock:
            try:
                torrents = await sync_to_async(qbittorrent_client.torrents_info)
                if len(torrents) == 0:
                    intervals["qb"] = ""
                    break
                for tor_info in torrents:
                    tor_hash = tor_info.hash
                    if tor_hash not in qb_torrents:
                        continue
                    state = tor_info.state
                    if state == "metaDL":
                        TORRENT_TIMEOUT = config_dict["TORRENT_TIMEOUT"]
                        qb_torrents[tor_hash]["stalled_time"] = time()
                        if (
                            TORRENT_TIMEOUT
                            and time() - tor_info.added_on >= TORRENT_TIMEOUT
                        ):
                            await _on_download_error("Dead Torrent!", tor_info)
                        else:
                            await sync_to_async(
                                qbittorrent_client.torrents_reannounce,
                                torrent_hashes=tor_info.hash,
                            )
                    elif state == "downloading":
                        qb_torrents[tor_hash]["stalled_time"] = time()
                        if not qb_torrents[tor_hash]["stop_dup_check"]:
                            qb_torrents[tor_hash]["stop_dup_check"] = True
                            await _stop_duplicate(tor_info)
                    elif state == "stalledDL":
                        TORRENT_TIMEOUT = config_dict["TORRENT_TIMEOUT"]
                        if (
                            not qb_torrents[tor_hash]["rechecked"]
                            and 0.99989999999999999 < tor_info.progress < 1
                        ):
                            msg = f"Force recheck - Name: {tor_info.name} Hash: "
                            msg += f"{tor_info.hash} Downloaded Bytes: {tor_info.downloaded} "
                            msg += f"Size: {tor_info.size} Total Size: {tor_info.total_size}"
                            LOGGER.warning(msg)
                            await sync_to_async(
                                qbittorrent_client.torrents_recheck,
                                torrent_hashes=tor_info.hash,
                            )
                            qb_torrents[tor_hash]["rechecked"] = True
                        elif (
                            TORRENT_TIMEOUT
                            and time() - qb_torrents[tor_hash]["stalled_time"]
                            >= TORRENT_TIMEOUT
                        ):
                            await _on_download_error("Dead Torrent!", tor_info)
                        else:
                            await sync_to_async(
                                qbittorrent_client.torrents_reannounce,
                                torrent_hashes=tor_info.hash,
                            )
                    elif state == "missingFiles":
                        await sync_to_async(
                            qbittorrent_client.torrents_recheck,
                            torrent_hashes=tor_info.hash,
                        )
                    elif state == "error":
                        await _on_download_error(
                            "No enough space for this torrent on device", tor_info
                        )
                    # IMPROVED: Enhanced completion detection for 4.2.5
                    elif (
                        (tor_info.progress >= 0.999 or state in ["uploading", "stalledUP"])
                        and not qb_torrents[tor_hash]["uploaded"]
                        and state not in ["checkingUP", "checkingDL", "checkingResumeData"]
                    ):
                        qb_torrents[tor_hash]["uploaded"] = True
                        await _on_download_complete(tor_info)
                    elif (
                        state in ["pausedUP", "pausedDL"]
                        and qb_torrents[tor_hash]["seeding"]
                    ):
                        qb_torrents[tor_hash]["seeding"] = False
                        await _on_seed_finish(tor_info)
                        await sleep(0.5)
            except Exception as e:
                LOGGER.error(str(e))
        await sleep(3)


async def on_download_start(torrent_hash):
    async with qb_listener_lock:
        qb_torrents[torrent_hash] = {
            "stalled_time": time(),
            "stop_dup_check": False,
            "rechecked": False,
            "uploaded": False,
            "seeding": False,
        }
        if not intervals["qb"]:
            intervals["qb"] = await _qb_listener()
