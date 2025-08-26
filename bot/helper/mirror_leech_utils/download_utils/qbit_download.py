from aiofiles.os import remove, path as aiopath
from asyncio import sleep, create_task

from bot import (
    task_dict,
    task_dict_lock,
    qbittorrent_client,
    LOGGER,
    config_dict,
)
from ...ext_utils.bot_utils import bt_selection_buttons, sync_to_async
from ...ext_utils.task_manager import check_running_tasks
from ...listeners.qbit_listener import on_download_start
from ...mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ...telegram_helper.message_utils import (
    send_message,
    delete_message,
    send_status_message,
)
from ...ext_utils.Hash_Fetch import get_hash_magnet, get_hash_file


async def add_qb_torrent(listener, path, ratio, seed_time):
    try:
        url = listener.link
        tpath = None
        if await aiopath.exists(listener.link):
            url = None
            tpath = listener.link

        # Determine torrent hash upfront for duplicate check and tracking
        ext_hash = None
        if tpath and await aiopath.exists(tpath):
            ext_hash = get_hash_file(tpath)
        elif url and url.startswith("magnet:"):
            ext_hash = get_hash_magnet(url)

        if ext_hash:
            try:
                existing = await sync_to_async(
                    qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                )
                if existing:
                    await listener.on_download_error("This Torrent already added.")
                    return
            except Exception:
                pass  # non-fatal; proceed to add

        add_to_queue, event = await check_running_tasks(listener)

        # Add torrent (keep tags for compatibility, but tracking will be hash-first)
        op = await sync_to_async(
            qbittorrent_client.torrents_add,
            url,
            tpath,
            path,
            is_paused=add_to_queue,
            tags=f"{listener.mid}",
            ratio_limit=ratio,
            seeding_time_limit=seed_time,
        )
        if not isinstance(op, str) or op.lower() != "ok.":
            await listener.on_download_error(
                "This Torrent already added or unsupported/invalid link/file.",
            )
            return

        # Resolve torrent by hash for reliable tracking on 4.2.5+
        tor_info = None
        if not ext_hash:
            try:
                all_torrents = await sync_to_async(qbittorrent_client.torrents_info)
                if all_torrents:
                    tor_info = sorted(all_torrents, key=lambda t: t.added_on)[-1]
                    ext_hash = tor_info.hash
            except Exception:
                pass

        if ext_hash and not tor_info:
            for _ in range(15):
                lst = await sync_to_async(
                    qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                )
                if lst:
                    tor_info = lst
                    break
                if add_to_queue and event and event.is_set():
                    add_to_queue = False
                await sleep(1)

        if not tor_info:
            await listener.on_download_error(
                "Torrent was added but could not be retrieved. Check qBittorrent."
            )
            return

        listener.name = tor_info.name
        ext_hash = tor_info.hash

        # Register status (hash-first) and start listener keyed by hash
        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(
                listener, queued=add_to_queue, torrent_hash=ext_hash
            )
        await on_download_start(ext_hash)

        if add_to_queue:
            LOGGER.info(f"Added to Queue/Download: {tor_info.name} - Hash: {ext_hash}")
        else:
            LOGGER.info(f"QbitDownload started: {tor_info.name} - Hash: {ext_hash}")

        await listener.on_download_start()

        # Web selection flow (already hash-based for pause/resume/buttons)
        if config_dict["BASE_URL"] and listener.select:
            if listener.link.startswith("magnet:"):
                metamsg = "Downloading Metadata, wait then you can select files. Use torrent file to avoid this wait."
                meta = await send_message(listener.message, metamsg)
                while True:
                    lst = await sync_to_async(
                        qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                    )
                    if not lst:
                        await delete_message(meta)
                        return
                    try:
                        tinfo = lst
                        if tinfo.state not in [
                            "metaDL",
                            "checkingResumeData",
                            "pausedDL",
                        ]:
                            await delete_message(meta)
                            break
                    except:
                        await delete_message(meta)
                        return

            if not add_to_queue:
                await sync_to_async(
                    qbittorrent_client.torrents_pause, torrent_hashes=ext_hash
                )
            SBUTTONS = bt_selection_buttons(ext_hash)
            msg = "Your download paused. Choose files then press Done Selecting button to start downloading."
            await send_message(listener.message, msg, SBUTTONS)
        elif listener.multi <= 1:
            await send_status_message(listener.message)

        # Resume when queue event is released
        if event is not None:
            if not event.is_set():
                await event.wait()
                if listener.is_cancelled:
                    return
                async with task_dict_lock:
                    task_dict[listener.mid].queued = False
                LOGGER.info(
                    f"Start Queued Download from Qbittorrent: {tor_info.name} - Hash: {ext_hash}"
                )
            await sync_to_async(
                qbittorrent_client.torrents_resume, torrent_hashes=ext_hash
            )

    except Exception as e:
        await listener.on_download_error(f"{e}")
    finally:
        if tpath and await aiopath.exists(tpath):
            await remove(tpath)


async def add_qb_torrent_ghostleech(listener, torrent_path, ratio, seed_time):
    """
    Ghostleech function - only accepts .torrent files
    Start torrent and remove trackers like the source ghostleech:
      - Keep the first three pseudo-trackers (DHT/PeX/LSD placeholders)
      - From the remainder, remove the first failing tracker (status 2/3) and all subsequent trackers
    """
    try:
        # Validate input is .torrent file
        if not await aiopath.exists(torrent_path) or not torrent_path.endswith('.torrent'):
            await listener.on_download_error("Ghostleech only supports .torrent files")
            return

        # Get hash from .torrent file for duplicate check and tracking
        ext_hash = get_hash_file(torrent_path)
        if not ext_hash:
            await listener.on_download_error("Invalid .torrent file for ghostleech")
            return

        # Check for duplicates
        try:
            existing = await sync_to_async(
                qbittorrent_client.torrents_info, torrent_hashes=ext_hash
            )
            if existing:
                await listener.on_download_error("This Torrent already added.")
                return
        except Exception:
            pass  # non-fatal; proceed to add

        add_to_queue, event = await check_running_tasks(listener)

        # Add .torrent file (start if not queued)
        op = await sync_to_async(
            qbittorrent_client.torrents_add,
            None,                  # no URL
            torrent_path,          # .torrent file path
            f"{listener.dir}/",    # save path
            is_paused=add_to_queue,
            tags=f"{listener.mid}",
            ratio_limit=ratio,
            seeding_time_limit=seed_time,
        )
        
        if not isinstance(op, str) or op.lower() != "ok.":
            await listener.on_download_error("Failed to add torrent in ghostleech mode")
            return

        # Poll by hash until visible
        tor_info = None
        for _ in range(15):
            lst = await sync_to_async(
                qbittorrent_client.torrents_info, torrent_hashes=ext_hash
            )
            if lst:
                tor_info = lst
                break
            await sleep(1)

        if not tor_info:
            await listener.on_download_error("Ghostleech torrent was added but could not be retrieved")
            return

        listener.name = tor_info.name
        ext_hash = tor_info.hash

        # Register status and start hash-based tracking
        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(
                listener, queued=add_to_queue, torrent_hash=ext_hash
            )
        await on_download_start(ext_hash)

        LOGGER.info(f"Ghostleech started: {tor_info.name} - Hash: {ext_hash}")
        await listener.on_download_start()

        # Ghostleech tracker removal mirroring source behavior
        async def remove_trackers_like_source():
            try:
                await sleep(2)  # allow trackers list to populate
                rm_urls = []
                isdone = False
                while True:
                    trackers = await sync_to_async(
                        qbittorrent_client.torrents_trackers, torrent_hash=ext_hash
                    )
                    # Keep the first three pseudo-trackers (DHT/PeX/LSD), operate on the rest
                    trackers = trackers[3:] if len(trackers) > 3 else []
                    for t in trackers:
                        # Remove the first failing tracker (2/3), and then everything after it
                        if getattr(t, "status", None) in (2, 3) or isdone:
                            try:
                                await sync_to_async(
                                    qbittorrent_client.torrents_remove_trackers,
                                    torrent_hash=ext_hash,
                                    urls=t.url,
                                )
                                rm_urls.append(t.url)
                                isdone = True
                            except Exception:
                                pass
                    await sleep(0.5)
                    if isdone:
                        break
                if rm_urls:
                    rmmsg = "<b>Removed These Trackers</b>\n" + "\n".join(f"<code>{u}</code>" for u in rm_urls)
                    await send_message(listener.message, rmmsg)
                    LOGGER.info(f"Ghostleech tracker removal done for {listener.name}")
            except Exception as e:
                LOGGER.error(f"Ghostleech tracker removal failed: {e}")

        # Start tracker removal in background
        create_task(remove_trackers_like_source())

        # Send status message
        if listener.multi <= 1:
            await send_status_message(listener.message)

        # Handle queue release and resume
        if event is not None:
            if not event.is_set():
                await event.wait()
                if listener.is_cancelled:
                    return
                async with task_dict_lock:
                    task_dict[listener.mid].queued = False
                LOGGER.info(f"Start Queued Ghostleech: {tor_info.name} - Hash: {ext_hash}")
            
            # Resume the torrent after queue release
            await sync_to_async(
                qbittorrent_client.torrents_resume, torrent_hashes=ext_hash
            )

    except Exception as e:
        await listener.on_download_error(f"Ghostleech error: {e}")
    finally:
        # Clean up .torrent file
        if torrent_path and await aiopath.exists(torrent_path):
            await remove(torrent_path)

        if ext_hash:
            try:
                existing = await sync_to_async(
                    qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                )
                if existing:
                    await listener.on_download_error("This Torrent already added.")
                    return
            except Exception:
                pass  # non-fatal; proceed to add

        add_to_queue, event = await check_running_tasks(listener)

        # Add torrent (keep tags for compatibility, but tracking will be hash-first)
        op = await sync_to_async(
            qbittorrent_client.torrents_add,
            url,
            tpath,
            path,
            is_paused=add_to_queue,
            tags=f"{listener.mid}",
            ratio_limit=ratio,
            seeding_time_limit=seed_time,
        )
        if not isinstance(op, str) or op.lower() != "ok.":
            await listener.on_download_error(
                "This Torrent already added or unsupported/invalid link/file.",
            )
            return

        # Resolve torrent by hash for reliable tracking on 4.2.5
        # If hash wasn't computed (non-magnet/edge cases), try to discover from list
        tor_info = None
        if not ext_hash:
            # Best-effort fallback: try to find newest matching by save path
            # but prefer hash-based resolution when possible
            try:
                all_torrents = await sync_to_async(qbittorrent_client.torrents_info)
                if all_torrents:
                    # pick the most recently added as a fallback
                    tor_info = sorted(all_torrents, key=lambda t: t.added_on)[-1]
                    ext_hash = tor_info.hash
            except Exception:
                pass

        # Poll by hash until visible
        if ext_hash and not tor_info:
            for _ in range(15):
                lst = await sync_to_async(
                    qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                )
                if lst:
                    tor_info = lst[0]
                    break
                if add_to_queue and event and event.is_set():
                    add_to_queue = False
                await sleep(1)

        if not tor_info:
            await listener.on_download_error(
                "Torrent was added but could not be retrieved. Check qBittorrent."
            )
            return

        listener.name = tor_info.name
        ext_hash = tor_info.hash

        # Register status (hash-first) and start listener keyed by hash
        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(
                listener, queued=add_to_queue, torrent_hash=ext_hash
            )
        await on_download_start(ext_hash)

        if add_to_queue:
            LOGGER.info(f"Added to Queue/Download: {tor_info.name} - Hash: {ext_hash}")
        else:
            LOGGER.info(f"QbitDownload started: {tor_info.name} - Hash: {ext_hash}")

        await listener.on_download_start()

        # Web selection flow (already hash-based for pause/resume/buttons)
        if config_dict["BASE_URL"] and listener.select:
            if listener.link.startswith("magnet:"):
                metamsg = "Downloading Metadata, wait then you can select files. Use torrent file to avoid this wait."
                meta = await send_message(listener.message, metamsg)
                while True:
                    lst = await sync_to_async(
                        qbittorrent_client.torrents_info, torrent_hashes=ext_hash
                    )
                    if not lst:
                        await delete_message(meta)
                        return
                    try:
                        tinfo = lst[0]
                        if tinfo.state not in [
                            "metaDL",
                            "checkingResumeData",
                            "pausedDL",
                        ]:
                            await delete_message(meta)
                            break
                    except:
                        await delete_message(meta)
                        return

            if not add_to_queue:
                await sync_to_async(
                    qbittorrent_client.torrents_pause, torrent_hashes=ext_hash
                )
            SBUTTONS = bt_selection_buttons(ext_hash)
            msg = "Your download paused. Choose files then press Done Selecting button to start downloading."
            await send_message(listener.message, msg, SBUTTONS)
        elif listener.multi <= 1:
            await send_status_message(listener.message)

        # Resume when queue event is released
        if event is not None:
            if not event.is_set():
                await event.wait()
                if listener.is_cancelled:
                    return
                async with task_dict_lock:
                    task_dict[listener.mid].queued = False
                LOGGER.info(
                    f"Start Queued Download from Qbittorrent: {tor_info.name} - Hash: {ext_hash}"
                )
            await sync_to_async(
                qbittorrent_client.torrents_resume, torrent_hashes=ext_hash
            )

    except Exception as e:
        await listener.on_download_error(f"{e}")
    finally:
        if tpath and await aiopath.exists(tpath):
            await remove(tpath)


async def add_qb_torrent_ghostleech(listener, torrent_path, ratio, seed_time):
    """
    Ghostleech function - only accepts .torrent files
    Starts torrent and removes unwanted trackers (like the source repository)
    """
    try:
        # Validate input is .torrent file
        if not await aiopath.exists(torrent_path) or not torrent_path.endswith('.torrent'):
            await listener.on_download_error("Ghostleech only supports .torrent files")
            return

        # Get hash from .torrent file for duplicate check and tracking
        ext_hash = get_hash_file(torrent_path)
        if not ext_hash:
            await listener.on_download_error("Invalid .torrent file for ghostleech")
            return

        # Check for duplicates
        try:
            existing = await sync_to_async(
                qbittorrent_client.torrents_info, torrent_hashes=ext_hash
            )
            if existing:
                await listener.on_download_error("This Torrent already added.")
                return
        except Exception:
            pass  # non-fatal; proceed to add

        add_to_queue, event = await check_running_tasks(listener)

        # Add .torrent file (ghostleech mode - start normally, not paused)
        op = await sync_to_async(
            qbittorrent_client.torrents_add,
            None,  # no URL
            torrent_path,  # .torrent file path
            f"{listener.dir}/",  # save path
            is_paused=add_to_queue,  # Only pause if queue requires it
            tags=f"{listener.mid}",
            ratio_limit=ratio,
            seeding_time_limit=seed_time,
        )
        
        if not isinstance(op, str) or op.lower() != "ok.":
            await listener.on_download_error("Failed to add torrent in ghostleech mode")
            return

        # Poll by hash until visible (ghostleech pattern)
        tor_info = None
        for _ in range(15):
            lst = await sync_to_async(
                qbittorrent_client.torrents_info, torrent_hashes=ext_hash
            )
            if lst:
                tor_info = lst[0]
                break
            await sleep(1)

        if not tor_info:
            await listener.on_download_error("Ghostleech torrent was added but could not be retrieved")
            return

        listener.name = tor_info.name
        ext_hash = tor_info.hash

        # Register status and start hash-based tracking
        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(
                listener, queued=add_to_queue, torrent_hash=ext_hash
            )
        await on_download_start(ext_hash)

        LOGGER.info(f"Ghostleech started: {tor_info.name} - Hash: {ext_hash}")
        await listener.on_download_start()

        # Ghostleech functionality: Remove unwanted trackers after starting
        async def remove_bad_trackers():
            try:
                await sleep(2)  # Wait a bit for torrent to initialize
                rmmsg = "<b>Removed These Trackers</b>\n"
                isdone = False
                
                while not isdone:
                    trackers = await sync_to_async(
                        qbittorrent_client.torrents_trackers, torrent_hash=ext_hash
                    )
                    
                    # Skip first 3 trackers (usually DHT, PEX, LSD)
                    trackers = trackers[3:] if len(trackers) > 3 else []
                    
                    for tracker in trackers:
                        # Remove trackers with status 2 (not working) or 3 (not contacted)
                        if tracker.status == 2 or tracker.status == 3:
                            LOGGER.info(f"Removing tracker {tracker.url} with status {tracker.status}")
                            rmmsg += f"<code>{tracker.url}</code>\n"
                            await sync_to_async(
                                qbittorrent_client.torrents_remove_trackers,
                                torrent_hash=ext_hash,
                                urls=tracker.url
                            )
                            isdone = True
                    
                    await sleep(0.5)
                    if isdone:
                        break
                
                if rmmsg != "<b>Removed These Trackers</b>\n":
                    await send_message(listener.message, rmmsg)
                    LOGGER.info(f"Ghostleech completed tracker removal for {listener.name}")
                
            except Exception as e:
                LOGGER.error(f"Ghostleech tracker removal failed: {e}")

        # Start tracker removal in background
        create_task(remove_bad_trackers())

        # Send status message
        if listener.multi <= 1:
            await send_status_message(listener.message)

        # Handle queue release and resume
        if event is not None:
            if not event.is_set():
                await event.wait()
                if listener.is_cancelled:
                    return
                async with task_dict_lock:
                    task_dict[listener.mid].queued = False
                LOGGER.info(f"Start Queued Ghostleech: {tor_info.name} - Hash: {ext_hash}")
            
            # Resume the torrent after queue release
            await sync_to_async(
                qbittorrent_client.torrents_resume, torrent_hashes=ext_hash
            )

    except Exception as e:
        await listener.on_download_error(f"Ghostleech error: {e}")
    finally:
        # Clean up .torrent file
        if torrent_path and await aiopath.exists(torrent_path):
            await remove(torrent_path)
