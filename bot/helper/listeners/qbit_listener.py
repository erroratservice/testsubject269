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
                    # FIXED: Improved completion detection
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
