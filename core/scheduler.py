from __future__ import annotations
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import schedule
from zoneinfo import ZoneInfo

from core.config import (
    DOWNLOAD_DIR, ADMIN_CHAT_ID,
    CHANNEL_USERNAME, BOT_USERNAME, CHANNEL_NAME,
    DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME
)
from core.client import client, FFMPEG_AVAILABLE, currently_processing
from core.state import (
    auto_download_state, quality_settings, anime_queue,
    episode_tracker, EpisodeState, deferred_episodes
)
from core.utils import (
    sanitize_filename, format_filename, format_size, format_speed,
    get_fixed_thumbnail, is_episode_processed, update_processed_qualities, mark_episode_processed,
    ProgressMessage, UploadProgressBar, safe_edit,
    generate_batch_link, generate_single_link
)
from core.anime_api import (
    search_anime, get_all_episodes, get_latest_releases,
    get_stream_links, extract_m3u8_from_kwik, download_m3u8,
    get_quality_streams, detect_audio_type, get_anime_info,
    find_closest_episode, map_resolution_to_quality_tier
)
from core.download import (
    rename_video_with_ffmpeg, robust_upload_file
)

logger = logging.getLogger(__name__)

from telethon.errors import FloodWaitError
from telethon.tl.custom import Button

_currently_processing = False
_scheduler_lock = asyncio.Lock() if asyncio else None

_request_time_job_tag = "daily_request_processing"

def get_currently_processing():
    return _currently_processing

def set_currently_processing(value: bool):
    global _currently_processing
    _currently_processing = value

def _get_scheduler_lock():
    global _scheduler_lock
    if _scheduler_lock is None:
        _scheduler_lock = asyncio.Lock()
    return _scheduler_lock


async def _get_best_image(anime_info):
    if not anime_info:
        return None

    banner = anime_info.get('coverImage')
    if banner:
        return banner

    relations = anime_info.get('relations', {}).get('edges', [])
    for rel in relations:
        node_banner = rel.get('node', {}).get('coverImage')
        if node_banner:
            return node_banner

    try:
        import aiohttp
        anilist_id = anime_info.get('id')
        if not anilist_id:
            cover_data = anime_info.get('coverImage', {})
            return cover_data.get('extraLarge') or cover_data.get('large')

        query = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    relations {
      edges {
        relationType
        node {
          id
          bannerImage
        }
      }
    }
  }
}
"""
        visited = {anilist_id}
        queue = []
        for rel in relations:
            if rel.get('relationType') in ('PREQUEL', 'PARENT'):
                nid = rel.get('node', {}).get('id')
                if nid and nid not in visited:
                    queue.append(nid)
                    visited.add(nid)

        url = 'https://graphql.anilist.co'
        async with aiohttp.ClientSession() as session:
            for _ in range(5):
                if not queue:
                    break
                current_id = queue.pop(0)
                async with session.post(url, json={'query': query, 'variables': {'id': current_id}}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    media = data.get('data', {}).get('Media', {})
                    if not media:
                        continue
                    edges = media.get('relations', {}).get('edges', [])
                    for rel in edges:
                        node = rel.get('node', {})
                        nb = node.get('bannerImage')
                        if nb:
                            return nb
                        if rel.get('relationType') in ('PREQUEL', 'PARENT'):
                            nid = node.get('id')
                            if nid and nid not in visited:
                                queue.append(nid)
                                visited.add(nid)
    except Exception as e:
        logger.warning(f"Error walking prequel chain for banner: {e}")

    cover_data = anime_info.get('coverImage', {})
    return cover_data.get('extraLarge') or cover_data.get('large')

async def post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files):
    from core.config import CHANNEL_ID, CHANNEL_USERNAME, FIXED_THUMBNAIL_URL

    channel_target = CHANNEL_ID or CHANNEL_USERNAME
    if not channel_target:
        logger.warning("No main channel configured for posting")
        return

    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')

    try:
        title_romaji = anime_title
        title_english = ""
        genres = ""
        score = ""
        studios = ""

        if anime_info:
            titles = anime_info.get('title', {})
            title_romaji = titles.get('romaji', anime_title)
            title_english = titles.get('english', '')
            genres = ', '.join(anime_info.get('genres', [])[:4])
            score = anime_info.get('averageScore', '')
            studio_nodes = anime_info.get('studios', {}).get('nodes', [])
            studios = ', '.join([s['name'] for s in studio_nodes[:2]]) if studio_nodes else ''

        if audio_type == "Sub":
            audio_alpha = "Japanese"
        else:
            audio_alpha = "English"

        caption = (
            f"<b><blockquote>✦ {title_english} ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>"
        )
        caption += f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
        caption += f"・ Aᴜᴅɪᴏ: {audio_alpha}\n"
        if genres:
            caption += f"・ Gᴇɴʀᴇs: {genres}</blockquote>\n"
        caption += (
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>"
        )

        button_list = []
        sorted_qualities = sorted(quality_files.keys(), key=lambda x: int(x[:-1]))

        for quality in sorted_qualities:
            msg_ids = quality_files[quality]
            if not msg_ids:
                continue

            if len(msg_ids) == 1:
                link = await generate_single_link(msg_ids[0])
            else:
                link = await generate_batch_link(msg_ids)

            quality_map = {
                "360p": "𝟯𝟲𝟬𝗣",
                "720p": "𝟳𝟮𝟬𝗣",
                "1080p": "𝟭𝟬𝟴𝟬𝗣"
            }

            quality_btn = quality_map.get(quality, quality)

            if link:
                button_list.append(Button.url(f"{quality_btn}", link))

        if not button_list:
            logger.error("No valid download links generated for buttons")
            return

        buttons = _arrange_buttons(button_list)

        poster_path = None

        ani_id = anime_info.get("id") if anime_info else None
        image_url = f"https://img.anili.st/media/{ani_id}" if ani_id else None

        if image_url:
            import aiohttp
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 200:
                            poster_path = os.path.join(DOWNLOAD_DIR, f"poster_{sanitize_filename(anime_title)}.jpg")
                            with open(poster_path, 'wb') as f:
                                f.write(await resp.read())
            except Exception as e:
                logger.warning(f"Failed to download poster: {e}")

        if poster_path and os.path.exists(poster_path):
            await client.send_file(
                channel_target,
                poster_path,
                caption=caption,
                parse_mode='html',
                buttons=buttons,
                link_preview=False
            )
            try:
                os.remove(poster_path)
            except:
                pass
        else:
            await client.send_message(
                channel_target,
                caption,
                parse_mode='html',
                buttons=buttons,
                link_preview=False
            )

        logger.info(f"Posted {anime_title} Episode {episode_number} to channel with {len(button_list)} quality buttons")

    except FloodWaitError as e:
        logger.warning(f"Flood wait during post: {e.seconds}s")
        await asyncio.sleep(e.seconds + 5)
        raise
    except Exception as e:
        logger.error(f"Error posting anime with buttons: {e}")
        raise


def _arrange_buttons(button_list):
    if len(button_list) == 1:
        return [[button_list[0]]]
    elif len(button_list) == 2:
        return [[button_list[0], button_list[1]]]
    else:
        rows = []
        i = 0
        while i < len(button_list):
            if i + 1 < len(button_list):
                rows.append([button_list[i], button_list[i + 1]])
                i += 2
            else:
                rows.append([button_list[i]])
                i += 1
        return rows

async def post_anime_batch_with_buttons(client, anime_title, anime_info, quality_files, total_episodes, audio_type):
    from core.config import CHANNEL_ID, CHANNEL_USERNAME

    channel_target = CHANNEL_ID or CHANNEL_USERNAME
    if not channel_target:
        logger.warning("No main channel configured for posting")
        return

    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')

    try:
        title_romaji = anime_title
        title_english = ""
        genres = ""

        if anime_info:
            titles = anime_info.get('title', {})
            title_romaji = titles.get('romaji', anime_title)
            title_english = titles.get('english', '')
            genres = ', '.join(anime_info.get('genres', [])[:4])

        if audio_type == "Sub":
            audio_alpha = "Japanese"
        else:
            audio_alpha = "English"

        caption = (
            f"<b><blockquote>✦ {title_romaji} ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>"
            f"・ Eᴘɪsᴏᴅᴇs: 1-{total_episodes}\n"
            f"・ Aᴜᴅɪᴏ: {audio_alpha}\n"
        )
        if genres:
            caption += f"・ Gᴇɴʀᴇs: {genres}</blockquote>\n"
        caption += (
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>"
        )

        button_list = []
        sorted_qualities = sorted(quality_files.keys(), key=lambda x: int(x[:-1]))

        for quality in sorted_qualities:
            msg_ids = quality_files[quality]
            if not msg_ids:
                continue

            if len(msg_ids) == 1:
                link = await generate_single_link(msg_ids[0])
            else:
                link = await generate_batch_link(msg_ids)

            quality_map = {
                "360p": "𝟯𝟲𝟬𝗣",
                "720p": "𝟳𝟮𝟬𝗣",
                "1080p": "𝟭𝟬𝟴𝟬𝗣"
            }

            quality_btn = quality_map.get(quality, quality)

            if link:
                button_list.append(Button.url(f"{quality} - {total_episodes} Episodes", link))

        if not button_list:
            logger.error("No valid download links for batch buttons")
            return

        buttons = _arrange_buttons(button_list)

        poster_path = None

        ani_id = anime_info.get("id") if anime_info else None
        image_url = f"https://img.anili.st/media/{ani_id}" if ani_id else None

        if image_url:
            import aiohttp
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 200:
                            poster_path = os.path.join(DOWNLOAD_DIR, f"poster_{sanitize_filename(anime_title)}_batch.jpg")
                            with open(poster_path, 'wb') as f:
                                f.write(await resp.read())
            except Exception as e:
                logger.warning(f"Failed to download batch poster: {e}")

        if poster_path and os.path.exists(poster_path):
            await client.send_file(
                channel_target,
                poster_path,
                caption=caption,
                parse_mode='html',
                buttons=buttons,
                link_preview=False
            )
            try:
                os.remove(poster_path)
            except:
                pass
        else:
            await client.send_message(
                channel_target,
                caption,
                parse_mode='html',
                buttons=buttons,
                link_preview=False
            )

        logger.info(f"Posted batch: {anime_title} ({total_episodes} episodes) to channel")

    except Exception as e:
        logger.error(f"Error posting anime batch with buttons: {e}")

async def _download_and_upload_single_quality(
    anime_title, episode_number, quality, stream_info, audio_type, progress=None, channel_format=""
):
    download_path = None
    try:
        kwik_url = stream_info['url']
        resolution = stream_info['resolution']
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
                f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ URL...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
        if not m3u8_data:
            logger.error(f"Failed to extract m3u8 from {kwik_url}")
            return None
        
        m3u8_url = m3u8_data['m3u8_url']
        m3u8_headers = m3u8_data['headers']
        
        base_name = format_filename(anime_title, episode_number, quality, audio_type)
        main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
        full_caption = f"**{base_name} {main_channel_username}.mkv**"
        filename = sanitize_filename(full_caption)
        download_path = os.path.join(DOWNLOAD_DIR, filename)
        
        from core.dl_progress import FFmpegProgressReporter, make_upload_status_text
        dl_reporter = FFmpegProgressReporter(
            progress_message=progress,
            anime_title=anime_title,
            episode_number=episode_number,
            quality=quality,
            audio_type=audio_type,
            channel_format=channel_format,
        )
        
        download_start = time.time()
        success = await download_m3u8(m3u8_url, m3u8_headers, download_path,
                                       progress_callback=dl_reporter.callback)
        
        if not success:
            logger.error(f"M3U8 download failed for {quality}")
            return None
        
        if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
            logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
            return None
        
        download_time = time.time() - download_start
        file_size = os.path.getsize(download_path)
        avg_speed = file_size / download_time if download_time > 0 else 0
        
        logger.info(f"Download complete: {quality} - {format_size(file_size)} in {download_time:.1f}s ({format_speed(avg_speed)})")
        
        async def _upload_progress(current, total):
            if not progress:
                return
            pct = int(current * 100 / total) if total else 0
            bar_fill = pct // 5
            bar = "█" * bar_fill + "░" * (20 - bar_fill)
            status = f"[{bar}] {pct}% — {format_size(current)}/{format_size(total)}"
            text = make_upload_status_text(
                anime_title, episode_number, quality, audio_type,
                total, channel_format, extra_status=status,
            )
            await progress.update(text, parse_mode='html')
        
        thumb = await get_fixed_thumbnail()
        
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=full_caption,
            thumb_path=thumb,
            max_retries=3,
            progress_callback=_upload_progress,
        )
        
        try:
            os.remove(download_path)
        except:
            pass
        
        if dump_msg_id:
            logger.info(f"Successfully uploaded {quality} version: msg_id={dump_msg_id}")
            return dump_msg_id
        else:
            logger.error(f"Upload failed for {quality}")
            return None
            
    except Exception as e:
        logger.error(f"Error in _download_and_upload_single_quality for {quality}: {e}")
        try:
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
        except:
            pass
        return None

async def auto_download_latest_episode():
    global _currently_processing
    
    logger.info("Starting auto download process...")
    
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return False
    
    _currently_processing = True
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    if ADMIN_CHAT_ID:
        progress = ProgressMessage(client, ADMIN_CHAT_ID, "<b>Auto processing started...</b>")
        await progress.send()
    
    try:
        if auto_download_state.last_checked:
            last_check = datetime.fromisoformat(auto_download_state.last_checked)
            time_since_last_check = (datetime.now() - last_check).total_seconds()
            
            cooldown_period = auto_download_state.interval / 2
            if time_since_last_check < cooldown_period:
                logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                return False
        
        if progress:
            await progress.update("<b><blockquote>ᴄʜᴇᴄᴋɪɴɢ ғᴏʀ ɴᴇᴡ ᴇᴘɪsᴏᴅᴇs...</blockquote></b>", parse_mode='html')
        
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            if progress:
                await progress.update("<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇsᴛ ʀᴇʟᴇᴀsᴇ</blockquote></b>", parse_mode='html')
            return False
        
        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)
        
        logger.info(f"Latest airing anime: {anime_title} Episode {episode_number}")
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗖𝗛𝗘𝗖𝗞𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title} \n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Cʜᴇᴄᴋɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title} \n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssᴇᴅ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return True
        
        success = await process_specific_anime(latest_anime, progress)
        
        auto_download_state.last_checked = datetime.now().isoformat()
        return success
        
    except Exception as e:
        logger.error(f"Error in auto_download_latest_episode: {e}")
        if progress:
            await progress.update(f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</blockquote></b>", parse_mode='html')
        return False
    finally:
        _currently_processing = False

async def check_and_process_next_episode(progress=None):
    if anime_queue.pending_queue:
        next_item = anime_queue.pending_queue[0]
        anime_queue.pending_queue.pop(0)
        
        anime_data = {
            'anime_title': next_item.get('title'),
            'episode': next_item.get('episode')
        }
        return await process_specific_anime(anime_data, progress)
    return False

async def process_pending_queue(progress=None):
    while anime_queue.pending_queue:
        await check_and_process_next_episode(progress)
        await asyncio.sleep(5)

async def process_single_episode(anime_title, episode_number, progress=None, from_queue=False):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    anime_data = {
        'anime_title': anime_title,
        'episode': episode_number
    }
    return await process_specific_anime(anime_data, progress)

async def check_for_new_episodes(client):
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    
    if not auto_download_state.enabled:
        return
    
    scheduler_lock = _get_scheduler_lock()
    if scheduler_lock:
        if scheduler_lock.locked():
            logger.info("Scheduler lock held by another task. Skipping this check.")
            return
    
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    async with scheduler_lock if scheduler_lock else asyncio.Lock():
        _currently_processing = True
        logger.info("Checking for new episodes, deferred episodes, and pending queue...")
        
        if anime_queue.pending_queue:
            logger.info(f"Processing {len(anime_queue.pending_queue)} pending episodes first...")
            await process_pending_queue()
        
        # Process deferred episodes first (these are waiting for all qualities to appear)
        deferred_list = deferred_episodes.get_all_deferred()
        if deferred_list:
            logger.info(f"Checking {len(deferred_list)} deferred episodes for quality availability...")
            deferred_episodes.cleanup_expired()
            
            for deferred_item in deferred_list:
                d_title = deferred_item.get('anime_title')
                d_episode = deferred_item.get('episode_number')
                d_anime_data = deferred_item.get('anime_data', {})
                
                if is_episode_processed(d_title, d_episode):
                    deferred_episodes.remove_episode(d_title, d_episode)
                    continue
                
                if episode_tracker.is_posted(d_title, d_episode):
                    deferred_episodes.remove_episode(d_title, d_episode)
                    continue
                
                logger.info(f"Re-checking deferred: {d_title} Ep{d_episode} (check #{deferred_item.get('check_count', 0)})")
                
                if not episode_tracker.try_start_processing(d_title, d_episode):
                    continue
                
                try:
                    success = await process_specific_anime(d_anime_data, progress, _caller_holds_lock=True)
                    if success:
                        logger.info(f"Deferred episode now processed successfully: {d_title} Ep{d_episode}")
                    else:
                        episode_tracker.release_processing(d_title, d_episode, success=False)
                        logger.info(f"Deferred episode still not ready: {d_title} Ep{d_episode}")
                except Exception as e:
                    logger.error(f"Error processing deferred {d_title} Ep{d_episode}: {e}")
                    episode_tracker.release_processing(d_title, d_episode, success=False)
        
        try:
            if auto_download_state.last_checked:
                last_check = datetime.fromisoformat(auto_download_state.last_checked)
                time_since_last_check = (datetime.now() - last_check).total_seconds()
                
                cooldown_period = auto_download_state.interval / 2
                if time_since_last_check < cooldown_period:
                    logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                    return
            
            latest_data = get_latest_releases(page=1)
            if not latest_data or 'data' not in latest_data:
                logger.error("Failed to get latest releases")
                return
            
            unprocessed_anime = []
            for anime_data in latest_data['data']:
                anime_title = anime_data.get('anime_title', 'Unknown Anime')
                episode_number = anime_data.get('episode', 0)
                
                if is_episode_processed(anime_title, episode_number):
                    continue
                
                if episode_tracker.is_posted(anime_title, episode_number):
                    continue
                
                if episode_tracker.is_processing(anime_title, episode_number):
                    continue
                
                # Skip if already deferred (will be handled in deferred check above)
                if deferred_episodes.is_deferred(anime_title, episode_number):
                    continue
                
                unprocessed_anime.append(anime_data)
                logger.info(f"Found unprocessed: {anime_title} Episode {episode_number}")
            
            if not unprocessed_anime:
                logger.info("No new unprocessed anime found.")
                auto_download_state.last_checked = datetime.now().isoformat()
                return
            
            logger.info(f"Found {len(unprocessed_anime)} unprocessed anime to process sequentially")
            
            if ADMIN_CHAT_ID:
                progress = ProgressMessage(client, ADMIN_CHAT_ID, f"<b><blockquote>ғᴏᴜɴᴅ {len(unprocessed_anime)} ɴᴇᴡ ᴀɴɪᴍᴇ ᴛᴏ ᴘʀᴏᴄᴇss...</blockquote></b>", parse_mode='html')
                await progress.send()
            
            processed_count = 0
            failed_count = 0
            skipped_count = 0
            
            for idx, anime_data in enumerate(unprocessed_anime):
                anime_title = anime_data.get('anime_title', 'Unknown Anime')
                episode_number = anime_data.get('episode', 0)
                
                if not episode_tracker.try_start_processing(anime_title, episode_number):
                    logger.info(f"Skipping {anime_title} Ep{episode_number}: could not acquire processing lock")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing anime {idx + 1}/{len(unprocessed_anime)}: {anime_title} Episode {episode_number}")
                
                if progress:
                    await progress.update(
                        f"<b><blockquote>✦ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                        f"・ Pʀᴏɢʀᴇss: {idx + 1}/{len(unprocessed_anime)}\n"
                        f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssɪɴɢ</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                        parse_mode='html'
                    )
                
                try:
                    success = await process_specific_anime(anime_data, progress, _caller_holds_lock=True)
                    
                    if success:
                        processed_count += 1
                        logger.info(f"Successfully processed: {anime_title} Episode {episode_number}")
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to process: {anime_title} Episode {episode_number}")
                        episode_tracker.release_processing(anime_title, episode_number, success=False)
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing {anime_title} Episode {episode_number}: {e}")
                    episode_tracker.release_processing(anime_title, episode_number, success=False)
                    continue
            
            auto_download_state.last_checked = datetime.now().isoformat()
            
            deferred_count = len(deferred_episodes.get_all_deferred())
            
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Pʀᴏᴄᴇssᴇᴅ: {processed_count}\n"
                    f"・ Dᴇғᴇʀʀᴇᴅ: {deferred_count}\n"
                    f"・ Fᴀɪʟᴇᴅ: {failed_count}\n"
                    f"・ Sᴋɪᴘᴘᴇᴅ: {skipped_count}\n"
                    f"・ Tᴏᴛᴀʟ: {len(unprocessed_anime)}</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            
            logger.info(f"Batch processing complete: {processed_count} processed, {failed_count} failed, {skipped_count} skipped, {deferred_count} deferred")
            
        except Exception as e:
            logger.error(f"Error checking for new episodes: {str(e)}")
            if progress:
                await progress.update(
                    f"<b><blockquote>ᴇʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ ᴀɴɪᴍᴇ:</b> {str(e)}</blockquote>",
                    parse_mode='html'
                )
        finally:
            _currently_processing = False

async def process_specific_anime(anime_data: dict, progress=None, _caller_holds_lock: bool = False) -> bool:
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    anime_title = anime_data.get('anime_title', 'Unknown Anime')
    episode_number = anime_data.get('episode', 0)
    
    # Check DB first - if already processed with all qualities, skip immediately
    if is_episode_processed(anime_title, episode_number):
        logger.info(f"Episode {episode_number} of {anime_title} already fully processed in DB. Skipping.")
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗘𝗗 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Aʟʀᴇᴀᴅʏ ɪɴ DB</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        return True
    
    # Only acquire the processing lock if caller hasn't already done so
    if not _caller_holds_lock:
        if _currently_processing:
            logger.info(f"Already processing another anime, skipping {anime_title} Ep{episode_number}")
            return False
        _currently_processing = True
    
    try:
        logger.info(f"Starting processing: {anime_title} Episode {episode_number}")
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            target_episode = find_closest_episode(episodes, episode_number)
            if target_episode:
                episode_number = int(target_episode['episode'])
            else:
                logger.error(f"No episodes found for {anime_title}")
                return False
        
        episode_session = target_episode['session']
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗦𝗧𝗥𝗘𝗔𝗠𝗦 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ URLs...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
        if not stream_links:
            logger.error(f"No stream links found for {anime_title} Episode {episode_number}")
            # Defer this episode - stream links not available yet
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=[],
                missing_qualities=list(quality_settings.enabled_qualities),
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Rᴇᴀsᴏɴ: Nᴏ sᴛʀᴇᴀᴍ ʟɪɴᴋs ʏᴇᴛ\n"
                    f"・ Sᴛᴀᴛᴜs: Wɪʟʟ ʀᴇᴛʀʏ ʟᴀᴛᴇʀ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        enabled_qualities = quality_settings.enabled_qualities
        preferred_audio = "jpn"
        
        quality_mapping = get_quality_streams(stream_links, enabled_qualities, preferred_audio)
        available_qualities = [q for q, s in quality_mapping.items() if s is not None]
        missing_qualities = [q for q in enabled_qualities if q not in available_qualities]
        
        audio_type = detect_audio_type(stream_links)
        
        logger.info(f"Quality mapping result - Available: {available_qualities}, Missing: {missing_qualities}")
        
        if not available_qualities:
            logger.error(f"No suitable qualities found for {anime_title} Episode {episode_number}")
            # Defer - no qualities available at all
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=[],
                missing_qualities=missing_qualities,
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Rᴇᴀsᴏɴ: Nᴏ ǫᴜᴀʟɪᴛɪᴇs ᴀᴠᴀɪʟᴀʙʟᴇ\n"
                    f"・ Sᴛᴀᴛᴜs: Wɪʟʟ ʀᴇᴛʀʏ ʟᴀᴛᴇʀ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        # KEY LOGIC: If ANY selected quality is missing, DO NOT process the episode.
        # Defer it and wait until ALL selected qualities have embed URLs available.
        if missing_qualities:
            logger.info(f"NOT processing {anime_title} Ep{episode_number}: missing qualities {missing_qualities}. "
                       f"Available: {available_qualities}. Will retry later when all qualities are available.")
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=available_qualities,
                missing_qualities=missing_qualities,
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Aᴠᴀɪʟᴀʙʟᴇ: {', '.join(available_qualities)}\n"
                    f"・ Mɪssɪɴɢ: {', '.join(missing_qualities)}\n"
                    f"・ Sᴛᴀᴛᴜs: Wᴀɪᴛɪɴɢ ғᴏʀ ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        # All qualities are available! Remove from deferred list if it was there
        if deferred_episodes.is_deferred(anime_title, episode_number):
            deferred_episodes.remove_episode(anime_title, episode_number)
            logger.info(f"All qualities now available for {anime_title} Ep{episode_number}, removed from deferred list")
        
        sorted_qualities = sorted(available_qualities, key=lambda x: int(x[:-1]))
        quality_files = {}
        downloaded_qualities = []

        for quality_idx, quality in enumerate(sorted_qualities):
            stream_info = quality_mapping.get(quality)
            if not stream_info:
                continue

            try:
                if progress:
                    await progress.update(
                        f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                        f"・ Qᴜᴀʟɪᴛʏ: {quality} ({quality_idx+1}/{len(sorted_qualities)})\n"
                        f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                        f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ M3U8...</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                        parse_mode='html'
      
