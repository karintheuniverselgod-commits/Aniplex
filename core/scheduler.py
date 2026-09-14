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
                    f"<
