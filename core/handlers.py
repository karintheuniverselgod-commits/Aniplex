from __future__ import annotations
import os
import re
import time
import base64
import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
from bs4 import BeautifulSoup
from telethon import events, types
from telethon.tl import functions
from telethon.tl.custom import Button
from telethon.tl.types import PeerUser
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import WebpageMediaEmptyError

from core.config import *
from core.client import *
from core.state import *
from core.utils import *
from core.anime_api import (
    search_anime, get_episode_list, get_all_episodes, get_latest_releases,
    get_stream_links, extract_m3u8_from_kwik, download_m3u8,
    get_quality_streams, detect_audio_type, get_anime_info,
    find_closest_episode, map_resolution_to_quality_tier
)
from core.download import fast_upload_file, robust_upload_file, rename_video_with_ffmpeg
from core.scheduler import *

logger = logging.getLogger(__name__)

DOWNLOAD_DIR = BASE_DIR / "anime_downloads"

currently_processing = False

async def delete_message_after(message, seconds):
    await asyncio.sleep(seconds)
    try:
        await client.delete_messages(message.chat_id, [message.id])
        logger.info(f"Deleted message {message.id} from chat {message.chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete message: {e}")

async def download_and_upload_quality(anime_title, episode_number, quality, stream_info, 
                                       audio_type, event, progress, channel_format):
    try:
        kwik_url = stream_info['url']
        resolution = stream_info['resolution']
        
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
        
        from core.dl_progress import FFmpegProgressReporter
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
        
        from core.dl_progress import make_upload_status_text
        
        async def _upload_progress(current, total):
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
        caption = full_caption
        
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=caption,
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
        logger.error(f"Error in download_and_upload_quality for {quality}: {e}")
        try:
            if 'download_path' in locals() and os.path.exists(download_path):
                os.remove(download_path)
        except:
            pass
        return None

async def download_anime_by_index(event, index: int, force_redownload: bool = False):
    global currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    logger.info(f"Downloading anime at index {index} from latest airing list...")
    
    if currently_processing:
        await safe_respond(event, "<b><blockquote>ᴀʟʀᴇᴀᴅʏ ᴘʀᴏᴄᴇssɪɴɢ ᴀɴᴏᴛʜᴇʀ ᴀɴɪᴍᴇ. ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ.</b></blockquote>", parse_mode='html')
        return False
    
    currently_processing = True
    try:
        progress = ProgressMessage(client, event.chat_id, f"<b><blockquote>ᴀᴅᴅɪɴɢ ᴛᴀsᴋ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴀɴɪᴍᴇ ᴀᴛ ɪɴᴅᴇx {index}...</b></blockquote>", parse_mode='html')
        if not await progress.send():
            await safe_respond(event, "<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɪɴɪᴛɪᴀʟɪᴢᴇ ᴘʀᴏɢʀᴇss ᴛʀᴀᴄᴋɪɴɢ</b></blockquote>", parse_mode='html')
            return False
        
        await progress.update("<b><blockquote>ғᴇᴛᴄʜɪɴɢ ʟᴀᴛᴇsᴛ ᴀɴɪᴍᴇ ʟɪsᴛ...</b></blockquote>", parse_mode='html')
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            await progress.update("<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇsᴛ ʀᴇʟᴇᴀsᴇs</b></blockquote>", parse_mode='html')
            return False
        
        if index < 1 or index > len(latest_data['data']):
            logger.error(f"Invalid index: {index}")
            await progress.update(f"<b><blockquote>ɪɴᴠᴀʟɪᴅ ɪɴᴅᴇx: {index}. ᴍᴜsᴛ ʙᴇ 1-{len(latest_data['data'])}</b></blockquote>", parse_mode='html')
            return False
        
        anime_data = latest_data['data'][index - 1]
        anime_title = anime_data.get('anime_title', 'Unknown Anime')
        episode_number = anime_data.get('episode', 0)
        
        logger.info(f"Selected anime: {anime_title} Episode {episode_number}")
        await progress.update(
            f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗗𝗘𝗧𝗔𝗜𝗟𝗦 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Sᴛᴀᴛᴜs: Fᴇᴛᴄʜɪɴɢ ᴇᴘɪsᴏᴅᴇ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            await progress.update(f"<b><blockquote>ᴀɴɪᴍᴇ ɴᴏᴛ ғᴏᴜɴᴅ: {anime_title}</b></blockquote>", parse_mode='html')
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            await progress.update(f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ᴇᴘɪsᴏᴅᴇ ʟɪsᴛ ғᴏʀ {anime_title}</b></blockquote>", parse_mode='html')
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
                await progress.update(f"<b><blockquote>ɴᴏ ᴇᴘɪsᴏᴅᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title}</b></blockquote>", parse_mode='html')
                return False
        
        episode_session = target_episode['session']
        
        await progress.update(
            f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗦𝗧𝗥𝗘𝗔𝗠𝗦 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ ᴜʀʟs...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
        if not stream_links:
            logger.error(f"No stream links found for {anime_title} Episode {episode_number}")
            await progress.update(f"<b><blockquote>ɴᴏ sᴛʀᴇᴀᴍ ʟɪɴᴋs ғᴏᴜɴᴅ ғᴏʀ {anime_title} Eᴘ {episode_number}</b></blockquote>", parse_mode='html')
            return False
        
        audio_type = detect_audio_type(stream_links)
        preferred_audio = "jpn"
        
        enabled_qualities = quality_settings.enabled_qualities
        quality_mapping = get_quality_streams(stream_links, enabled_qualities, preferred_audio)
        available_qualities = [q for q, s in quality_mapping.items() if s is not None]
        
        if not available_qualities:
            logger.error(f"No suitable qualities found for {anime_title} Episode {episode_number}")
            await progress.update(
                f"<b><blockquote>ɴᴏ sᴜɪᴛᴀʙʟᴇ ǫᴜᴀʟɪᴛɪᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title} Eᴘ {episode_number}</b></blockquote>",
                parse_mode='html'
            )
            return False
        
        logger.info(f"Available qualities: {available_qualities}")
        sorted_qualities = sorted(available_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality in sorted_qualities:
            stream_info = quality_mapping[quality]
            
            dump_msg_id = await download_and_upload_quality(
                anime_title, episode_number, quality, stream_info,
                audio_type, event, progress, channel_format
            )
            
            if dump_msg_id:
                if quality not in quality_files:
                    quality_files[quality] = []
                quality_files[quality].append(dump_msg_id)
                update_processed_qualities(anime_title, episode_number, quality)
                downloaded_qualities.append(quality)
                logger.info(f"Successfully processed {quality}")
            else:
                logger.error(f"Failed to process {quality}")
        
        if quality_files:
            anilist_info = await get_anime_info(anime_title)
            if anilist_info:
                await post_anime_with_buttons(client, anime_title, anilist_info, episode_number, audio_type, quality_files)
        
        if downloaded_qualities:
            await progress.update(
                f"<b><blockquote>sᴜᴄᴄᴇssғᴜʟʟʏ ᴘʀᴏᴄᴇssᴇᴅ:</blockquote>\n"
                f"<blockquote>ᴀɴɪᴍᴇ: {anime_title}\n"
                f"ᴇᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ: {', '.join(downloaded_qualities)}</b></blockquote>\n",
                parse_mode='html'
            )
            return True
        else:
            await progress.update(
                f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ:</blockquote>\n"
                f"<blockquote>ᴀɴɪᴍᴇ: {anime_title}\n"
                f"ᴇᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs ғᴀɪʟᴇᴅ</b></blockquote>",
                parse_mode='html'
            )
            return False
    
    except Exception as e:
        logger.error(f"Error in download_anime_by_index: {e}")
        await safe_respond(event, f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</b></blockquote>", parse_mode='html')
        return False
    finally:
        currently_processing = False

async def download_episode(event, anime_title, anime_session, episode_number, 
                           episode_session, selected_quality_info):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    try:
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {selected_quality_info.get('text', 'Unknown')}\n"
            f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        kwik_url = selected_quality_info['url']
        resolution = selected_quality_info['resolution']
        audio = selected_quality_info.get('audio', 'jpn')
        quality = f"{resolution}p"
        audio_type = "Dub" if audio == "eng" else "Sub"
        
        m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
        if not m3u8_data:
            await safe_edit(event, "<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴇxᴛʀᴀᴄᴛ sᴛʀᴇᴀᴍ URL.</blockquote></b>", parse_mode='html')
            return
        
        m3u8_url = m3u8_data['m3u8_url']
        m3u8_headers = m3u8_data['headers']
        
        base_name = format_filename(anime_title, episode_number, quality, audio_type)
        main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
        full_caption = f"**{base_name} {main_channel_username}.mkv**"
        filename = sanitize_filename(full_caption)
        download_path = os.path.join(DOWNLOAD_DIR, filename)
        
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
            f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ ᴠɪᴀ M3U8...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        download_start = time.time()
        success = await download_m3u8(m3u8_url, m3u8_headers, download_path)
        
        if not success or not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
            await safe_edit(event, "<b><blockquote>ᴅᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ.</blockquote></b>", parse_mode='html')
            return
        
        download_time = time.time() - download_start
        file_size = os.path.getsize(download_path)
        
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗨𝗣𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
            f"・ Sɪᴢᴇ: {format_size(file_size)}\n"
            f"・ Sᴛᴀᴛᴜs: Uᴘʟᴏᴀᴅɪɴɢ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        thumb = await get_fixed_thumbnail()
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=full_caption,
            thumb_path=thumb,
            max_retries=3
        )
        
        try:
            os.remove(download_path)
        except:
            pass
        
        if dump_msg_id:
            await safe_edit(event,
                f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
                f"・ Sɪᴢᴇ: {format_size(file_size)}\n"
                f"・ Tɪᴍᴇ: {download_time:.1f}s</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
            update_processed_qualities(anime_title, episode_number, quality)
        else:
            await safe_edit(event, "<b><blockquote>ᴜᴘʟᴏᴀᴅ ғᴀɪʟᴇᴅ.</blockquote></b>", parse_mode='html')
    
    except Exception as e:
        logger.error(f"Error in download_episode: {e}")
        await safe_edit(event, f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</blockquote></b>", parse_mode='html')

async def download_anime_batch(event, anime_session, anime_title):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    try:
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            await safe_edit(event, f"<b><blockquote>ɴᴏ ᴇᴘɪsᴏᴅᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title}</blockquote></b>", parse_mode='html')
            return False
        
        total = len(episodes)
        success_count = 0
        
        for idx, ep in enumerate(episodes, 1):
            episode_number = ep['episode']
            episode_session = ep['session']
            
            await safe_edit(event,
                f"<b><blockquote>✦ 𝗕𝗔𝗧𝗖𝗛 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Pʀᴏɢʀᴇss: {idx}/{total}\n"
                f"・ Cᴜʀʀᴇɴᴛ: Eᴘɪsᴏᴅᴇ {episode_number}</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
            
            stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
            if not stream_links:
                logger.warning(f"No streams for Episode {episode_number}, skipping")
                continue
            
            audio_type = detect_audio_type(stream_links)
            enabled_qualities = quality_settings.enabled_qualities
            quality_mapping = get_quality_streams(stream_links, enabled_qualities, "jpn")
            
            for quality, stream_info in quality_mapping.items():
                if stream_info is None:
                    continue
                
                kwik_url = stream_info['url']
                m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
                if not m3u8_data:
                    continue
                
                base_name = format_filename(anime_title, episode_number, quality, audio_type)
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"**{base_name} {main_channel_username}.mkv**"
                filename = sanitize_filename(full_caption)
                download_path = os.path.join(DOWNLOAD_DIR, filename)
                
                dl_success = await download_m3u8(m3u8_data['m3u8_url'], m3u8_data['headers'], download_path)
                
                if dl_success and os.path.exists(download_path) and os.path.getsize(download_path) > 1000:
                    thumb = await get_fixed_thumbnail()
                    dump_msg_id = await robust_upload_file(download_path, full_caption, thumb)
                    try:
                        os.remove(download_path)
                    except:
                        pass
                    if dump_msg_id:
                        success_count += 1
            
            await asyncio.sleep(2)
        
        return success_count > 0
    
    except Exception as e:
        logger.error(f"Error in batch download: {e}")
        return False

def register_handlers():

    @client.on(events.NewMessage(pattern=r'^/start(?:\s+(.*))?$'))
    async def start_handler(event):
        user_id = event.sender_id
        chnl_name = CHANNEL_NAME
        chnl_user = CHANNEL_USERNAME.lstrip("@")
        user = await event.get_sender()
        mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    
        param = event.pattern_match.group(1)

        if param:
            try:
            
                base64_string = param
                string = await decode(base64_string)
                argument = string.split("-")
            
                if len(argument) == 3:
                    try:
                        start = int(int(argument[1]) / abs(DUMP_CHANNEL_ID))
                        end = int(int(argument[2]) / abs(DUMP_CHANNEL_ID))
                    except (ValueError, ZeroDivisionError):
                        await event.respond("Invalid link format.")
                        return
                    
                    if start <= end:
                        ids = list(range(start, end + 1))
                    else:
                        ids = []
                        i = start
                        while i >= end:
                            ids.append(i)
                            i -= 1
                
                elif len(argument) == 2:
                    try:
                        ids = [int(int(argument[1]) / abs(DUMP_CHANNEL_ID))]
                    except (ValueError, ZeroDivisionError):
                        await event.respond("Invalid link format.")
                        return
                else:
                    await event.respond("Invalid link format.")
                    return

                dump_channel = (
                    bot_settings.get("dump_channel_id")
                    or bot_settings.get("dump_channel_username")
                )
                if not dump_channel:
                    await event.respond("Dump channel not configured.")
                    return
    
                try:
                    processing_msg = await event.respond("<b><blockquote>Pʀᴏᴄᴇssɪɴɢ...</b></blockquote>", parse_mode='html')
                    
                    try:
                        messages = await event.client.get_messages(dump_channel, ids=ids)
                    except Exception as e:
                        logger.error(f"Error fetching messages: {e}")
                        await event.respond("Something went wrong while fetching files.")
                        return
                    
                    if not isinstance(messages, list):
                        messages = [messages]
    
                    delete_timer = bot_settings.get("file_delete_timer", 600)
                    minutes = delete_timer // 60

                    track_msgs = []
                    file_count = 0
                    
                    for msg in messages:
                        if msg and msg.media:
                            file_count += 1
                            try:
                                sent_msg = await event.client.send_file(
                                    event.chat_id,
                                    file=msg.media,
                                    caption=msg.message,
                                    force_document=False,
                                    link_preview=False
                                )
                                
                                if delete_timer and delete_timer > 0:
                                    track_msgs.append(sent_msg)
                                
                            except Exception as e:
                                logger.error(f"Error sending file: {e}")
                                continue
    
                    try:
                        await processing_msg.delete()
                    except:
                        pass
    
                    if file_count > 0:
                        final_msg = await event.client.send_message(
                            event.chat_id, 
                            f"<blockquote><b>sᴜᴄᴄᴇssғᴜʟʟʏ sᴇɴᴛ {file_count} ғɪʟᴇ(s)!</b></blockquote>\n"
                            f"<blockquote><b>ғɪʟᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ {minutes} ᴍɪɴs. ᴘʟᴇᴀsᴇ sᴀᴠᴇ ᴏʀ ғᴏʀᴡᴀʀᴅ ᴛʜᴇᴍ ʙᴇғᴏʀᴇ ᴛʜᴇʏ ɢᴇᴛ ᴅᴇʟᴇᴛᴇᴅ.</b></blockquote>",
                            parse_mode='html',
                            link_preview=False
                        )
                        
                        if delete_timer and delete_timer > 0:
                            track_msgs.append(final_msg)
                            for sent_msg in track_msgs:
                                asyncio.create_task(delete_message_after(sent_msg, delete_timer))
                    else:
                        await event.respond("No files found for this request.")
                        
                except Exception as e:
                    logger.error(f"Error sending files: {e}")
                    try:
                        await processing_msg.delete()
                    except:
                        pass
    
            except Exception as e:
                logger.error(f"Error in start_with_param: {e}")
                await event.respond("An error occurred while processing your request.")
    
        else:
            try:
                start_pic_path = bot_settings.get("start_pic", None)
                if start_pic_path and os.path.exists(start_pic_path):
                    start_media = start_pic_path
                else:
                    temp_pic_path = os.path.join(THUMBNAIL_DIR, "start_pic_temp.jpg")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(START_PIC_URL) as response:
                            if response.status == 200:
                                with open(temp_pic_path, 'wb') as f:
                                    f.write(await response.read())
                                start_media = temp_pic_path
                            else:
                                raise Exception("Failed to download start picture")

                caption_text=(
                    f"<blockquote><b>🍁 Hᴇʏ, {mention}!</b></blockquote>\n"
                    f"<blockquote><b><i>I'ᴍ ᴀ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ. ɪ ᴄᴀɴ ᴅᴏᴡɴʟᴏᴀᴅ ᴏɴɢᴏɪɴɢ ᴀɴᴅ ғɪɴɪsʜᴇᴅ ᴀɴɪᴍᴇ ғʀᴏᴍ ᴀɴɪᴍᴇᴘᴀʜᴇ.ʀᴜ ᴀɴᴅ ᴜᴘʟᴏᴀᴅ ᴛʜᴏsᴇ ғɪʟᴇs ᴏɴ ʏᴏᴜʀ ᴄʜᴀɴᴇʟ ᴅɪʀᴇᴄᴛʟʏ...</i></b>\n</blockquote>"
                    f"<blockquote><b>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/{chnl_user}'>{chnl_name}</a></blockquote></b>"
                )
                
                if is_admin(event.chat_id):
                    buttons = [
                        [Button.inline("𝗔𝘂𝘁𝗼 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"auto_settings"), Button.inline("𝗛𝗲𝗹𝗽", b"show_help")],
                    ]
                else:
                    public_buttons = []
                    if DEVELOPER_URL:
                        public_buttons.append(Button.url("𝗗𝗲𝘃𝗲𝗹𝗼𝗽𝗲𝗿", DEVELOPER_URL))
                    if CHANNEL_USERNAME:
                        public_buttons.append(
                            Button.url(
                                "𝗠𝗮𝗶𝗻 𝗖𝗵𝗮𝗻𝗻𝗲𝗹",
                                f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}",
                            )
                        )
                    if SUPPORT_CHANNEL_URL:
                        public_buttons.append(Button.url("𝗦𝘂𝗽𝗽𝗼𝗿𝘁", SUPPORT_CHANNEL_URL))
                    buttons = [public_buttons] if public_buttons else None
    
                try:
                    await event.client.send_file(
                        event.chat_id,
                        start_media,
                        caption=caption_text,
                        parse_mode='HTML',
                        buttons=buttons,
                        link_preview=False
                    )
                except Exception as photo_error:
                    logger.error(f"Primary send_file failed: {photo_error}")
                    raise
            except Exception as e:
                logger.error(f"Error sending start message: {e}")
                await safe_respond(event, "Welcome! I'm an anime bot. Type /help for more info.")

    @client.on(events.NewMessage(pattern='/cancel'))
    async def cancel(event):
        if not is_admin(event.chat_id):
            return
        await safe_respond(event, "<blockquote><b>ᴏᴘᴇʀᴀᴛɪᴏɴ ᴄᴀɴᴄᴇʟʟᴇᴅ. sᴇɴᴅ /sᴛᴀʀᴛ ᴛᴏ ʙᴇɢɪɴ ᴀɢᴀɪɴ.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/add_admin'))
    async def add_admin_command(event):
        if not is_admin(event.chat_id):
            return
        if event.chat_id != ADMIN_CHAT_ID:
            await safe_respond(event, "<blockquote><b>ᴏᴡɴᴇʀ ᴏɴʟʏ!</b></blockquote>", parse_mode='html')
            return
        parts = event.text.split()
        if len(parts) < 2:
            await safe_respond(event, "<blockquote><b>ᴜsᴀɢᴇ:</b> <code>/add_admin [user_id]</code></blockquote>", parse_mode='html')
            return
        try:
            user_id = int(parts[1])
            if add_admin(user_id):
                await safe_respond(event, f"<blockquote><b>sᴜᴄᴄᴇssғᴜʟʟʏ ᴀᴅᴅᴇᴅ {user_id} ᴀs ᴀᴅᴍɪɴ.</b></blockquote>", parse_mode='html')
            else:
                await safe_respond(event, f"<blockquote><b>ғᴀɪʟᴇᴅ ᴛᴏ ᴀᴅᴅ ᴀᴅᴍɪɴ.</b></blockquote>", parse_mode='html')
        except ValueError:
            await safe_respond(event, "<blockquote><b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ᴜsᴇʀ ID.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/remove_admin'))
    async def remove_admin_command(event):
        if not is_admin(event.chat_id):
            return
        if event.chat_id != ADMIN_CHAT_ID:
            await safe_respond(event, "<blockquote><b>ᴏᴡɴᴇʀ ᴏɴʟʏ!</b></blockquote>", parse_mode='html')
            return
        parts = event.text.split()
        if len(parts) < 2:
            await safe_respond(event, "<blockquote><b>ᴜsᴀɢᴇ:</b> <code>/remove_admin [user_id]</code></blockquote>", parse_mode='html')
            return
        try:
            user_id = int(parts[1])
            if remove_admin(user_id):
                await safe_respond(event, f"<blockquote><b>sᴜᴄᴄᴇssғᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ {user_id} ғʀᴏᴍ ᴀᴅᴍɪɴ.</b></blockquote>", parse_mode='html')
            else:
                await safe_respond(event, f"<blockquote><b>ғᴀɪʟᴇᴅ ᴛᴏ ʀᴇᴍᴏᴠᴇ ᴀᴅᴍɪɴ.</b></blockquote>", parse_mode='html')
        except ValueError:
            await safe_respond(event, "<blockquote><b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ᴜsᴇʀ ID.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/del_timer'))
    async def del_timer_command(event):
        if not is_admin(event.chat_id):
            return
        parts = event.text.split()
        if len(parts) == 1:
            current_timer = bot_settings.get("file_delete_timer", 600)
            await safe_respond(event, f"<blockquote><b>ᴄᴜʀʀᴇɴᴛ ᴛɪᴍᴇʀ: {current_timer}s ({current_timer/60:.1f} ᴍɪɴs)</b></blockquote>", parse_mode='html')
        else:
            try:
                seconds = int(parts[1])
                if seconds < 60:
                    await safe_respond(event, "<blockquote><b>ᴛɪᴍᴇʀ ᴍᴜsᴛ ʙᴇ ᴀᴛ ʟᴇᴀsᴛ 60s.</b></blockquote>", parse_mode='html')
                    return
                bot_settings.set("file_delete_timer", seconds)
                await safe_respond(event, f"<blockquote><b>ᴛɪᴍᴇʀ sᴇᴛ ᴛᴏ {seconds}s ({seconds/60:.1f} ᴍɪɴs).</b></blockquote>", parse_mode='html')
            except ValueError:
                await safe_respond(event, "<blockquote><b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/latest'))
    async def latest_command(event):
        if not is_admin(event.chat_id):
            return
        try:
            status_msg = await safe_respond(event, "<blockquote><b>ғᴇᴛᴄʜɪɴɢ ʟᴀᴛᴇsᴛ ᴀɴɪᴍᴇ ʟɪsᴛ...</blockquote></b>", parse_mode='html')
            API_URL = f"{ANIME_SOURCE_BASE_URL}/api?m=airing&page=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        anime_list = data.get('data', [])
                        if not anime_list:
                            await status_msg.edit("<blockquote><b>ɴᴏ ʟᴀᴛᴇsᴛ ᴀɴɪᴍᴇ ᴀᴠᴀɪʟᴀʙʟᴇ.</b></blockquote>", parse_mode='html')
                            return
                        latest_anime_text = "<blockquote><b>Lᴀᴛᴇsᴛ Aɪʀɪɴɢ Aɴɪᴍᴇ:</b></blockquote>\n"
                        for idx, anime in enumerate(anime_list[:10], start=1):
                            title = anime.get('anime_title', 'Unknown')
                            episode = anime.get('episode', 'N/A')
                            latest_anime_text += f"<blockquote><b>{idx}. {title} [E{episode}]</b></blockquote>\n"
                        await status_msg.edit(latest_anime_text, parse_mode='html', link_preview=False)
                    else:
                        await status_msg.edit(f"<blockquote><b>ғᴀɪʟᴇᴅ. sᴛᴀᴛᴜs: {response.status}</b></blockquote>", parse_mode='html')
        except Exception as e:
            logger.error(f"Error in latest_command: {e}")
            await safe_respond(event, "<blockquote><b>sᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/addtask'))
    async def add_task(event):
        if not is_admin(event.chat_id):
            return
        parts = event.text.split()
        if len(parts) < 2:
            try:
                status_msg = await safe_respond(event, "<blockquote><b>ғᴇᴛᴄʜɪɴɢ ʟᴀᴛᴇsᴛ...</blockquote></b>", parse_mode='html')
                API_URL = f"{ANIME_SOURCE_BASE_URL}/api?m=airing&page=1"
                async with aiohttp.ClientSession() as session:
                    async with session.get(API_URL, headers=HEADERS) as response:
                        if response.status == 200:
                            data = await response.json()
                            anime_list = data.get('data', [])
                            if not anime_list:
                                await status_msg.edit("<blockquote><b>ɴᴏ ᴀɴɪᴍᴇ ᴀᴠᴀɪʟᴀʙʟᴇ.</b></blockquote>", parse_mode='html')
                                return
                            text = "<blockquote><b>Lᴀᴛᴇsᴛ Aɪʀɪɴɢ:</b></blockquote>\n"
                            for idx, anime in enumerate(anime_list[:10], start=1):
                                title = anime.get('anime_title', 'Unknown')
                                episode = anime.get('episode', 'N/A')
                                text += f"<blockquote><b>{idx}. {title} [E{episode}]</b></blockquote>\n"
                            text += "\n<b><blockquote>ᴜsᴇ /addtask [number] ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ.</b></blockquote>"
                            await status_msg.edit(text, parse_mode='html', link_preview=False)
            except Exception as e:
                logger.error(f"Error in add_task: {e}")
            return
        try:
            index = int(parts[1])
            if index < 1:
                await safe_respond(event, "<blockquote><b>ɪɴᴅᴇx ᴍᴜsᴛ ʙᴇ ᴘᴏsɪᴛɪᴠᴇ.</b></blockquote>", parse_mode='html')
                return
            await download_anime_by_index(event, index)
        except ValueError:
            await safe_respond(event, "<blockquote><b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ.</b></blockquote>", parse_mode='html')

    @client.on(events.NewMessage(pattern='/redownload'))
    async def redownload(event):
        if not is_admin(event.chat_id):
            return
        parts = event.text.split()
        if len(parts) < 2:
            try:
                status_msg = await safe_respond(event, "<blockquote><b>ғᴇᴛᴄʜɪɴɢ ʟᴀᴛᴇsᴛ...</blockquote></b>", parse_mode='html')
                API_URL = f"{ANIME_SOURCE_BASE_URL}/api?m=airing&page=1"
                async with aiohttp.ClientSession() as session:
                    async with session.get(API_URL, headers=HEADERS) as response:
                        if response.status == 200:
                            data = await response.json()
                            anime_list = data.get('data', [])
                            if not anime_list:
                                await status_msg.edit("<blockquote><b>ɴᴏ ᴀɴɪᴍᴇ ᴀᴠᴀɪʟᴀʙʟᴇ.</b></blockquote>", parse_mode='html')
                                return
                            text = "<blockquote><b>Lᴀᴛᴇsᴛ Aɪʀɪɴɢ:</b></blockquote>\n"
                            for idx, anime in enumerate(anime_list[:10], start=1):
                                title = anime.get('anime_title', 'Unknown')
                                episode = anime.get('episode', 'N/A')
                                text += f"<blockquote><b>{idx}. {title} [E{episode}]</b></blockquote>\n"
                            text += "\n<b><blockquote>ᴜsᴇ /redownload [number] ᴛᴏ ʀᴇᴅᴏᴡɴʟᴏᴀᴅ.</b></blockquote>"
                            await status_msg.edit(text, parse_mode='html', link_preview=False)
            except Exception as e:
                logger.error(f"Error in redownload: {e}")
            return
        try:
            index = int(parts[1])
            if index < 1:
                await safe_respond(event, "<blockquote><b>ɪɴᴅᴇx ᴍᴜsᴛ ʙᴇ ᴘᴏsɪᴛɪᴠᴇ.</b></blockquote>", parse_mode='html')
                return
            await download_anime_by_index(event, index, force_redownload=True)
        except ValueError:
            await safe_respond(event, "<blockquote><b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ.</b></blockquote>", parse_mode='html')

    @client.on(events.CallbackQuery(data=b"close_menu"))
    async def close_menu_callback(event):
        await event.delete()

    @client.on(events.CallbackQuery(data=b"show_help"))
    async def show_help_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        buttons = [[Button.inline("𝗕𝗮𝗰𝗸", b"back_to_main")]]
        await safe_edit(event, HELP_TEXT, buttons=buttons, parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_settings"))
    async def auto_settings_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
        enabled = auto_down
