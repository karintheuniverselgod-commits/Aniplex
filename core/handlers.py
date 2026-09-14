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
                m3u8_data = await asyncio.to_threa
