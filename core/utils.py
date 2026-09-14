from __future__ import annotations
import os
import re
import time
import logging
import asyncio
import aiohttp
import requests
import base64
from datetime import datetime
from typing import List, Optional
import base64

from telethon.errors import FloodWaitError
from telethon.tl.types import PeerChannel

from core.config import (
    THUMBNAIL_DIR, FIXED_THUMBNAIL_URL, START_PIC_URL,
    ADMIN_CHAT_ID, BOT_USERNAME,
    DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME
)
from core.database import (
    admins_collection, processed_episodes_collection,
    anime_banners_collection, anime_hashtags_collection,
    load_json_data, save_json_data
)

logger = logging.getLogger(__name__)

def sanitize_filename(file_name: str) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*]', '', file_name)
    return sanitized.strip()

def create_short_name(name: str, max_length: int = 30) -> str:
    if len(name) > max_length:
        return ''.join(word[0].upper() for word in name.split())
    return name

def format_size(size_bytes: int) -> str:
    if not isinstance(size_bytes, (int, float)):
        return "0 B"
        
    if size_bytes < 0:
        return "0 B"
        
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.2f} MB"
    else:
        return f"{size_bytes/(1024**3):.2f} GB"

def format_speed(speed_bytes):
    if not isinstance(speed_bytes, (int, float)):
        return "0 B/s"
        
    if speed_bytes < 1024:
        return f"{speed_bytes} B/s"
    elif speed_bytes < 1024**2:
        return f"{speed_bytes/1024:.2f} KB/s"
    elif speed_bytes < 1024**3:
        return f"{speed_bytes/(1024**2):.2f} MB/s"
    else:
        return f"{speed_bytes/(1024**3):.2f} GB/s"

def format_time(seconds):
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_filename(anime_title, episode_number, quality, type_str):
    season_match = re.search(r'Season (\d+)', anime_title, re.IGNORECASE)
    if season_match:
        season = f"S{int(season_match.group(1)):02d}"
    else:
        season = "S01"
    
    ep_num = f"{int(episode_number):02d}"
    
    clean_title = re.sub(r'\s*\(.*?\)\s*', '', anime_title)
    clean_title = re.sub(r'\s*\[.*?\]\s*', '', clean_title)
    clean_title = clean_title.strip()
    
    return f"[{season}-{ep_num}] {clean_title} [{quality}] [{type_str}]"

async def resolve_channel_entity(client, channel_id_or_username):
    entity = await client.get_entity(channel_id_or_username)

    if not isinstance(entity, types.Channel):
        raise ValueError("Target is not a channel")

    return await client.get_input_entity(PeerChannel(entity.id))

def download_start_pic(url: str, save_path=THUMBNAIL_DIR / "start_pic.jpg"):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"[INFO] Start pic downloaded and saved as '{save_path}'")
        return str(save_path)
    except Exception as e:
        print(f"[ERROR] Failed to download start pic: {e}")
        return None

def download_start_pic_if_not_exists(url: str, save_path=THUMBNAIL_DIR / "start_pic.jpg"):
    if save_path.exists():
        print(f"[INFO] Start pic already exists at '{save_path}'")
        return str(save_path)
    return download_start_pic(url, save_path)

async def get_fixed_thumbnail():
    thumbnail_path = os.path.join(THUMBNAIL_DIR, "fixed_thumbnail.png")
    
    if not os.path.exists(thumbnail_path):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(FIXED_THUMBNAIL_URL) as response:
                    if response.status == 200:
                        with open(thumbnail_path, 'wb') as f:
                            f.write(await response.read())
                        logger.info("Downloaded fixed thumbnail")
        except Exception as e:
            logger.error(f"Error downloading fixed thumbnail: {e}")
    
    return thumbnail_path if os.path.exists(thumbnail_path) else None

def is_admin(user_id: int) -> bool:
    if user_id == ADMIN_CHAT_ID:
        return True
    
    if admins_collection is not None:
        try:
            result = admins_collection.find_one({"user_id": user_id})
            return result is not None
        except Exception as e:
            logger.error(f"Error checking admin status: {e}")
            return False
    else:
        data = load_json_data()
        for admin in data.get("admins", []):
            if admin["user_id"] == user_id:
                return True
        return False

def add_admin(user_id: int, username: str = None) -> bool:
    if admins_collection is not None:
        try:
            admins_collection.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "added_at": datetime.now()
                }},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding admin: {e}")
            return False
    else:
        data = load_json_data()
        
        for admin in data.get("admins", []):
            if admin["user_id"] == user_id:
                return True
        
        data.setdefault("admins", []).append({
            "user_id": user_id,
            "username": username,
            "added_at": datetime.now().isoformat()
        })
        
        save_json_data(data)
        return True

def remove_admin(user_id: int) -> bool:
    if admins_collection is not None:
        try:
            result = admins_collection.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            return False
    else:
        data = load_json_data()
        admins = data.get("admins", [])
        
        for i, admin in enumerate(admins):
            if admin["user_id"] == user_id:
                admins.pop(i)
                save_json_data(data)
                return True
        
        return False

def is_episode_processed(anime_title: str, episode_number: int) -> bool:
    from core.state import quality_settings
    
    if processed_episodes_collection is not None:
        try:
            result = processed_episodes_collection.find_one({
                "anime_title": anime_title,
                "episode_number": episode_number
            })
            if result:
                processed_qualities = set(result.get("qualities", []))
                enabled_qualities = set(quality_settings.enabled_qualities)
                
                # If no qualities recorded but entry exists, treat as not fully processed
                if not processed_qualities:
                    logger.warning(f"Episode {anime_title} Ep{episode_number} exists in DB but has no qualities recorded")
                    return False
                
                is_complete = processed_qualities.issuperset(enabled_qualities)
                if is_complete:
                    logger.debug(f"Episode {anime_title} Ep{episode_number} fully processed: {processed_qualities}")
                else:
                    logger.info(f"Episode {anime_title} Ep{episode_number} partially processed: have {processed_qualities}, need {enabled_qualities}")
                return is_complete
            return False
        except Exception as e:
            logger.error(f"Error checking processed episode: {e}")
            return False
    else:
        data = load_json_data()
        for ep in data["processed_episodes"]:
            if ep["anime_title"] == anime_title and ep["episode_number"] == episode_number:
                processed_qualities = set(ep.get("qualities", []))
                enabled_qualities = set(quality_settings.enabled_qualities)
                if not processed_qualities:
                    return False
                return processed_qualities.issuperset(enabled_qualities)
    return False

def update_processed_qualities(anime_title: str, episode_number: int, quality: str) -> bool:
    if processed_episodes_collection is not None:
        try:
            doc = processed_episodes_collection.find_one({
                "anime_title": anime_title,
                "episode_number": episode_number
            })
            
            if doc:
                current_qualities = set(doc.get("qualities", []))
                current_qualities.add(quality)
                processed_episodes_collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "qualities": list(current_qualities),
                        "updated_at": datetime.now()
                    }}
                )
            else:
                processed_episodes_collection.insert_one({
                    "anime_title": anime_title,
                    "episode_number": episode_number,
                    "qualities": [quality],
                    "created_at": datetime.now()
                })
            return True
        except Exception as e:
            logger.error(f"Error updating processed qualities: {e}")
            return False
    else:
        data = load_json_data()

        entry_exists = False
        for ep in data["processed_episodes"]:
            if ep["anime_title"] == anime_title and ep["episode_number"] == episode_number:
                if quality not in ep["qualities"]:
                    ep["qualities"].append(quality)
                    ep["updated_at"] = datetime.now().isoformat()
                entry_exists = True
                break
        
        if not entry_exists:
            data["processed_episodes"].append({
                "anime_title": anime_title,
                "episode_number": episode_number,
                "qualities": [quality],
                "created_at": datetime.now().isoformat()
            })
        
        save_json_data(data)
        return True

def mark_episode_processed(anime_title: str, episode_number: int, qualities: List[str] = None) -> bool:
    from core.state import quality_settings
    
    # If no qualities provided, use current enabled qualities as fallback
    if not qualities:
        qualities = quality_settings.enabled_qualities
        logger.warning(f"mark_episode_processed called without qualities for {anime_title} Ep{episode_number}, using enabled: {qualities}")
    
    if processed_episodes_collection is not None:
        try:
            processed_episodes_collection.update_one(
                {"anime_title": anime_title, "episode_number": episode_number},
                {"$set": {
                    "qualities": qualities,
                    "processed_at": datetime.now()
                }},
                upsert=True
            )
            logger.info(f"Marked {anime_title} Ep{episode_number} as processed in DB with qualities: {qualities}")
            return True
        except Exception as e:
            logger.error(f"Error marking episode as processed: {e}")
            return False
    else:
        data = load_json_data()
        # Check if entry already exists
        existing = None
        for ep in data["processed_episodes"]:
            if ep["anime_title"] == anime_title and ep["episode_number"] == episode_number:
                existing = ep
                break
        
        if existing:
            # Update existing entry
            existing["qualities"] = qualities
            existing["processed_at"] = datetime.now().isoformat()
        else:
            data["processed_episodes"].append({
                "anime_title": anime_title,
                "episode_number": episode_number,
                "qualities": qualities,
                "processed_at": datetime.now().isoformat()
            })
        save_json_data(data)
        logger.info(f"Marked {anime_title} Ep{episode_number} as processed in JSON with qualities: {qualities}")
        return True

def is_banner_posted(anime_title: str) -> bool:
    if anime_banners_collection is not None:
        try:
            result = anime_banners_collection.find_one({"anime_title": anime_title})
            return result is not None
        except Exception as e:
            logger.error(f"Error checking banner posted: {e}")
            return False
    else:
        data = load_json_data()
        for banner in data["posted_banners"]:
            if banner["anime_title"] == anime_title:
                return True
        return False

def mark_banner_posted(anime_title: str) -> bool:
    if anime_banners_collection is not None:
        try:
            anime_banners_collection.update_one(
                {"anime_title": anime_title},
                {"$set": {"posted_at": datetime.now()}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error marking banner posted: {e}")
            return False
    else:
        data = load_json_data()
        data["posted_banners"].append({
            "anime_title": anime_title,
            "posted_at": datetime.now().isoformat()
        })
        save_json_data(data)
        return True

def get_anime_hashtag(anime_title: str) -> str:
    if anime_hashtags_collection is not None:
        try:
            result = anime_hashtags_collection.find_one({"anime_title": anime_title})
            if result:
                return result["hashtag"]
        except Exception as e:
            logger.error(f"Error getting anime hashtag: {e}")
    else:
        data = load_json_data()
        for hashtag_data in data["anime_hashtags"]:
            if hashtag_data["anime_title"] == anime_title:
                return hashtag_data["hashtag"]
    
    predefined_hashtags = {
        "Lord of the Mysteries": "LOTM",
        "Nukitashi THE ANIMATION": "NTA",
        "ONE PIECE": "OP",
        "There's No Freaking Way I'll be Your Lover! Unless...": "Freaking",
        "Grand Blue Dreaming Season 2": "GB",
        "SAKAMOTO DAYS Part 2": "SD",
        "Sword of the Demon Hunter: Kijin Gentosho": "KG",
        "Solo Leveling": "SL",
        "Jujutsu Kaisen": "JK",
        "Demon Slayer": "DS",
        "Attack on Titan": "AOT",
        "My Hero Academia": "MHA",
        "Naruto": "NAR",
        "Bleach": "BL",
        "Dragon Ball Z": "DBZ",
        "Death Note": "DN",
        "One Punch Man": "OPM",
        "Tokyo Ghoul": "TG",
        "Black Clover": "BC",
        "Hunter x Hunter": "HXH",
        "Fullmetal Alchemist": "FMA",
        "Steins;Gate": "SG",
        "Re:Zero": "RZ",
        "No Game No Life": "NGL",
        "The Rising of the Shield Hero": "SH",
        "Sword Art Online": "SAO",
        "Fairy Tail": "FT",
        "Dragon Ball Super": "DBS",
        "Boruto": "BOR",
        "Dragon Ball": "DB",
        "One Piece Film: Red": "OPR",
        "Demon Slayer: Entertainment District Arc": "DS2",
        "Attack on Titan Final Season": "AOT4",
        "Jujutsu Kaisen 0": "JK0",
        "My Hero Academia: Heroes Rising": "MHA2",
        "Fate/Zero": "FZ",
        "Fate/stay night": "FSN",
        "Fate/stay night: Unlimited Blade Works": "UBW",
        "Fate/Apocrypha": "FA",
        "Fate/Grand Order": "FGO",
        "Fate/kaleid liner Prisma Illya": "PI",
        "Fate/hollow ataraxia": "FH",
        "Fate/EXTRA": "FE",
        "Fate/strange Fake": "FSF",
        "Fate/Prototype": "FP",
        "Fate/Labyrinth": "FL",
        "Fate/side material": "FSM",
        "Fate/complete material": "FCM",
        "Fate/extra material": "FEM",
        "Fate/Apocrypha material": "FAM",
        "Fate/Grand Order material": "FGOM",
        "Fate/Zero material": "FZM",
        "Fate/stay night material": "FSNM",
        "Fate/hollow ataraxia material": "FHM",
        "Fate/EXTRA material material": "FEMMMM",
        "Fate/strange Fake material material": "FSFMMM",
        "Fate/Prototype material material": "FPMMM",
        "Fate/Labyrinth material material": "FLMMM",
        "Fire Force Season 3 Part 2": "FFS3P2",
        "Chained Soldier Season 2": "CSS2",
        "Oshi No Ko Season 3": "ONKS3"
    }
    
    for title, hashtag in predefined_hashtags.items():
        if title.lower() in anime_title.lower():
            if anime_hashtags_collection is not None:
                try:
                    anime_hashtags_collection.update_one(
                        {"anime_title": anime_title},
                        {"$set": {"hashtag": hashtag}},
                        upsert=True
                    )
                except Exception as e:
                    logger.error(f"Error storing anime hashtag: {e}")
            else:
                data = load_json_data()
                data["anime_hashtags"].append({
                    "anime_title": anime_title,
                    "hashtag": hashtag,
                    "created_at": datetime.now().isoformat()
                })
                save_json_data(data)
            
            return hashtag
    
    words = re.findall(r'\b\w+\b', anime_title)
    common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'of', 'to', 'in', 'on', 'at', 'for', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'between', 'among', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should', 'may', 'might', 'must', 'can', 'could'}
    
    filtered_words = [word for word in words if word.lower() not in common_words and len(word) > 2]
    
    if filtered_words:
        hashtag = ''.join([word[0].upper() for word in filtered_words[:3]])
    else:
        hashtag = words[0][:3].upper() if words else "ANI"
    
    if anime_hashtags_collection is not None:
        try:
            anime_hashtags_collection.update_one(
                {"anime_title": anime_title},
                {"$set": {"hashtag": hashtag}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error storing anime hashtag: {e}")
    else:
        data = load_json_data()
        data["anime_hashtags"].append({
            "anime_title": anime_title,
            "hashtag": hashtag,
            "created_at": datetime.now().isoformat()
        })
        save_json_data(data)
    
    return hashtag

async def encode(string):
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    base64_string = (base64_bytes.decode("ascii")).strip("=")
    return base64_string

async def decode(base64_string):
    base64_string = base64_string.strip("=")
    base64_bytes = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes = base64.urlsafe_b64decode(base64_bytes) 
    string = string_bytes.decode("ascii")
    return string

async def get_messages(client, message_ids):
    messages = []
    total_messages = 0
    while total_messages != len(message_ids):
        temb_ids = message_ids[total_messages:total_messages+200]
        try:
            msgs = await client.get_messages(
                chat_id=DUMP_CHANNEL_ID,
                message_ids=temb_ids
            )
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            msgs = await client.get_messages(
                chat_id=DUMP_CHANNEL_ID,
                message_ids=temb_ids
            )
        except:
            pass
        total_messages += len(temb_ids)
        messages.extend(msgs)
    return messages

async def get_message_id(client, message):
 
