from __future__ import annotations

import logging
from typing import Optional

from core.config import CHANNEL_NAME, BOT_USERNAME, CHANNEL_USERNAME
from core.utils import format_size, format_speed, format_time
from core.downloader import DownloadProgress

logger = logging.getLogger(__name__)

class FFmpegProgressReporter:

    def __init__(
        self,
        progress_message,
        anime_title: str,
        episode_number,
        quality: str,
        audio_type: str,
        channel_format: Optional[str] = None,
    ):
        self.progress = progress_message
        self.anime_title = anime_title
        self.episode_number = episode_number
        self.quality = quality
        self.audio_type = audio_type
        self.channel_format = channel_format or (CHANNEL_USERNAME or BOT_USERNAME).lstrip("@")

    def _render(self, snap: DownloadProgress) -> str:
        status_label = {
            "starting":     "Sᴛᴀʀᴛɪɴɢ ғғᴍᴘᴇɢ...",
            "downloading":  "Dᴏᴡɴʟᴏᴀᴅɪɴɢ ᴠɪᴀ M3U8...",
            "processing":   "Pʀᴏᴄᴇssɪɴɢ...",
            "done":         "Dᴏɴᴇ ✓",
            "failed":       "Fᴀɪʟᴇᴅ ✗",
        }.get(snap.status, snap.status)

        downloaded = format_size(snap.downloaded_bytes)
        speed = format_speed(snap.speed_bps) if snap.speed_bps else "—"
        elapsed = format_time(snap.elapsed) if snap.elapsed else "0s"
        eta = format_time(snap.eta) if snap.eta else "—"

        return (
            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>"
            f"・ Aɴɪᴍᴇ: {self.anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {self.episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {self.quality} ({self.audio_type})\n"
            f"・ Dᴏᴡɴʟᴏᴀᴅᴇᴅ: {downloaded}\n"
            f"・ Sᴘᴇᴇᴅ: {speed}\n"
            f"・ Dᴜʀᴀᴛɪᴏɴ: {snap.duration} (@ {snap.current_time})\n"
            f"・ Eʟᴀᴘsᴇᴅ: {elapsed}\n"
            f"・ ETA: {eta}\n"
            f"・ Sᴛᴀᴛᴜs: {status_label}"
            f"</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{self.channel_format}'>{CHANNEL_NAME}</a></blockquote></b>"
        )

    async def callback(self, snap: DownloadProgress) -> None:
        if not self.progress:
            return
        try:
            await self.progress.update(self._render(snap), parse_mode="html")
        except Exception as e:
            logger.debug("FFmpegProgressReporter update failed: %s", e)

def make_upload_status_text(
    anime_title,
    episode_number,
    quality: str,
    audio_type: str,
    file_size_bytes: int,
    channel_format: Optional[str] = None,
    extra_status: str = "Uᴘʟᴏᴀᴅɪɴɢ ᴛᴏ Tᴇʟᴇɢʀᴀᴍ...",
) -> str:
    cf = channel_format or (CHANNEL_USERNAME or BOT_USERNAME).lstrip("@")
    return (
        f"<b><blockquote>✦ 𝗨𝗣𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
        f"──────────────────\n"
        f"<blockquote>"
        f"・ Aɴɪᴍᴇ: {anime_title}\n"
        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
        f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
        f"・ Sɪᴢᴇ: {format_size(file_size_bytes)}\n"
        f"・ Sᴛᴀᴛᴜs: {extra_status}"
        f"</blockquote>\n"
        f"──────────────────\n"
        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{cf}'>{CHANNEL_NAME}</a></blockquote></b>"
    )

