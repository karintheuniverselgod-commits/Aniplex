from core.config import (
    BASE_DIR, LOG_DIR, DOWNLOAD_DIR, THUMBNAIL_DIR, DB_NAME,
    API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID, MONGO_URI, PORT, BOT_USERNAME,
    BOT_DISPLAY_NAME, SUPPORT_CHANNEL_URL, DEVELOPER_URL,
    CHANNEL_ID, CHANNEL_USERNAME, DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME,
    FIXED_THUMBNAIL_URL, START_PIC_URL,
    AUTO_DOWNLOAD_STATE_FILE, QUALITY_SETTINGS_FILE, SESSION_FILE, JSON_DATA_FILE,
    HEADERS, ANILIST_API, ANIME_SOURCE_BASE_URL, FFMPEG_PATH,
    Config, logger, CHANNEL_NAME, DELETE_TIMER, HELP_TEXT,
    SEARCH, SELECT_ANIME, SELECT_EPISODE, SELECT_QUALITY, DOWNLOADING,
    AUTO_DISABLED, AUTO_ENABLED, WEB_PORT
)

from core.database import (
    mongo_client, db,
    processed_episodes_collection, anime_banners_collection,
    anime_hashtags_collection, admins_collection, bot_settings_collection,
    load_json_data, save_json_data,
    save_bot_setting, load_bot_setting
)

from core.client import (
    client, pyro_client, PYROFORK_AVAILABLE, FFMPEG_AVAILABLE,
    currently_processing, processing_lock
)

from core.state import (
    AnimeQueue, QualitySettings, BotSettings, AutoDownloadState, UserState,
    anime_queue, quality_settings, bot_settings, auto_download_state, user_states,
    EpisodeState, EpisodeTracker, episode_tracker
)

from core.utils import (
    sanitize_filename, create_short_name, format_size, format_speed, format_time, format_filename,
    download_start_pic, download_start_pic_if_not_exists,
    get_fixed_thumbnail, is_admin, add_admin, remove_admin,
    is_episode_processed, update_processed_qualities, mark_episode_processed,
    is_banner_posted, mark_banner_posted, get_anime_hashtag,
    encode, decode, generate_batch_link, generate_single_link,
    ProgressMessage, UploadProgressBar,
    safe_edit, safe_respond, safe_send_message
)

from core.anime_api import (
    search_anime, get_episode_list, get_latest_releases, get_all_episodes,
    get_stream_links, extract_m3u8_from_kwik, download_m3u8,
    get_quality_streams, detect_audio_type, map_resolution_to_quality_tier,
    get_anime_info, download_anime_poster, find_closest_episode
)

from core.download import (
    rename_video_with_ffmpeg, fast_upload_file, robust_upload_file
)

from core.scheduler import (
    setup_scheduler, process_all_qualities, check_for_new_episodes,
    auto_download_latest_episode, process_specific_anime,
    process_daily_requests, reschedule_daily_requests,
    post_anime_with_buttons, post_anime_batch_with_buttons
)

from core.handlers import register_handlers

