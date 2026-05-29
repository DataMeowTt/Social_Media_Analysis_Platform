"""
Local convenience script — runs YouTube sentiment + stance analysis for a given video.

Usage:
    YOUTUBE_VIDEO_ID=<video_id> ENV=dev PYTHONPATH=. python scripts/run_youtube_local.py
"""
import os
from src.youtube.analytics.analytics_job import analysis_local
from src.utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    video_id = os.environ["YOUTUBE_VIDEO_ID"]
    analysis_local(youtube_id=video_id)
