import os

from src.youtube.ingestion.ingestion_job import run_ingestion_comments

YOUTUBE_ID = os.getenv("YOUTUBE_VIDEO_ID", "nHkKJ87FS6s")


def run_ingestion_task(**context):
    run_ingestion_comments(video_id=YOUTUBE_ID)
