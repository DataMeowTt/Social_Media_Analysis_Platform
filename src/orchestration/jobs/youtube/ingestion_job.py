import os

from src.youtube.ingestion.ingestion_job import run_ingestion_comments

YOUTUBE_ID = os.environ["YOUTUBE_VIDEO_ID"]


def run_ingestion_task(**_):
    run_ingestion_comments(video_id=YOUTUBE_ID)
