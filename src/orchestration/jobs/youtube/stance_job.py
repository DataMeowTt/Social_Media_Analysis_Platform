import os

from src.youtube.analytics.analytics_job import run_stance_analysis

YOUTUBE_ID = os.getenv("YOUTUBE_VIDEO_ID", "nHkKJ87FS6s")

if __name__ == "__main__":
    run_stance_analysis(youtube_id=YOUTUBE_ID)
