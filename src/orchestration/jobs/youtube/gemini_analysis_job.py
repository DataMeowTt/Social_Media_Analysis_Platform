import os

from src.youtube.conversation.gemini_analysis import run_gemini_analysis

YOUTUBE_ID = os.getenv("YOUTUBE_VIDEO_ID", "nHkKJ87FS6s")

if __name__ == "__main__":
    run_gemini_analysis(youtube_id=YOUTUBE_ID)
