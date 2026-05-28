import os

from src.youtube.conversation.thread_builder import run_conversation_builder

YOUTUBE_ID = os.getenv("YOUTUBE_VIDEO_ID", "nHkKJ87FS6s")
FORCE = os.getenv("FORCE_REANALYZE", "").lower() == "true"

if __name__ == "__main__":
    run_conversation_builder(youtube_id=YOUTUBE_ID, force=FORCE)
