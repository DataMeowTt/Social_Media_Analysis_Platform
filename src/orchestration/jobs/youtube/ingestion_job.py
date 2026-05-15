from src.youtube.ingestion.ingestion_job import run_ingestion_comments


def run_ingestion_task(video_id: str, num_comments: int = 1000, **context):
    run_ingestion_comments(video_id=video_id, num_comments=num_comments)
