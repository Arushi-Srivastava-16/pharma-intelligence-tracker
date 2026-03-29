from dotenv import load_dotenv
load_dotenv()

from src.main import run_pipeline


def pipeline_entry(environ, start_response):
    """WSGI-compatible entry point for gunicorn on Cloud Run."""
    result = run_pipeline()
    body = str(result).encode()
    start_response("200 OK", [
        ("Content-Type", "text/plain"),
        ("Content-Length", str(len(body))),
    ])
    return [body]
