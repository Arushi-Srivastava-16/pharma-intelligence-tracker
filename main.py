from dotenv import load_dotenv
load_dotenv()

from src.main import run_pipeline


def pipeline_entry(request):
    result = run_pipeline()
    return str(result), 200
