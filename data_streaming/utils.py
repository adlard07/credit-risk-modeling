from pathlib import Path
import os
import logging
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def create_path(path):
	filepath = Path(path)
	filepath.mkdir(parents=True, exist_ok=True)



create_path(os.path.join(os.getenv("ARTIFACTS"), 'logs'))

logging.basicConfig(
	format='%(asctime)s - %(levelname)s: \n%(message)s',
	filename='artifacts/logs/app.log',
	filemode='w',
	level=logging.DEBUG,
)

