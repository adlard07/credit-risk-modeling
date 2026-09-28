import logging

logging.basicConfig(
	format='%(asctime)s - %(levelname)s: \n%(message)s',
	filename='src/artifacts/app.log',
	filemode='w',
	level=logging.DEBUG,
)

