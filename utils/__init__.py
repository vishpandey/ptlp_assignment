import logging


logging.basicConfig(filename='logger.log', filemode='w', 
					format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
					datefmt='%d-%b-%y %H:%M:%S')


logger = logging.getLogger()