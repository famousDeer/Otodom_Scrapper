import argparse
import logging

from logging.handlers import RotatingFileHandler
from utils.data_scrapper import OtodomScraper
from utils.visualize_data import Visualization
from utils.save_to_csv import save_to_csv
from time import gmtime, strftime

def setup_logger(name="app_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_format = logging.Formatter(fmt='%(threadName)s | %(asctime)s | [%(levelname)s] -> %(message)s',
                                   datefmt='%Y-%m-%d %H:%M')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # File handler with rotation (max 5MB, 3 backups)
    file_handler = RotatingFileHandler("logs/"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+" | app.log", maxBytes=5*1024*1024, backupCount=3)
    file_handler.setFormatter(log_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def main():
    parser = argparse.ArgumentParser(description="Run the Otodom data scraper and analysis tool.")
    parser.add_argument('--scrape', action='store_true', help="Run the web scraper to collect data from Otodom.")
    parser.add_argument('city', type=str, help="City name to scrape data for.")
    parser.add_argument('--minarea', type=int, default=0, help="Minimum area in meters for filtering properties.")
    parser.add_argument('--maxarea', type=int, default=1000, help="Maximum area in meters for filtering properties.")
    parser.add_argument('--visualize', action='store_true', help="Visualize the data collected from Otodom.")
    parser.add_argument('--save', action='store_true', help="Save the data to a CSV file.")
    parser.add_argument('--darkmode', action='store_true', help="Use dark mode for visualizations.")
    args = parser.parse_args()
    if args.scrape:
        scraper = OtodomScraper(min_area=args.minarea, max_area=args.maxarea, setup_logger=setup_logger, city=args.city)
        data = scraper.parse_data()
        if data:
            logger.info(f"Scraping completed successfully. Total flats found: {data}")
            logger.info(f"Total flats in database: {scraper.get_total_flats()}")
        else:
            logger.error("Scraping failed")    
    if args.visualize:
        visualizer = Visualization(dark_mode=args.darkmode, min_area=args.minarea, max_area=args.maxarea)
        visualizer.visualize()
    if args.save:
        save_to_csv()

if __name__ == "__main__":
    logger = setup_logger()
    try:
        main()

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
