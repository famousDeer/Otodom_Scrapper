import argparse
import logging
import os

from logging.handlers import RotatingFileHandler
from utils.data_scrapper import OtodomScraper
from utils.visualize_data import Visualization
from utils.save_to_csv import save_to_csv
from time import gmtime, strftime

def setup_logger(name="app_logger", city=''):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_format = logging.Formatter(fmt='%(threadName)s | %(asctime)s | [%(levelname)s] -> %(message)s',
                                   datefmt='%Y-%m-%d %H:%M')

    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # File handler with rotation (max 5MB, 3 backups)
    log_filename = "logs/"+strftime("%Y-%m-%d_%H-%M-%S", gmtime())+"| "+city+" |app.log"
    file_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=3)
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
    # Setup logger
    logger = setup_logger(city=args.city)
    logger.info(f"Starting application with arguments: {vars(args)}")
    
    if args.scrape:
        logger.info(f"Starting scraping for city: {args.city}")
        scraper = OtodomScraper(min_area=args.minarea, max_area=args.maxarea, setup_logger=setup_logger, city=args.city)
        data = scraper.parse_data()
        if data:
            logger.info(f"Scraping completed successfully. Total flats found: {data}")
            logger.info(f"Total flats in database: {scraper.get_total_flats()}")
        else:
            logger.error("Scraping failed")    
    
    if args.visualize:
        logger.info("Starting data visualization")
        visualizer = Visualization(dark_mode=args.darkmode, min_area=args.minarea, max_area=args.maxarea)
        visualizer.visualize()
        logger.info("Visualization completed")
    
    if args.save:
        logger.info("Starting data save to CSV")
        save_to_csv()
        logger.info("Data save completed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Setup logger for exception handling
        logger = setup_logger()
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
