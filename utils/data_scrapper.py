import logging
import re
import requests
import psycopg2
import concurrent.futures
import time

from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from logging.handlers import RotatingFileHandler
from math import ceil
from unidecode import unidecode
from typing import Optional, List, Dict, Union, Tuple

class OtodomScraper:
    def __init__(self, 
                 min_area, 
                 max_area, 
                 setup_logger: Optional[logging.Logger] = None,
                 city: Optional[str] = None):
        self.logger = setup_logger(name=__name__, city=city+" Scraper")
        self.user_input = input("Write the city name: ") if city is None else city
        self.city_name, self.city_district, self.vojevodian = self.__get_place_details(self.user_input)
        self.city_name = self.__convert_to_ascii(self.city_name)
        self.city_district = self.__convert_to_ascii(self.city_district)
        self.vojevodian = self.__convert_to_ascii(self.vojevodian)
        self.min_area = min_area
        self.max_area = max_area
        self.page = 1
        self.base_url = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/" + self.vojevodian + "/" + self.city_district + ("/" + self.city_name)*2
        self.params = {
            "limit": 72,
            "ownerTypeSingleSelect": "ALL",
            "areaMin": self.min_area,
            "areaMax": self.max_area,
            "isPromoted": False,
            "by": "LATEST",
            "direction": "DESC",
            "viewType": "listing",
            "page": self.page
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pl,en-US;q=0.7,en;q=0.3",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
            
    def __clean_numeric_data(self, string: str) -> Union[float,int,None]:
        '''
        Function clean numeric data from the preffix and suffix.
        --------------------------------
        Args:
            string (str): The string to clean.
        Example:
            Input: 23 000zł
            Output: 23000
        Returns:
            Float | Int | None
        '''
        string = string.replace(',', '.')
        string = string.split("zł")[0]
        digit_str = re.sub(r'[^\d+.]', '', string)
        if digit_str:
            return float(digit_str) if '.' in digit_str else int(digit_str)
        
    def __convert_to_ascii(self, text: str) -> str:
        '''
        Convert Polish characters to their ASCII equivalents.
        Example: 'gdańsk' -> 'gdansk'
        --------------------------------
        Args:
            text (str): Text to convert
        
        Returns:
            str: Converted text
        '''
        return unidecode(text).lower()
    
    def __create_database(self):
        '''
        Creating database and table for searched city.
        --------------------------------
        '''
        conn = None
        try:
            self.city_name = re.sub(r'\W+', '_', self.city_name)
            conn = psycopg2.connect(
                dbname="otodom_db",
                user="scraper_user",
                password="1234",
                host="localhost"
            )
            cursor = conn.cursor()
            
            self.logger.info(f"Creating table for city: {self.city_name}")

            # Wykonaj komendy SQL
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cities (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrapes (
                    id SERIAL PRIMARY KEY,
                    city_id INTEGER,
                    scrape_date TEXT,
                    FOREIGN KEY (city_id) REFERENCES cities(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS flats (
                    id SERIAL PRIMARY KEY,
                    scrape_id INTEGER,
                    title TEXT,
                    address TEXT,
                    link TEXT,
                    rooms TEXT,
                    surface FLOAT,
                    price_per_meter FLOAT,
                    total_price INTEGER,
                    rent_price INTEGER,
                    FOREIGN KEY (scrape_id) REFERENCES scrapes(id) ON DELETE CASCADE
                )
            ''')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_flats_link ON flats(link)')
            conn.commit()
            self.logger.info(f"Table '{self.city_name}' created or already exists.")

        except psycopg2.Error as e:
            self.logger.error(f"Database error when creating table '{self.city_name}': {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error when creating table '{self.city_name}': {e}")
        finally:
            if conn:
                conn.close()
                self.logger.info("Database connection closed.")

    def __insert_data(self, data: List[Dict[str, Union[str, int, float]]]) -> None:
        '''
        Insert data into the database using relational structure.
        --------------------------------
        Args:
            data: List of dictionaries with flat data.
        '''
        conn = None
        try:
            self.logger.info("Inserting data into the database (relational structure).")
            conn = psycopg2.connect(
                dbname="otodom_db",
                user="scraper_user",
                password="1234",
                host="localhost"
            )
            cursor = conn.cursor()

            # Wstawienie miasta, jeśli nie istnieje
            cursor.execute('INSERT INTO cities (name) VALUES (%s) ON CONFLICT (name) DO NOTHING', (self.city_name,))
            cursor.execute('SELECT id FROM cities WHERE name = %s', (self.city_name,))
            city_id = cursor.fetchone()[0]

            # Wstawienie rekordu scrapowania
            scrape_date = time.strftime('%Y-%m-%d')
            cursor.execute('INSERT INTO scrapes (city_id, scrape_date) VALUES (%s, %s) RETURNING id', (city_id, scrape_date))
            scrape_id = cursor.fetchone()[0]

            # Wstawienie mieszkań
            for flat in data:
                cursor.execute('''
                    INSERT INTO flats (scrape_id, title, address, link, rooms, surface, price_per_meter, total_price, rent_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                ''', (
                    scrape_id,
                    flat.get('title', ''),
                    flat.get('address', ''),
                    flat.get('link', ''),
                    flat.get('rooms', ''),
                    flat.get('surface', 0),
                    flat.get('price_per_meter', 0),
                    flat.get('total_price', 0),
                    flat.get('rent_price', 0)
                ))
            conn.commit()
            self.logger.info(f"Inserted {len(data)} flats for city '{self.city_name}' and date {scrape_date}.")
        except psycopg2.Error as e:
            self.logger.error(f"Database error when inserting scrape data: {e}")
        finally:
            if conn:
                self.logger.info("Database connection closed.")
                conn.close()
    
    def __get_place_details(self, city: str) -> Tuple[str, str, str]:
        '''
        This function is used to get the city name and district name from the city name.
        Example:
        Input: gdynia
        Output: gdynia, gdynia, pomorskie
        --------------------------------
        Args:
            city: The city name.
        
        Returns:
            tuple: A tuple containing the city name, city district name and vojevodian name.
        '''
        raw_address = Nominatim(user_agent="otodom_scraper").geocode(city, language="pl", country_codes="pl", addressdetails=True).address # type: ignore
        if raw_address is None:
            self.logger.error(f"Could not find location for city: {city}")
            return city.lower(), city.lower(), "unknown"
        self.logger.info(f"Found location for city: {city} - {raw_address}")
        address = raw_address.split(',')
        if len(address) < 4:
            return address[0].lower(), address[0].lower(), address[1].split()[1].lower()
        else:
            return address[0].lower(), address[1].split()[1].lower(), address[2].split()[1].lower()

    def get_pageContent(self, url: Optional[str] = None) -> Union[str, None]:
        """
        This function is used to get the HTML content of the page.
        --------------------------------
        Args:
            url: The URL to fetch. If not provided, uses the base URL with parameters.
        Returns:
            str: The HTML content of the page.
        """
        try:
            if url is not None:
                response = requests.get(url, headers=self.headers)
            else:
                response = requests.get(self.base_url, params=self.params, headers=self.headers)
            self.logger.info(f"Requesting URL: {response.url}")
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                if url is not None:
                    response = requests.get(url, headers=self.headers)
                else:
                    self.base_url = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/" + self.vojevodian + "/" + self.city_district + "/" + f"gmina-miejska--{self.city_name}" + "/" + self.city_name
                    response = requests.get(self.base_url, params=self.params, headers=self.headers)
                    self.logger.info(f"Requesting URL: {response.url}")
                    if response.status_code == 200:
                        return response.text
            elif response.status_code == 403:
                self.logger.error("Access forbidden (403). Check your headers or IP restrictions. Waiting 5 minutes before retrying...")
                time.sleep(300)
                self.get_pageContent(url if url else None)
                return None
            else:
                self.logger.error(f"Failed to fetch data: HTTP {response.status_code}")
                return None
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None

    def parse_data(self) -> Union[int, None]:
        '''
        Scrapes property listings data from Otodom website using multiple threads.
        --------------------------------
        Returns:
            int: Total number of successfully scraped items
            None: If scraping fails at initial stage
        '''
        self.totalitems = 0
        all_data = []

        html_content = self.get_pageContent()
        if not html_content:
            self.logger.error("Failed to fetch initial page content")
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        if self.page == 1 and not self.get_page_number(soup):
            self.logger.error("Failed to determine total page count")
            return None

        def process_page(page: int) -> List[Dict]:
            """Process a single page and return extracted data"""
            page_data = []
            self.logger.info(f"Processing page {page}/{self.page-1}")
            self.params["page"] = page
            html_content = self.get_pageContent()
            
            if not html_content:
                self.logger.error(f"Failed to fetch page {page}, skipping")
                return page_data

            soup = BeautifulSoup(html_content, 'html.parser')
            articles = soup.find_all('article', {'data-sentry-component': 'AdvertCard'})
            
            for article in articles:
                try:
                    entry = self._extract_property_data(article)
                    if entry:
                        page_data.append(entry)
                except Exception as e:
                    self.logger.error(f"Failed to process listing on page {page}: {str(e)}")
                    continue
            
            return page_data

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_page = {executor.submit(process_page, page): page 
                             for page in range(1, self.page)}
            
            for future in concurrent.futures.as_completed(future_to_page):
                page_data = future.result()
                all_data.extend(page_data)
                self.totalitems += len(page_data)
        if all_data:
            self.__create_database()
            self.__insert_data(all_data)

        return self.totalitems

    def _extract_property_data(self, article: BeautifulSoup) -> Optional[Dict[str, Union[str, int, float]]]:
        '''
        Extracts property data from a single article element.
        
        Args:
            article: BeautifulSoup object representing a single property listing
            
        Returns:
            dict: Property data if extraction successful
            None: If extraction fails
        '''
        try:
            price_text = article.find_next("span", {'data-sentry-element': 'MainPrice'})
            if not price_text:
                self.logger.error("Missing price_text in property extraction")
                return None
            
            price_per_meter_text = price_text.find_next('span', {'class': 'css-13du2ho'})
            if not price_per_meter_text:
                self.logger.error("Missing price_per_meter_text in property extraction")
                return None
            
            link = price_per_meter_text.find_next('a', {'data-cy': 'listing-item-link'})
            if not link:
                self.logger.error("Missing link in property extraction")
                return None

            title = link.find_next('p', {'data-cy': 'listing-item-title'})
            if not title:
                self.logger.error("Missing title in property extraction")
                return None

            address = title.find_next('p', {'data-sentry-component': 'Address'})
            if not address:
                self.logger.error("Missing address in property extraction")
                return None
            rooms_dd = address.find_next('dd', {'class': 'css-17je0kd'})
            if not rooms_dd:
                self.logger.error("Missing rooms dd in property extraction")
                return None
            rooms_span = rooms_dd.find('span')
            if not rooms_span:
                self.logger.error("Missing rooms span in property extraction")
                return None
            rooms = rooms_span

            surface = rooms.find_next('dd')
            if not surface:
                self.logger.error("Missing surface in property extraction")
                return None

            full_url = 'https://www.otodom.pl' + link['href']
            return {
                'total_price': self.__clean_numeric_data(price_text.text),
                'title': title.text.strip(),
                'address': address.text.strip(),
                'link': full_url,
                'rooms': rooms.text.strip(),
                'surface': self.__clean_numeric_data(surface.text),
                'price_per_meter': self.__clean_numeric_data(price_per_meter_text.text),
                'rent_price': self.get_rent_price(full_url)
            }
        except (AttributeError, KeyError, ValueError) as e:
            import traceback
            tb = traceback.format_exc()
            self.logger.error(f"Property data extraction failed: {str(e)}\nTraceback:\n{tb}")
            return None

    def get_page_number(self, soup: BeautifulSoup) -> Union[int, None]:
        '''
        Function to get the total number of pages in website.
        --------------------------------
        Args:
            soup: The BeautifulSoup object containing the HTML content.
        Returns:
            int: The total number of pages.
        '''
        try:
            items_counter = soup.find("span", {"data-sentry-component": "ItemsCounter"})
            if items_counter:
                total_items = int(items_counter.text.split()[-1])
                self.page = ceil(total_items/72)
                self.logger.info(f"Total items found: {total_items}")
                return self.page
            self.logger.warning("Items counter not found in the page")
            return None
        except (ValueError, IndexError) as e:
            self.logger.error(f"Error parsing items counter: {str(e)}")
            return None

    def get_total_flats(self) -> int:
        '''
        Function to get the total number of flats in DB for the current city.
        --------------------------------
        Returns:
            int: The total number of flats in the database for the current city.
        '''
        conn = None
        try:
            conn = psycopg2.connect(
                dbname="otodom_db",
                user="scraper_user",
                password="1234",
                host="localhost"
            )
            cursor = conn.cursor()

            # Pobranie ID miasta
            cursor.execute('SELECT id FROM cities WHERE name = %s', (self.city_name,))
            city_row = cursor.fetchone()
            if not city_row:
                self.logger.warning(f"No city found with name '{self.city_name}' in database.")
                cursor.close()
                conn.close()
                return 0
            city_id = city_row[0]

            # Zliczenie mieszkań dla danego miasta
            cursor.execute('''
                SELECT COUNT(*)
                FROM flats f
                JOIN scrapes s ON f.scrape_id = s.id
                WHERE s.city_id = %s
            ''', (city_id,))
            total_flats = cursor.fetchone()[0]
        except psycopg2.Error as e:
            self.logger.error(f"Database error when counting flats for city '{self.city_name}': {e}")
            return 0
        except Exception as e:
            self.logger.exception(f"Unexpected error when counting flats for city '{self.city_name}': {e}")
            return 0
        finally:
            if conn:
                self.logger.info("Database connection closed.")
                conn.close()
        return total_flats
    
    def get_rent_price(self, link: str) -> Union[int, float]:
        '''
        Function to get the rent price if exists.
        --------------------------------
        Args:
            link: The URL of the listing to fetch rent price from.
        Returns:
            int: The rent price if found, otherwise 0.
        '''
        html_content = self.get_pageContent(url=link)
        soup = BeautifulSoup(html_content, 'html.parser') # type: ignore
        first_item = soup.find("div", {"data-sentry-element": "ItemGridContainer", "data-sentry-source-file": "AdDetailItem.tsx"})
        for i in range(4):
            first_item = first_item.find_next("div", {"data-sentry-element": "ItemGridContainer", "data-sentry-source-file": "AdDetailItem.tsx"})
        value = self.__clean_numeric_data(first_item.text.split(":")[1])
        return value if value is not None else 0
