# Apartment Prices data

A Python application for scraping and visualizing apartment prices from Otodom.pl. This tool helps analyze real estate prices across different cities in Poland, providing insights through various data visualizations. User provide city name which he want to analyze, then data scrapper script will download available information depend on the user restrictions.

## Features

- Web scraping of apartment listings from Otodom.pl
- Data storage in SQLite database
- Comprehensive data visualization including:
  - Price per square meter distribution
  - Surface area distribution
  - Price correlation with location
  - Rental prices analysis
  - Statistical analysis by location
  - Price variation coefficient analysis

## Requirements

- Python 3.x
- Required Python packages (listed in requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd CenyMieszkan
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### 1. Data Collection

Run the scraper to collect apartment data:
```bash
python data_scrapper.py
```
- Enter the city name when prompted
- The script will collect data for apartments between 50-100m²
- Data is saved to a SQLite database (otodom.db)
- Log file will also be provide as well with terminal information logging

### 2. Data Visualization

To visualize the collected data:
```bash
python visualize_data.py
```

First user will be asked to choose from available cities. List of the cities will be listed on terminal.

This will generate several visualizations:
- Surface area distribution
- Price per square meter distribution
- Surface vs. Total price correlation
- Number of listings per address
- Price per square meter by number of rooms
- Price analysis by location
- Rent price analysis
- Statistical variations and ranges

## Visualizations Description

1. **Surface Distribution**: Shows the distribution of apartment sizes
2. **Price per Meter Distribution**: Displays the spread of prices per square meter
3. **Surface vs Price**: Correlation between apartment size and total price
4. **Listings per Address**: Shows which locations have the most listings
5. **Price per Meter by Rooms**: Box plots showing price variations based on number of rooms
6. **Location Analysis**: Various graphs showing price statistics by location
7. **Coefficient of Variation**: Shows price variability in different locations
8. **Price Range Analysis**: Displays the interquartile range of prices by location

## Data Structure

The collected data includes:
- Title
- Address
- Link to offer
- Number of rooms
- Surface area
- Price per square meter
- Total price
- Rent price (if available)

## Error Handling

The application includes:
- Robust error handling for web scraping
- Logging system with rotation
- Duplicate entry prevention
- Database connection management

## Limitations

- Only works with Polish cities
- Limited to Otodom.pl listings apartments for sale
- Focuses on apartments between 50-100m²
- Subject to website structure changes

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT-License
