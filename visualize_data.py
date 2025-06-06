import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import sqlite3

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = [12, 8]
plt.rcParams['font.size'] = 12
min_meters = 50
max_meters = 70

def load_data():
    conn = sqlite3.connect('otodom.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables_names = cursor.fetchall()
    cities = {idx: city for idx, tup in enumerate(tables_names[1:], start=1) for city in tup }
    for key, value in cities.items():
        print(key, ". ", value, sep="")
    try:
        city = int(input("Wybierz miasto wpicujac cyfre: "))
    except:
        print("Must be number")
    df = pd.read_sql_query(f"SELECT * FROM {cities[city]}", conn)
    conn.close()
    return df

def remove_street(street):
    street = [s.strip() for s in street.split(',')]
    if len(street) > 3:
        street = street[-3:]
    return ', '.join(street)

def get_price_per_address(df):
    df['rent_price'] = df['rent_price'].replace(0, np.nan)
    
    # Define custom aggregation functions
    def cv(x):
        return (x.std() / x.mean()) * 100 if x.mean() != 0 else np.nan
    
    return df.groupby('address').agg({
        'total_price': [
            'count',
            'mean',
            'median',
            'std',
            ('cv', cv),
            ('q25', lambda x: x.quantile(0.25)),
            ('q75', lambda x: x.quantile(0.75)),
            'min',
            'max'
        ],
        'price_per_meter': [
            'mean',
            'median',
            'std',
            ('cv', cv),
            ('q25', lambda x: x.quantile(0.25)),
            ('q75', lambda x: x.quantile(0.75)),
            'min',
            'max'
        ],
        'rent_price': [
            'mean',
            'median',
            'std',
            ('cv', cv),
            ('q25', lambda x: x.quantile(0.25)),
            ('q75', lambda x: x.quantile(0.75))
        ]
    }).round(2).sort_values(('price_per_meter', 'mean'), ascending=True)

def print_statistics(df):
    print("\n=== Detailed Statistics ===")
    print(f"\nTotal number of apartments: {len(df)}")
    
    print(df)

def plot_price_per_meter_per_localization(df):
    # Sort by mean price_per_meter
    mean_prices = df[('price_per_meter', 'mean')].sort_values(ascending=False)
    ax = sns.barplot(x=mean_prices.values, y=mean_prices.index)
    plt.xlabel('Cena za metr kwadratowy [zł]')
    plt.ylabel('Lokalizacja')
    plt.title('Lokalizacji po średniej cenie za metr kwadratowy')
    # Annotate each bar with the price value
    for i, v in enumerate(mean_prices.values):
        ax.text(v, i, f"{v:,.0f} zł", color='black', va='center', fontweight='bold')
    plt.tight_layout()
    plt.show()

def plot_rent_per_localization(df):
    mean_prices = df[('rent_price', 'mean')].sort_values(ascending=False)
    ax = sns.barplot(x=mean_prices.values, y=mean_prices.index)
    plt.xlabel('Czynsz [zł]')
    plt.ylabel('Lokalizacja')
    plt.title(f'Średni czynsz do lokalizacji {min_meters}m\u00b2 - {max_meters}m\u00b2')

    for i, v in enumerate(mean_prices.values):
        ax.text(v, i, f"{v:,.0f} zł", color='black', va='center', fontweight='bold')
    plt.tight_layout()
    plt.show()

def plot_surface_distribution(df):
    plt.figure(figsize=(10, 6))
    sns.histplot(df['surface'], bins=30, kde=True)
    plt.xlabel('Powierzchnia mieszkania [m²]')
    plt.ylabel('Liczba mieszkań')
    plt.title('Rozkład powierzchni mieszkań')
    plt.tight_layout()
    plt.show()

def plot_price_per_meter_distribution(df):
    plt.figure(figsize=(10, 6))
    sns.histplot(df['price_per_meter'], bins=30, kde=True)
    plt.xlabel('Cena za metr kwadratowy [zł]')
    plt.ylabel('Liczba mieszkań')
    plt.title('Rozkład ceny za metr kwadratowy')
    plt.tight_layout()
    plt.show()

def plot_surface_vs_total_price(df):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=df['surface'], y=df['total_price'])
    plt.xlabel('Powierzchnia mieszkania [m²]')
    plt.ylabel('Cena całkowita [zł]')
    plt.title('Powierzchnia mieszkania vs Cena całkowita')
    plt.tight_layout()
    plt.show()

def plot_listings_per_address(df):
    address_counts = df['address'].value_counts()
    plt.figure(figsize=(10, 8))
    ax = sns.barplot(x=address_counts.values, y=address_counts.index)
    plt.xlabel('Liczba ogłoszeń')
    plt.ylabel('Adres')
    plt.title('Top adresów z największą liczbą ogłoszeń')
    for i, v in enumerate(address_counts.values):
        ax.text(v, i, f"{v:,.0f}", color='black', va='center', fontweight='bold')
    plt.tight_layout()
    plt.show()

def plot_price_per_meter_by_rooms(df):
    plt.figure(figsize=(10, 6))
    sns.boxplot(x=df['rooms'], y=df['price_per_meter'])
    plt.xlabel('Liczba pokoi')
    plt.ylabel('Cena za metr kwadratowy [zł]')
    plt.title('Cena za metr kwadratowy w zależności od liczby pokoi')
    plt.tight_layout()
    plt.show()

def show_address_col(df):
    # Display all records
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.width', None)  # Auto-detect display width
    df = df.groupby('address').count()
    print("\n=== All Records ===")
    print(df)

def plot_price_per_meter_boxplot(df):
    plt.figure(figsize=(12, 8))
    # Create boxplot using the new statistics
    data_to_plot = []
    labels = []
    for address in df.index:
        q25 = df.loc[address, ('price_per_meter', 'q25')]
        median = df.loc[address, ('price_per_meter', 'median')]
        q75 = df.loc[address, ('price_per_meter', 'q75')]
        min_val = df.loc[address, ('price_per_meter', 'min')]
        max_val = df.loc[address, ('price_per_meter', 'max')]
        data_to_plot.append([min_val, q25, median, q75, max_val])
        labels.append(address)
    
    plt.boxplot(data_to_plot, labels=labels, vert=False)
    plt.xlabel('Cena za metr kwadratowy [zł]')
    plt.ylabel('Lokalizacja')
    plt.title('Rozkład ceny za metr kwadratowy według lokalizacji')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_coefficient_of_variation(df):
    plt.figure(figsize=(12, 8))
    cv_values = df[('price_per_meter', 'cv')].sort_values(ascending=False)
    
    ax = sns.barplot(x=cv_values.values, y=cv_values.index)
    plt.xlabel('Współczynnik zmienności [%]')
    plt.ylabel('Lokalizacja')
    plt.title('Współczynnik zmienności ceny za metr kwadratowy według lokalizacji')
    
    # Add value labels
    for i, v in enumerate(cv_values.values):
        ax.text(v, i, f"{v:.1f}%", color='black', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.show()

def plot_price_range(df):
    plt.figure(figsize=(12, 8))
    
    # Calculate price ranges (q75 - q25)
    price_ranges = df[('price_per_meter', 'q75')] - df[('price_per_meter', 'q25')]
    price_ranges = price_ranges.sort_values(ascending=False)
    
    ax = sns.barplot(x=price_ranges.values, y=price_ranges.index)
    plt.xlabel('Rozstęp międzykwartylowy ceny za metr kwadratowy [zł]')
    plt.ylabel('Lokalizacja')
    plt.title('Rozstęp międzykwartylowy ceny za metr kwadratowy według lokalizacji')
    
    # Add value labels
    for i, v in enumerate(price_ranges.values):
        ax.text(v, i, f"{v:.0f} zł", color='black', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    df = load_data()
    df['address'] = df['address'].apply(remove_street)
    # show_address_col(df)
    plot_surface_distribution(df)
    plot_price_per_meter_distribution(df)
    plot_surface_vs_total_price(df)
    plot_listings_per_address(df)
    plot_price_per_meter_by_rooms(df)
    df = df[(df['surface'] >= min_meters) & (df['surface'] <= max_meters)]
    df = get_price_per_address(df)
    plot_price_per_meter_per_localization(df)
    plot_rent_per_localization(df)
    # Add new visualizations
    plot_price_per_meter_boxplot(df)
    plot_coefficient_of_variation(df)
    plot_price_range(df)