import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import sqlite3

class Visualization:
    def __init__(self, dark_mode=False, min_area=50, max_area=100):
        self.dark_mode = dark_mode
        self.min_area = min_area
        self.max_area = max_area
        sns.set_theme(context="poster", style="darkgrid" if self.dark_mode else "whitegrid")
        plt.style.use('dark_background' if self.dark_mode else 'default')
        plt.rcParams['figure.figsize'] = [12, 8]
        plt.rcParams['font.size'] = 12

    def __fetch_data(self) -> pd.DataFrame:
        conn = sqlite3.connect('databases/otodom.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables_names = cursor.fetchall()
        tables_names = [tup[0] for tup in tables_names if tup[0] not in ['sqlite_sequence', 'sqlite_stat1']]
        cities = {idx: city for idx, city in enumerate(tables_names, start=1)}
        for key, value in cities.items():
            print(key, ". ", value, sep="")
        try:
            city = int(input("Wybierz miasto wpisujac cyfre: "))
        except:
            print("Must be number")
        df = pd.read_sql_query(f"SELECT * FROM {cities[city]}", conn)
        df['address'] = df['address'].apply(self.__remove_street_name)
        df['address'] = df['address'].apply(lambda x: x.split(',')[0])
        conn.close()
        return df

    def __remove_street_name(self, street: str) -> str:
        if not isinstance(street, str):
            return ''
        street = [s.strip() for s in street.split(',')]
        if len(street) > 3:
            street = street[-3:]
        return ', '.join(street)

    def get_price_per_address(self, df):
        df['rent_price'] = df['rent_price'].replace(0, np.nan)
        
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

    def plot_price_per_meter_per_localization(self, df):
        mean_prices = df[('price_per_meter', 'mean')].sort_values(ascending=False)
        ax = sns.barplot(x=mean_prices.values, y=mean_prices.index)
        plt.xlabel('Cena za metr kwadratowy [zł]')
        plt.ylabel('Lokalizacja')
        plt.title('Lokalizacji po średniej cenie za metr kwadratowy')
        for i, v in enumerate(mean_prices.values):
            ax.text(v, i, f"{v:,.0f} zł", color='black' if not self.dark_mode else 'white', va='center', fontweight='bold')
        plt.tight_layout()
        plt.show()

    def plot_rent_per_localization(self, df):
        mean_prices = df[('rent_price', 'mean')].sort_values(ascending=False)
        ax = sns.barplot(x=mean_prices.values, y=mean_prices.index)
        plt.xlabel('Czynsz [zł]')
        plt.ylabel('Lokalizacja')
        plt.title(f'Średni czynsz do lokalizacji {self.min_area}m\u00b2 - {self.max_area}m\u00b2')

        for i, v in enumerate(mean_prices.values):
            ax.text(v, i, f"{v:,.0f} zł", color='black' if not self.dark_mode else 'white', va='center', fontweight='bold')
        plt.tight_layout()
        plt.show()

    def plot_surface_distribution(self, df):
        plt.figure(figsize=(10, 6))
        sns.histplot(df['surface'], bins=30, kde=True)
        plt.xlabel('Powierzchnia mieszkania [m²]')
        plt.ylabel('Liczba mieszkań')
        plt.title('Rozkład powierzchni mieszkań')
        plt.tight_layout()
        plt.show()

    def plot_price_per_meter_distribution(self, df):
        plt.figure(figsize=(10, 6))
        sns.histplot(df['price_per_meter'], bins=30, kde=True)
        plt.xlabel('Cena za metr kwadratowy [zł]')
        plt.ylabel('Liczba mieszkań')
        plt.title('Rozkład ceny za metr kwadratowy')
        plt.tight_layout()
        plt.show()

    def plot_surface_vs_total_price(self, df):
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=df['surface'], y=df['total_price'])
        plt.xlabel('Powierzchnia mieszkania [m²]')
        plt.ylabel('Cena całkowita [zł]')
        plt.title('Powierzchnia mieszkania vs Cena całkowita')
        plt.tight_layout()
        plt.show()

    def plot_listings_per_address(self, df):
        address_counts = df['address'].value_counts()
        plt.figure(figsize=(10, 8))
        ax = sns.barplot(x=address_counts.values, y=address_counts.index)
        plt.xlabel('Liczba ogłoszeń')
        plt.ylabel('Adres')
        plt.title('Top adresów z największą liczbą ogłoszeń')
        for i, v in enumerate(address_counts.values):
            ax.text(v, i, f"{v:,.0f}", color='black' if not self.dark_mode else 'white', va='center', fontweight='bold')
        plt.tight_layout()
        plt.show()

    def plot_price_per_meter_by_rooms(self, df):
        plt.figure(figsize=(10, 6))
        sns.boxplot(x=df['rooms'], y=df['price_per_meter'])
        plt.xlabel('Liczba pokoi')
        plt.ylabel('Cena za metr kwadratowy [zł]')
        plt.title('Cena za metr kwadratowy w zależności od liczby pokoi')
        plt.tight_layout()
        plt.show()

    def show_address_col(self, df):
        pd.set_option('display.max_rows', None) 
        pd.set_option('display.max_columns', None) 
        pd.set_option('display.width', None)
        df = df.groupby('address').count()
        print("\n=== All Records ===")
        print(df)

    def plot_price_per_meter_boxplot(self, df):
        plt.figure(figsize=(12, 8))
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

    def plot_coefficient_of_variation(self, df):
        plt.figure(figsize=(12, 8))
        cv_values = df[('price_per_meter', 'cv')].sort_values(ascending=False)
        
        ax = sns.barplot(x=cv_values.values, y=cv_values.index)
        plt.xlabel('Współczynnik zmienności [%]')
        plt.ylabel('Lokalizacja')
        plt.title('Współczynnik zmienności ceny za metr kwadratowy według lokalizacji')
        
        for i, v in enumerate(cv_values.values):
            ax.text(v, i, f"{v:.1f}%", color='black' if not self.dark_mode else 'white', va='center', fontweight='bold')
        
        plt.tight_layout()
        plt.show()

    def plot_price_range(self, df):
        plt.figure(figsize=(12, 8))
        
        price_ranges = df[('price_per_meter', 'q75')] - df[('price_per_meter', 'q25')]
        price_ranges = price_ranges.sort_values(ascending=False)
        
        ax = sns.barplot(x=price_ranges.values, y=price_ranges.index)
        plt.xlabel('Rozstęp międzykwartylowy ceny za metr kwadratowy [zł]')
        plt.ylabel('Lokalizacja')
        plt.title('Rozstęp międzykwartylowy ceny za metr kwadratowy według lokalizacji')
        
        for i, v in enumerate(price_ranges.values):
            ax.text(v, i, f"{v:.0f} zł", color='black' if not self.dark_mode else 'white', va='center', fontweight='bold')
        
        plt.tight_layout()
        plt.show()
    
    def visualize(self):
        df = self.__fetch_data()
        self.plot_surface_distribution(df)
        self.plot_price_per_meter_distribution(df)
        self.plot_surface_vs_total_price(df)
        self.plot_listings_per_address(df)
        self.plot_price_per_meter_by_rooms(df)
        df = df[(df['surface'] >= self.min_area) & (df['surface'] <= self.max_area)]
        df = self.get_price_per_address(df)
        self.plot_price_per_meter_per_localization(df)
        self.plot_rent_per_localization(df)
        self.plot_price_per_meter_boxplot(df)
        self.plot_coefficient_of_variation(df)
        self.plot_price_range(df)