import pandas as pd
import sqlite3

def save_to_csv():
    conn = sqlite3.connect('databases/otodom.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables_names = cursor.fetchall()
    tables_names = [tup[0] for tup in tables_names if tup[0] not in ['sqlite_sequence', 'sqlite_stat1']]
    cities = {idx: city for idx, city in enumerate(tables_names, start=1)}
    for key, value in cities.items():
        print(key, ". ", value, sep="")
    try:
        city = int(input("Wybierz miasto wpicujac cyfre: "))
    except:
        print("Must be number")
    df = pd.read_sql_query(f"SELECT * FROM {cities[city]}", conn)
    conn.close()
    df.to_csv(f"{cities[city]}.csv", index=False)
    print(f"Data saved to {cities[city]}.csv")

if __name__ == "__main__":
    save_to_csv()