import sqlite3
import pandas as pd

def get_all_indices(db_path="INSTRUMENT_PRICE_MODIFIER.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM INSTRUMENT_PRICE_MODIFIER", conn)
    conn.close()
    return df

# Test
if __name__ == "__main__":
    indices_df = get_all_indices()
    print("Wszystkie ID z tabeli instruments:")
    print(indices_df)
