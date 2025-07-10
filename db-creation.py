import sqlite3
import os

FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'

db_name = "books.db"
table_name = "cards"
column_name = ['card', 'field22']

conn = None
cursor = None
separator = '*****'
obj_lines = []

try:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        card TEXT NOT NULL
        field22 TEXT
    );
    """
    cursor.execute(create_table)
    conn.commit()
    
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line == separator:
                insert_sql = f"INSERT INFO {table_name} (card, field22) VALUES (?, ?)"
                cursor.execute(insert_sql, (comb_string, comb_field))
                comb_string = ''
                comb_field = ''
            else:
                comb_string = comb_string.join(stripped_line)
                if '#22:' in stripped_line:
                    comb_field = comb_field.join(stripped_line)
    
    conn.commit()

except FileNotFoundError:
    print(f"Error: File '{FILE_PATH}' does not exist.")
except sqlite3.Error as e:
    print(f"DB error: {e}.")
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
        print('DB connesction is closed.')