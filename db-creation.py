# import sqlite3

# FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'

# # from search import create_db
# # create_db()

# db_name = "/home/glushenko/Desktop/project1/project1/flsite.db"
# table_name = "cards"
# column_name = ['card', 'field22']

# conn = None
# cursor = None
# separator = '*****'
# obj_lines = []

# try:
#     conn = sqlite3.connect(db_name)
#     cursor = conn.cursor()
#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         card TEXT NOT NULL,
#         field22 TEXT
#     );
#     """
#     cursor.execute(create_table)
#     conn.commit()
    
#     with open(FILE_PATH, 'r', encoding='utf-8') as f:
#         comb_string = ''
#         comb_field = ''
#         for line in f:
#             stripped_line = line.strip()
#             if stripped_line == separator:
#                 insert_sql = f"INSERT INTO {table_name} (card, field22) VALUES (?, ?)"
#                 cursor.execute(insert_sql, (comb_string, comb_field))
#                 comb_string = ''
#                 comb_field = ''
#             else:
#                 comb_string += stripped_line
#                 if '#22:' in stripped_line:
#                     comb_field += stripped_line
    
#     conn.commit()

# except FileNotFoundError:
#     print(f"Error: File '{FILE_PATH}' does not exist.")
# except sqlite3.Error as e:
#     print(f"DB error: {e}.")
#     if conn:
#         conn.rollback()
# finally:
#     if conn:
#         conn.close()
#         print('DB connesction is closed.')


# import os
# import json
import psycopg
import numpy as np
from sentence_transformers import SentenceTransformer
from pgvector.psycopg import register_vector

#MODEL
MODEL_NAME = "sentence-transformers/distilbert-base-nli-mean-tokens"
EMBEDDING_DIM = 768

def calculate_embeddings(data_entity, model_name=MODEL_NAME):
    model = SentenceTransformer(model_name)
    embedding = model.encode(data_entity)
    return embedding

#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

def parsing(rec_id = 0):
    records = {}
    current_rec = [] 
    FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                # If the separator ***** was found, then record is complete
                if stripped_line == '*****':
                    current = ''.join(current_rec).strip()
                    records[str(rec_id)] = current
                    rec_id += 1
                    current_rec = []
                else:
                    current_rec.append(stripped_line.rstrip(' '))
        #json_output = json.dums(records, indent=4, ensure_ascii=False)
        #return json_output
        return records
try:    
    # json_result = parsing()

    recs = parsing()
    
    with psycopg.connect(connection) as conn:

        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()
            
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS cards3 (
                id SERIAL PRIMARY KEY,
                content TEXT,
                embedding VECTOR({EMBEDDING_DIM})
                );
                """
            )
            conn.commit()

            for rec in recs:
                rec_emb = calculate_embeddings(rec)
                cur.execute(
                    "INSERT INTO cards3 (content, embedding) VALUES (%s, %s)",
                    (rec, rec_emb)
                )
            conn.commit()
            print(f"{len(recs)} records have been added to db.")

except psycopg.OperationalError as e:
    print(f"DB connection error: {e}")
except Exception as e:
    print(f'An error has occured: {e}')