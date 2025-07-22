# # import sqlite3

# # FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'

# # # from search import create_db
# # # create_db()

# # db_name = "/home/glushenko/Desktop/project1/project1/flsite.db"
# # table_name = "cards"
# # column_name = ['card', 'field22']

# # conn = None
# # cursor = None
# # separator = '*****'
# # obj_lines = []

# # try:
# #     conn = sqlite3.connect(db_name)
# #     cursor = conn.cursor()
# #     create_table = f"""
# #     CREATE TABLE IF NOT EXISTS {table_name} (
# #         id INTEGER PRIMARY KEY AUTOINCREMENT,
# #         card TEXT NOT NULL,
# #         field22 TEXT
# #     );
# #     """
# #     cursor.execute(create_table)
# #     conn.commit()
    
# #     with open(FILE_PATH, 'r', encoding='utf-8') as f:
# #         comb_string = ''
# #         comb_field = ''
# #         for line in f:
# #             stripped_line = line.strip()
# #             if stripped_line == separator:
# #                 insert_sql = f"INSERT INTO {table_name} (card, field22) VALUES (?, ?)"
# #                 cursor.execute(insert_sql, (comb_string, comb_field))
# #                 comb_string = ''
# #                 comb_field = ''
# #             else:
# #                 comb_string += stripped_line
# #                 if '#22:' in stripped_line:
# #                     comb_field += stripped_line
    
# #     conn.commit()

# # except FileNotFoundError:
# #     print(f"Error: File '{FILE_PATH}' does not exist.")
# # except sqlite3.Error as e:
# #     print(f"DB error: {e}.")
# #     if conn:
# #         conn.rollback()
# # finally:
# #     if conn:
# #         conn.close()
# #         print('DB connesction is closed.')


# import os
# import json
# import torch
import psycopg
# mport re
# import numpy as np
import time
import gc
# from sentence_transformers import SentenceTransformer
# from pgvector.psycopg import register_vector

# # MODEL 1
# # MODEL_NAME = "sentence-transformers/distilbert-base-nli-mean-tokens"
# # EMBEDDING_DIM = 768

# # MODEL 2
# # MODEL_NAME = "ai-forever/sbert_large_nlu_ru"
# # EMBEDDING_DIM = 1024e

# # device = 'cuda' if torch.cuda.is_available() else 'cpu'
# # model = SentenceTransformer(MODEL_NAME).to(device)

# # def calculate_embeddings(data_entity: list[str]):
# #     with torch.no_grad():
# #         res = []
# #         for data in data_entity:
# #             embeddings = model.encode(data, convert_to_tensor=True)
# #             rslt = embeddings.cpu().numpy().tolist()
# #             res.append(rslt)
# #     return res

# def chunk_generator(data_list, batch_size):
#     for i in range(0, len(data_list), batch_size):
#         yield data_list[i:i + batch_size]

# #DB CONFIG
# DB_NAME = "vectordb"
# DB_USER = "user"
# DB_PASSWORD = "password12345"
# DB_HOST = "localhost"
# DB_PORT = "5432"

# connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
# pattern = r'\^[A-Z]'

# def parsing(rec_id = 0):
#     records = []
#     current_rec = [] 
#     FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'
#     with open(FILE_PATH, 'r', encoding='utf-8') as f:
#         for line in f:
#             stripped_line = line.strip()
#             if stripped_line:
#                 # If the separator ***** was found, then record is complete
#                 if stripped_line == '*****':
#                     current = ''.join(str(current_rec)).strip()
#                     records.append(current)
#                     current_rec = []
#                 else:
#                     if "#200:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#                     if "#210:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#                     if "#225:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#                     if "#300:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#                     if "#972:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#                     if "#711:" in stripped_line:
#                         current_rec.append(stripped_line.rstrip(' '))
#         # json_output = json.dump(records, indent=4, ensure_ascii=False)
#         # return json_output
#         return records

# # def process_batch(batch):    
# #     with psycopg.connect(connection) as conn:

# #         register_vector(conn)

# #         with conn.cursor() as cur:
# #             # cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
# #             # conn.commit()

# #             cur.execute(
# #                 f"""
# #                 CREATE TABLE IF NOT EXISTS cards12 (
# #                 id SERIAL PRIMARY KEY,
# #                 content TEXT,
# #                 embedding VECTOR({EMBEDDING_DIM})
# #                 );
# #                 """
# #             )
# #             conn.commit()
            
# #             batch_emb = calculate_embeddings(batch)
# #             for i in range(len(batch)):
# #                 cur.execute(
# #                     "INSERT INTO cards12 (content, embedding) VALUES (%s, %s)",
# #                     (batch[i], str(batch_emb[i]))
# #                 )
# #             conn.commit()
# #             print(f"{len(batch)} records have been added to db.")

# records = parsing()
# filename = "data.json"
# with open(filename, 'w') as f:
#     json.dump(records, f)

# # try:    
# #     json_result = parsing()

# #     recs = parsing()
# #     print(len(recs))
# #     xk = 0

# #     for batch in chunk_generator(recs[0:1000], 100):
# #         process_batch(batch)
# #         xk += 100
# #         print(f"{xk} cards has been transferred.")
    
# #         gc.collect()
    
# #         time.sleep(1)

# # except psycopg.OperationalError as e:
# #     print(f"DB connection error: {e}")
# # except Exception as e:
# #     print(f'An error has occured: {e}')

FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'
#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

def parsing1(records = [], rec = []):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                # If the separator ***** was found, then record is complete
                if stripped_line == '*****':
                    current = ' '.join(str(rec))
                    records.append(current)
                    rec = []
                else:
                    rec.append(stripped_line.rstrip('\n'))
    return records

def parsing2(records = [], rec = []):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                # If the separator ***** was found, then record is complete
                if stripped_line == '*****':
                    current = ' '.join(str(rec))
                    records.append(current)
                    rec = []
                else:
                    if "#200: " in stripped_line:
                        rec.append(stripped_line.rstrip('\n'))
                    elif "#210: " in stripped_line:
                        rec.append(stripped_line.rstrip('\n'))
                    elif "#225: " in stripped_line:
                        rec.append(stripped_line.rstrip('\n'))
                    elif "#300: " in stripped_line:
                        rec.append(stripped_line.rstrip('\n'))
                    elif "#972: " in stripped_line:
                        rec.append(stripped_line.rstrip('\n'))
    return records

def process_batch_short(batch):    
    with psycopg.connect(connection) as conn:
        with conn.cursor() as cur:
            for i in range(len(batch)):
                cur.execute(
                    "INSERT INTO cards (main_info) VALUES (%s)",
                    (batch[i],)
                    )
            conn.commit()

def process_batch_full(batch):    
    with psycopg.connect(connection) as conn:
        with conn.cursor() as cur:
            for i in range(len(batch)):
                cur.execute(
                    "INSERT INTO cards (content) VALUES (%s)",
                    (batch[i],)
                    )
            conn.commit()

def chunk_generator(data_dict, batch_size):
    for i in range(0, len(data_dict), batch_size):
        yield data_dict[i:i + batch_size]

try:
    recs_full = parsing1()
    recs_short = parsing2()
    xk = 0

    # for batch in chunk_generator(recs_short, 100):
    #     process_batch_short(batch)
    #     xk += 100
    #     print(f"{xk} cards has been transferred.")
    
    #     gc.collect()
    
    #     time.sleep(1)
    for batch in chunk_generator(recs_full, 100):
        process_batch_full(batch)
        xk += 100
        print(f"{xk} cards has been transferred.")
    
        gc.collect()
    
        time.sleep(1)

except psycopg.OperationalError as e:
    print(f"DB connection error: {e}")
except Exception as e:
    print(f'An error has occured: {e}')