import psycopg
import torch
import gc
import time
import re
from pgvector.psycopg import register_vector
from sentence_transformers import SentenceTransformer

# FILE_PATH = '/media/storage/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'
FILE_PATH = '/media/storage/glushenko/Desktop/project1/data/AKO - введенные.TXT'

#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

#MODELS
#1 for cards1
MODEL_NAME1 = "sentence-transformers/distilbert-base-nli-mean-tokens"
EMBEDDING_DIM1 = 768
#2 for cards2
MODEL_NAME2 = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'
EMBEDDING_DIM2 = 768
#3 for cards3
MODEL_NAME3 = 'cointegrated/LaBSE-en-ru'
EMBEDDING_DIM3 = 768
#4 for cards4
MODEL_NAME4 = 'ai-forever/sbert_large_nlu_ru'
EMBEDDING_DIM4 = 1024
#5 for cards5
MODEL_NAME5 = 'sentence-transformers/msmarco-MiniLM-L-12-v3'
EMBEDDING_DIM5 = 384
#6 for cards6
MODEL_NAME6 = 'distiluse-base-multilingual-cased-v1'
EMBEDDING_DIM6 = 512

device = 'cuda' if torch.cuda.is_available() else 'cpu'
# model1 = SentenceTransformer(MODEL_NAME1).to(device)
model2 = SentenceTransformer(MODEL_NAME2).to(device)
# model3 = SentenceTransformer(MODEL_NAME3).to(device)
model4 = SentenceTransformer(MODEL_NAME4).to(device)
# model5 = SentenceTransformer(MODEL_NAME5).to(device)
# model6 = SentenceTransformer(MODEL_NAME6).to(device)

connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

def calculate_embeddings(data_entity: list[str], model):
    with torch.no_grad():
        res = []
        for data in data_entity:
            embeddings = model.encode(data, convert_to_tensor=True)
            rslt = embeddings.cpu().numpy().tolist()
            res.append(rslt)
    return res

def chunk_generator(data_list, batch_size):
    for i in range(0, len(data_list), batch_size):
        yield data_list[i:i + batch_size]

def parsing():
    # patterns to clean the data:
    pattern1 = r'\$[a-z]'
    pattern2 = r'\^[A-Z]'
    pattern3 = r'\^[a-z]'
    pattern4 = r'\^[А-Я]'

    """
    recs is a list of dictionaries.
    Each recs[i] is a dictionary which is corresponded to the record with an index i.
    Each dictionary contains pairs key-value with the following relations:
    'k0' - card text for embedding computation
    'k1' - field #22
    'k2' - field #200
    'k3' - field #210
    'k4' - field #328
    'k5' - field #621
    'k6' - field #675
    'k7' - field #686
    'k8' - field #700
    'k9' - field #822
    'k10' - field #852
    'k11' - field #899
    'k12' - field #903
    'k13' - field #952
    'k14' - field #2147483647
    'k15' - field 46
    'k16' - field 461
    'k17' - field 961
    """
    recs = []
    dictionary = {}
    text = []
    field22 = []
    field200 = []
    field210 = []
    field328 = []
    field621 = []
    field675 = []
    field686 = []
    field700 = []
    field822 = []
    field852 = []
    field899 = []
    field903 = []
    field952 = []
    field46 = []
    field461 = []
    field961 = []
    field_jpg = []
    with open(FILE_PATH, 'r', encoding='Windows-1251', errors='ignore') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                # If the separator ***** was found, then record is complete
                if stripped_line == '*****':
                    dictionary['k0'] = ' '.join(text).strip()
                    # if len(text) == 0:
                    #     dictionary['k0'] = ' '.join(field22).strip()
                    # else:
                    #     dictionary['k0'] = ' '.join(text).strip()
                    if len(field22) > 0:
                        dictionary['k1'] = ' '.join(field22).strip()
                    if len(field700) > 0:
                        dictionary['k8'] = ' '.join(field700).strip()
                    if len(field328) > 0:
                        dictionary['k4'] = ' '.join(field328).strip()
                    if len(field200) > 0:
                        dictionary['k2'] = ' '.join(field200).strip()
                    if len(field210) > 0:
                        dictionary['k3'] = ' '.join(field210).strip()
                    if len(field621) > 0:
                        dictionary['k5'] = ' '.join(field621).strip()
                    if len(field675) > 0:
                        dictionary['k6'] = ' '.join(field675).strip()
                    if len(field686) > 0:
                        dictionary['k7'] = ' '.join(field686).strip()
                    if len(field822) > 0:
                        dictionary['k9'] = ' '.join(field822).strip()
                    if len(field852) > 0:
                        dictionary['k10'] = ' '.join(field852).strip()
                    if len(field899) > 0:
                        dictionary['k11'] = ' '.join(field899).strip()
                    if len(field903) > 0:
                        dictionary['k12'] = ' '.join(field903).strip()
                    if len(field952) > 0:
                        dictionary['k13'] = ' '.join(field952).strip()
                    if len(field_jpg) > 0:
                        dictionary['k14'] = ' '.join(field_jpg).strip()
                    if len(field46) > 0:
                        dictionary['k15'] = ' '.join(field46).strip()
                    if len(field461) > 0:
                        dictionary['k16'] = ' '.join(field461).strip()
                    if len(field961) > 0:
                        dictionary['k17'] = ' '.join(field961).strip()
                    recs.append(dictionary)
                    dictionary = {}
                    text = []
                    field22 = []
                    field200 = []
                    field210 = []
                    field328 = []
                    field621 = []
                    field675 = []
                    field686 = []
                    field700 = []
                    field822 = []
                    field852 = []
                    field899 = []
                    field903 = []
                    field952 = []
                    field_jpg = []
                else:
                    str1 = re.sub(pattern1, ' ', stripped_line)
                    str2 = re.sub(pattern2, ' ', str1)
                    str3 = re.sub(pattern3, ' ', str2)
                    str4 = re.sub(pattern4, ' ', str3)
                    if "#22:" in str4:
                        field22.append(str4.rstrip(' ').replace("#22: ", ""))
                    if "#961:" in str4:
                        field961.append(str4.rstrip(' ').replace("#961: ", ""))
                        text.append(str4.rstrip(' ').replace("#961: ", ""))
                    if "#461:" in str4:
                        field46.append(str4.rstrip(' ').replace("#461: ", ""))
                        text.append(str4.rstrip(' ').replace("#461: ", ""))
                    if "#46:" in str4:
                        field461.append(str4.rstrip(' ').replace("#46: ", ""))
                        text.append(str4.rstrip(' ').replace("#46: ", ""))
                    if "#200:" in str4:
                        field200.append(str4.rstrip(' ').replace("#200: ", ""))
                        text.append(str4.rstrip(' ').replace("#200: ", ""))
                    if "#210:" in str4:
                        field210.append(str4.rstrip(' ').replace("#210: ", ""))
                        text.append(str4.rstrip(' ').replace("#210: ", ""))
                    if "#328:" in str4:
                        field328.append(str4.rstrip(' ').replace("#328: ", ""))
                        text.append(str4.rstrip(' ').replace("#328: ", ""))
                    if "#621:" in str4:
                        field621.append(str4.rstrip(' ').replace("#621: ", ""))
                    if "#675:" in str4:
                        field675.append(str4.rstrip(' ').replace("#675: ", ""))
                    if "#686:" in str4:
                        field686.append(str4.rstrip(' ').replace("#686: ", ""))
                    if "#700:" in str4:
                        field700.append(str4.rstrip(' ').replace("#700: ", ""))
                        text.append(str4.rstrip(' ').replace("#700: ", ""))
                    if "#822:" in str4:
                        field822.append(str4.rstrip(' ').replace("#822: ", ""))
                    if "#852:" in str4:
                        field852.append(str4.rstrip(' ').replace("#852: ", ""))
                    if "#899:" in str4:
                        field899.append(str4.rstrip(' ').replace("#899: ", ""))
                    if "#903:" in str4:
                        field903.append(str4.rstrip(' ').replace("#903: ", ""))
                    if "#952:" in str4:
                        field952.append(str4.rstrip(' ').replace("#952: ", ""))
                    if "#2147483547:" in str4:
                        field_jpg.append(str4.rstrip(' ').replace("#2147483547: ", ""))                  
        return recs

def create_table(emb_dim, tab_name):
    with psycopg.connect(connection) as conn:

        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {tab_name} (
                id SERIAL PRIMARY KEY,
                text TEXT,
                embedding VECTOR({emb_dim})
                );
                """
            )
            conn.commit()

def process_batch(batch, tab_name, model_name):    
    with psycopg.connect(connection) as conn:

        register_vector(conn)

        with conn.cursor() as cur:
            emb = calculate_embeddings(batch, model_name)            
            for i in range(len(batch)):
                cur.execute(
                    f"INSERT INTO {tab_name} (text, embedding) VALUES (%s, %s)",
                    (batch[i], str(emb[i]))
                )
            conn.commit()
            print(f"{len(batch)} records have been added to db.")

table_names = ['cards1', 'cards2']
emb_dimensions = [EMBEDDING_DIM2, EMBEDDING_DIM4]
model_names = [model2, model4]

# try:
#     records = parsing()
#     recs = []
#     for k in range(len(records)):
#         recs.append(records[k]['k0'])
#     for k in range(0, 1):
#         emb_dimension = emb_dimensions[k]
#         table_name = table_names[k]
#         create_table(emb_dimension, table_name)

#         xk = 0

#         for batch in chunk_generator(recs, 100):
#             process_batch(batch, table_name, model_names[k])
#             xk += 100
#             print(f"{xk} cards has been transferred.")
        
#             gc.collect()
        
#             time.sleep(1)

#         k += 1

# except psycopg.OperationalError as e:
#     print(f"DB connection error: {e}")
# except Exception as e:
#     print(f'An error has occured: {e}')

records = parsing()
records[200]