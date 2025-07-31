import psycopg
import numpy as np
import torch
from sentence_transformers import SentenceTransformer

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
#3 for cards5
MODEL_NAME5 = 'sentence-transformers/msmarco-MiniLM-L-12-v3'
EMBEDDING_DIM5 = 384

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model2 = SentenceTransformer(MODEL_NAME2).to(device)
device = 'cuda' if torch.cuda.is_available() else 'cpu'
model4 = SentenceTransformer(MODEL_NAME4).to(device)

#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = 'cards1'
ID_COLUMN = 'id'
TEXT_COLUMN = 'text'

def search_query(query: str):
    with torch.no_grad():
        embeddings = model2.encode(query, convert_to_tensor=True)
        data = embeddings.cpu().numpy().tolist()

    connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

    try:
        with psycopg.connect(connection) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, text, 1 - (embedding <=> %s::vector(768)) AS cosine_similarity
                    FROM {TABLE_NAME}
                    ORDER BY embedding <=> %s::vector(768)
                    LIMIT %s
                    """,
                    (str(data), str(data), 10)
                )

                results = cur.fetchall()
                if not results:
                    return False
                else:
                    final_results = []
                    for row in results:
                        doc_id, content, similarity = row
                        #final_results.append(f"ID: {doc_id}, similriy: {similarity:.4f}, card: {content}.\n")
                        final_results.append(doc_id)
                return final_results

    except psycopg.OperationalError as e:
        print(f"DB connection error: {e}")
    except Exception as e:
        print(f'An error has occured: {e}')

def search_author(author: str):
    connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
    
    sql_query = f"""
        SELECT {ID_COLUMN}
        FROM {TABLE_NAME}
        WHERE {TEXT_COLUMN} ILIKE %s;
    """

    search_pattern = f"%{author}%"

    try:
        with psycopg.connect(connection) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, (search_pattern,))

                results = cur.fetchall()
                
                if not results:
                    return False
                else:
                    final_results = [row[0] for row in results]
                    return final_results

    except psycopg.OperationalError as e:
        print(f"DB connection error: {e}")
    except Exception as e:
        print(f'An error has occured: {e}')

def search_query_author(query: str, author: str, limit: int = 10, author_weight: float = 0.5, semantic_weight: float = 0.5):

    with torch.no_grad():
        embeddings = model2.encode(query, convert_to_tensor=True)
        data = embeddings.cpu().numpy().tolist()

    connection_str = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
    
    author_search_pattern = f"%{author}%"

    sql_query = f"""
    SELECT
        id,
        (%s * (1 - (embedding <=> %s::vector(768))))
        +
        (%s * (CASE WHEN text ILIKE %s THEN 1.0 ELSE 0.0 END))
        AS final_score
    FROM
        {TABLE_NAME}
    WHERE
        {TEXT_COLUMN} ILIKE %s OR (embedding <=> %s::vector(768)) < 1.0
    ORDER BY
        final_score DESC
    LIMIT %s;
    """

    try:
        with psycopg.connect(connection_str) as conn:
            with conn.cursor() as cur:
                params = (
                    semantic_weight,
                    str(data),
                    author_weight,
                    author_search_pattern,
                    author_search_pattern,
                    str(data),
                    limit
                )
                cur.execute(sql_query, params)

                results = cur.fetchall()
                if not results:
                    return False
                else:
                    final_results = [row[0] for row in results]
                    return final_results

    except psycopg.OperationalError as e:
        print(f"DB connection error: {e}")
        return "Error: Could not connect to the database."
    except Exception as e:
        print(f'An error has occurred: {e}')
        return "An unexpected error occurred."