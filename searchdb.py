import psycopg
import numpy as np
import torch
from sentence_transformers import SentenceTransformer

#MODEL
MODEL_NAME = "ai-forever/sbert_large_nlu_ru"
EMBEDDING_DIM = 1024

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = SentenceTransformer(MODEL_NAME).to(device)

#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

def calculate_embeddings(data_entity: list[str]):
    with torch.no_grad():
        embeddings = model.encode(data_entity, convert_to_tensor=True)
        data = embeddings.cpu().numpy().tolist()
    return data

def searchdb(query: str):
    query_emb = calculate_embeddings(query)

    connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

    try:
        with psycopg.connect(connection) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, content, 1 - (embedding <=> %s::vector(1024)) AS cosine_similarity
                    FROM cards7
                    ORDER BY embedding <=> %s::vector(1024)
                    LIMIT %s
                    """,
                    (str(query_emb), str(query_emb), 10)
                )

                results = cur.fetchall()
                if not results:
                    return(["Nothing has been found."])
                else:
                    results_list = []
                    for row in results:
                        doc_id, content, similarity = row
                        results_list.append(f"ID: {doc_id}, \nsimilarity: {similarity:.4f}, \ncard: {content}.\n")
                    return results_list

    except psycopg.OperationalError as e:
        return(["DB connection error: {e}"])
    except Exception as e:
        return(['An error has occured: {e}'])