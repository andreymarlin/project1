import psycopg
import numpy as np
from modelemb import calculate_embeddings

#DB CONFIG
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

def search(query):
    query_emb = calculate_embeddings(query)

    connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

    try:
        with psycopg.connect(connection) as conn:
            with conn.sursor() as cur:
                cur.execute(
                    """
                    SELECT id, content, 1 - (embedding <=> %s) AS cosine_similarity
                    FROM cards
                    ORDER BY embedding <=> %s
                    LIMIT %s
                    """,
                    (query_emb, query_emb, 10)
                )

                results = cur.fetchall()
                if not results:
                    print("Nothing has been found.")
                else:
                    for row in results:
                        doc_id, content, similarity = row
                        print(f"ID: {doc_id}, similriy: {similarity:.4f}, card: {content}.")
    except psycopg.OperationalError as e:
        print(f"DB connection error: {e}")
    except Exception as e:
        print(f'An error has occured: {e}')