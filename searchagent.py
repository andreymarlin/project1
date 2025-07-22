import psycopg
import time
import gc
from pathlib import Path
from agno.agent import Agent, AgentKnowledge
from agno.embedder.ollama import OllamaEmbedder
from agno.models.ollama import Ollama
from agno.vectordb.pgvector import PgVector
from agno.knowledge.json import JSONKnowledgeBase
from ollama import Client

# DB config
DB_NAME = "vectordb"
DB_USER = "user"
DB_PASSWORD = "password12345"
DB_HOST = "localhost"
DB_PORT = "5432"

PG_TABLE_NAME = "cards"
PG_VECTOR_COLUMN_NAME = "embedding"
PG_CONTENT_COLUMN_NAME = "content"

connection = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

FILE_PATH = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'

# Initialize Ollama client
ollama_client = Client(host="http://10.1.15.44:11434//")

# Setup paths
cwd = Path(__file__).parent
tmp_dir = cwd.joinpath("tmp")
tmp_dir.mkdir(parents=True, exist_ok=True)

embedding_model = OllamaEmbedder(
    id="mxbai-embed-large:latest",
    ollama_client=ollama_client,
)

def create_table_if_not_exists():
    with psycopg.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.commit()
            print("pgvector extension ensured to be present.")

            cur.execute(
                f"""
    CREATE TABLE IF NOT EXISTS cards (
    id SERIAL PRIMARY KEY;
    content TEXTm
    embedding VECTOR(1024) );
    """)
            conn.commit()

def insert_batch_into_db(batch_data):
    try:
        with psycopg.connect(connection) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO cards (content, embedding) VALUES (%s, %s)",
                    batch_data
                )
                conn.commit()
        return True
    except Exception as e:
        print(f"Error inserting batch: {e}")
        return False

def parse_and_embed_generator(file_path):

    current_full_rec_lines = []
    
    # Счётчик для отслеживания прогресса и обработки потенциально больших файлов
    records_processed_in_file = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            stripped_line = line.strip()
            if not stripped_line:
                continue # Пропускаем пустые строки

            if stripped_line == '*****':
                # Конец записи, формируем строки и генерируем эмбеддинги
                full_record_str = ' '.join(current_full_rec_lines).strip()

                embedding = None
                if full_record_str: # Генерируем эмбеддинг только если есть контент
                    try:
                        embedding = embedding_model.embed(full_record_str)
                        embedding = embedding.tolist() # Agno embedder может вернуть NumPy array
                    except Exception as e:
                        print(f"Warning: Failed to generate embedding for record at line {line_num}. Error: {e}")
                        embedding = [] # Устанавливаем пустой список, чтобы избежать ошибки вставки

                yield (full_record_str, embedding)
                
                # Очищаем буферы для следующей записи
                current_full_rec_lines = []
                records_processed_in_file += 1
                if records_processed_in_file % 1000 == 0:
                    print(f"Parsed and prepared {records_processed_in_file} records from file...")
            else:
                current_full_rec_lines.append(stripped_line) # Всегда добавляем в полную запись

    print(f"Finished parsing file. Total records from file: {records_processed_in_file}")


# --- ГЛАВНЫЙ БЛОК ВЫПОЛНЕНИЯ ---

if __name__ == "__main__":
    start_time = time.time()
    try:
        # 1. Убедимся, что таблица и расширение pgvector существуют
        create_table_if_not_exists()

        # 2. Опционально: очистить таблицу перед заполнением
        # if truncate_table(): # Раскомментируйте, если хотите очистить таблицу перед заполнением
        #    pass
        # else:
        #    print("Skipping data population as table truncation was cancelled.")
        #    exit() # Выход, если пользователь отменил очистку

        batch_size = 100 # Размер пакета для вставки (можно увеличить до 100-500 для больших файлов)
        current_batch_for_insertion = []
        total_records_inserted = 0

        print(f"Starting data population into '{DB_NAME}.{PG_TABLE_NAME}' table...")

        # 3. Итерируем по записям из генератора, собираем пакеты и вставляем
        for short_rec, full_rec, embedding in parse_and_embed_generator(FILE_PATH):
            if embedding is None: # Пропускаем записи, для которых не удалось сгенерировать эмбеддинг
                print(f"Skipping record (no embedding): {full_rec[:100]}...")
                continue
            
            current_batch_for_insertion.append((short_rec, full_rec, embedding))
            total_records_inserted += 1

            if len(current_batch_for_insertion) >= batch_size:
                if insert_batch_into_db(current_batch_for_insertion):
                    print(f"--- {total_records_inserted} cards transferred and embedded. ---")
                    current_batch_for_insertion = [] # Очищаем пакет
                    gc.collect() # Запускаем сборщик мусора
                    # time.sleep(0.1) # Опционально: небольшая пауза, если нагрузка слишком велика

        # 4. Вставляем оставшиеся записи, если они есть в последнем неполном пакете
        if current_batch_for_insertion:
            if insert_batch_into_db(current_batch_for_insertion):
                print(f"--- {total_records_inserted} cards transferred and embedded. ---")

        print(f"\nData population complete! Total records inserted: {total_records_inserted}")

    except psycopg.OperationalError as e:
        print(f"DB connection error: {e}")
    except Exception as e:
        print(f'An unexpected error has occurred: {e}')
    finally:
        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")


# Create vector database with correct embedding dimensions
knowledge_base = PgVector(
    connection_string=connection,
    collection_name=PG_TABLE_NAME,
    embedding_field=PG_VECTOR_COLUMN_NAME,
    content_field=PG_CONTENT_COLUMN_NAME,
    embedding_model=embedding_model,
    vector_dimension=1024
)

# Create an agent with the knowledge base
knowledge_agent = Agent(
    model=Ollama(id="llama3.1:8b", client=ollama_client),
    knowledge=knowledge_base,
    search_knowledge=True,
    #debug_mode=True,
    instructions=[
        "You are a superfast library worker. Use the provided knowledge base to find relevant records from library catalog.",
        "Focus on matching the card information from the query",
        "When providing search result include 10 most relevant records to the query",
        "If multiple properties match the criteria, list not more then 10 records",
        "You need to return the whole card record from given json card",
        "I want information only from document, not from your knowledge",
        "Crucially, provide only the raw, exact search results. Do not add any introductory phrases like 'Here are the results:', 'I found:', or 'Based on your query:'. Just list the matching records directly, one per line or in a clear structured format like JSON if specified.",
        "If no results are found, simply state 'No relevant records found.'",
        "Present the results clearly, for example, as a list of found card records.",
    ]
)

# message = 'Книги про построение коммунизма'
def searchagent(message):
    # Process the message through the AI team
    # response = team_leader.run(message.text) #.process(message.text)
    # response = property_agent2.run(message.text)
    response = knowledge_agent.run(message)
    #logging.info(f"Response: {response.content}")
    #print(response)
    return response.content

print(searchagent('Металлургические конструкции.'))