# import json #???
# import os
# import numpy as np
from sentence_transformers import SentenceTransformer

# JSON_FILE_PATH = ""
# EMBEDDING_OUTPUT = "/home/glushenko/Desktop/project1/project1/embeddings.json"

MODEL_NAME = "sentence-transformers/distilbert-base-nli-mean-tokens"
# MODEL_NAME = "sberbank-ai/sbert_ru_base_cased"

# def load_json_data():
#     with open(JSON_FILE_PATH, 'r', encoding='utf-8')
#     data = json.load(f)
#     return data

def calculate_embeddings(data_entity, model_name=MODEL_NAME):
    model = SentenceTransformer(model_name)
    embedding = model.encode(data_entity)
    return embedding

print(calculate_embeddings('I like watermelons.'))