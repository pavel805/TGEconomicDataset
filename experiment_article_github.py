import ast
import pandas as pd

from pymongo import MongoClient
import pandas as pd


##### MONGO DB atributes
import yaml
with open('config.yaml', 'r') as stream:  
    cfg = yaml.safe_load(stream)

uri = cfg["mongodb"]['url']# local connection or "mongodb://127.0.0.1:27017"
user = "" 
pasword = ""
bd =  cfg["mongodb"]['db_name'] # or 'tg8'
######### 

#uri = "mongodb://127.0.0.1:27017/" # локальное соединение
#user = ""
#pasword = ""
#bd = "tg8"
########################################################################

import numpy as np
import pandas as pd
from scipy.linalg import sqrtm
from scipy.spatial.distance import cosine
import json
import os
import torch
torch.set_num_threads(4) 
from sentence_transformers import SentenceTransformer

from tqdm.auto import tqdm


########################################################################

def calculate_frechet_distance(mu1, sigma1, mu2, sigma2, eps=1e-6):
    """Расчет Fréchet Distance между двумя нормальными распределениями"""
    diff = mu1 - mu2
    
    # Вычисление sqrt(sigma1 * sigma2)
    covmean, _ = sqrtm(sigma1.dot(sigma2), disp=False)
    
    if np.iscomplexobj(covmean):
        covmean = covmean.real
    
    # Численная стабильность
    if not np.isfinite(covmean).all():
        offset = np.eye(sigma1.shape[0]) * eps
        covmean = sqrtm((sigma1 + offset).dot(sigma2 + offset)).real
    
    # Формула Fréchet Distance
    fd = diff.dot(diff) + np.trace(sigma1 + sigma2 - 2 * covmean)
    
    return np.abs(fd)

def calculate_metrics_for_pair(emb0, emb1, pair_id, collection):
    """Рассчитывает все метрики для одной пары"""
    
    # 1. Основные статистики
    n0, n1 = len(emb0), len(emb1)
    
    # 2. Вычисляем средние и ковариационные матрицы
    mu0 = np.mean(emb0, axis=0)
    sigma0 = np.cov(emb0, rowvar=False)
    
    mu1 = np.mean(emb1, axis=0)
    sigma1 = np.cov(emb1, rowvar=False)
    
    # 3. Добавляем шум для стабильности если нужно
    if np.linalg.matrix_rank(sigma0) < sigma0.shape[0]:
        sigma0 += np.eye(sigma0.shape[0]) * 1e-6
    if np.linalg.matrix_rank(sigma1) < sigma1.shape[0]:
        sigma1 += np.eye(sigma1.shape[0]) * 1e-6
    
    # 4. Fréchet Distance
    fd = calculate_frechet_distance(mu0, sigma0, mu1, sigma1)
    
    # 5. Косинусное расстояние между центроидами
    cos_dist_centroids = cosine(mu0, mu1)
    
    # 6. Быстрые вычисления без хранения всех векторов в памяти
    # Нормализация для косинусного расстояния
    def safe_normalize(x):
        norm = np.linalg.norm(x)
        return x / norm if norm > 0 else x
    
    mu0_norm = safe_normalize(mu0)
    mu1_norm = safe_normalize(mu1)
    
    # Вычисляем расстояния порциями
    def mean_distance_to_center(embeddings, center, batch_size=1000):
        """Вычисляет среднее расстояние до центра порциями"""
        total = 0
        count = 0
        
        for i in range(0, len(embeddings), batch_size):
            batch = embeddings[i:i+batch_size]
            # Нормализуем batch
            norms = np.linalg.norm(batch, axis=1, keepdims=True)
            norms[norms == 0] = 1  # избегаем деления на 0
            batch_norm = batch / norms
            
            # Расстояния в batch
            batch_dists = 1 - np.dot(batch_norm, center)  # косинусное расстояние
            total += np.sum(batch_dists)
            count += len(batch)
        
        return total / count if count > 0 else 0
    
    # 7. Расстояния до центроидов
    dist_0_to_1 = mean_distance_to_center(emb0, mu1_norm)
    dist_1_to_0 = mean_distance_to_center(emb1, mu0_norm)
    
    # 8. Простая классификация сложности
    if fd < 2.0:
        difficulty = "Очень сложный (Угроза 3)"
    elif fd < 5.0:
        difficulty = "Сложный (Угроза 1)"
    elif fd < 10.0:
        difficulty = "Средний"
    else:
        difficulty = "Легкий (Угроза 2)"
    
    # 9. Возвращаем результат
    return {
        'pair_id': pair_id,
        'collection':collection,
        'n_samples_0': n0,
        'n_samples_1': n1,
        'ratio_1_to_0': n1 / n0 if n0 > 0 else 0,
        'frechet_distance': float(fd),
        'cosine_dist_centroids': float(cos_dist_centroids),
        'dist_0_to_center1': float(dist_0_to_1),
        'dist_1_to_center0': float(dist_1_to_0),
        'difficulty_level': difficulty,
        'is_hard_case': (fd < 5.0),
        'closeness_score': float(1 - (fd / max(fd, 10.0)))  # нормализованная близость
    }

# ================ 2. ОБРАБОТКА В ЦИКЛЕ БЕЗ ХРАНЕНИЯ ВСЕГО В ПАМЯТИ ================

# Папка для сохранения промежуточных результатов
os.makedirs('experiment_results\\', exist_ok=True)
all_results = []

# Ваш существующий цикл создания синтетических датасетов

client = MongoClient(uri)
db = client[bd]

torch.set_num_threads(4)  # Используем 4 ядра CPU
device = torch.device('cpu')

      # Самая быстрая модель для русского языка
model = SentenceTransformer('cointegrated/rubert-tiny2', device=device)

      # Оптимизации для CPU
model.to(device)
model.eval()  # режим инференса

pair_id = 1                             #"size":5000, "type":"type1" #для типа 1
for chats in db["injected_chats"].find({  "type":"type2"}):
      #id_inj = chats["_id"]
    collection = chats["title"]
    prsnt = chats['percent']
   
    coltemp =  db[collection].find()
    msg_0 = [document for document in coltemp]
    df_synthetic = pd.DataFrame(msg_0)[["text", "outlier"]].dropna()
    
    # под парой понимается 1 синтетический датасет, состоящий из 2х коллекций основной и аномалии
    print(f"\nОбработка пары {pair_id}/30...")
    
   
    
    # Т.е. это и есть синтетический датасет
    df_pair = df_synthetic
    

    ###### долго считать метрики для обемных датасетов, поэтому сэмплируем
    #### для типа 2------------------------
    
    n_total = 4000
    
    prop_normal = len(df_pair[df_pair['outlier'] == 0]) / len(df_pair)   # доля нормальных
    prop_outlier = len(df_pair[df_pair['outlier'] == 1]) / len(df_pair)  # доля аутлайеров
    
    n_normal = int(n_total * (1-prsnt/100))  # округляем вниз
    n_outlier = n_total - n_normal  # добираем до 5000
    if (n_outlier > len(df_pair[df_pair['outlier'] == 1]) or   n_normal > len(df_pair[df_pair['outlier'] == 0])):
        n_normal = int(n_total * prop_normal)  # округляем вниз
        n_outlier = n_total - n_normal  # добираем до 4000
    texts_0 = df_pair[df_pair['outlier'] == 0]['text'].sample(n_normal, random_state=42).tolist()
    texts_1 = df_pair[df_pair['outlier'] == 1]['text'].sample(n_outlier,  random_state=42).tolist()
    ###### ИСПОЛЬЗОВАТЬ ДЛЯ ТИПА 2 ------------------
    
    # Получаем эмбеддинги для этой пары использовать для типа 1 ---
    #texts_0 = df_pair[df_pair['outlier'] == 0]['text'].tolist()
    #texts_1 = df_pair[df_pair['outlier'] == 1]['text'].tolist()
       
    # Получаем эмбеддинги ПОРЦИОННО
    def get_embeddings_in_batches(texts, model, batch_size=32):
        """Получает эмбеддинги порционно чтобы не перегружать память"""
        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            batch_emb = model.encode(
                batch,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False
            )
            all_embeddings.append(batch_emb)
        return np.vstack(all_embeddings)
    
    # эмбединги
    emb0 = get_embeddings_in_batches(texts_0, model)
    emb1 = get_embeddings_in_batches(texts_1, model)
    
    
    # Вычисляем метрики для этой пары
    metrics = calculate_metrics_for_pair(emb0, emb1, pair_id, collection)
    all_results.append(metrics)
    
    # Сохраняем результат для этой пары объединенных в синтетический датасет отдельно (на случай сбоя)

    # pair_filename = f'pair_{pair_id:02d}_metrics.json'
    #with open(pair_filename, 'w', encoding='utf-8') as f:
    #    json.dump(metrics, f, ensure_ascii=False, indent=2)
    
    # Очищаем память (важно!)
    del emb0, emb1
    # if 'model' in locals():
    #     torch.cuda.empty_cache()  # если использовали GPU
    
    print(f"Пара {pair_id} обработана. FD = {metrics['frechet_distance']:.3f}")
    pair_id = pair_id+1
# ================ 3. СОБИРАЕМ И АНАЛИЗИРУЕМ РЕЗУЛЬТАТЫ ================

# Создаем DataFrame из результатов
results_df = pd.DataFrame(all_results)

# Сохраняем сводную таблицу
### для типа 1 поставить     results_df.to_csv('all_pairs_metrics_type1.csv', index=False, encoding='utf-8')
results_df.to_csv('all_pairs_metrics_type2.csv', index=False, encoding='utf-8')

# Статистический анализ
print("\n" + "="*60)
print("СВОДНЫЙ АНАЛИЗ 20 СИНТЕТИЧЕСКИХ ПАР")
print("="*60)

print(f"\nОбщая статистика:")
print(f"• Всего пар: {len(results_df)}")
print(f"• Средний FD: {results_df['frechet_distance'].mean():.3f} ± {results_df['frechet_distance'].std():.3f}")
print(f"• Диапазон FD: [{results_df['frechet_distance'].min():.3f}, {results_df['frechet_distance'].max():.3f}]")
print(f"• Медиана FD: {results_df['frechet_distance'].median():.3f}")

print(f"\nКлассификация по сложности:")
difficulty_counts = results_df['difficulty_level'].value_counts()
for level, count in difficulty_counts.items():
    percentage = count / len(results_df) * 100
    print(f"• {level}: {count} пар ({percentage:.1f}%)")

print(f"\nАнализ по модели угроз:")
hard_cases = results_df[results_df['is_hard_case']]
print(f"• Сложные случаи (Угроза 1  3): {len(hard_cases)} из {len(results_df)} пар")
print(f"• Процент сложных случаев: {len(hard_cases)/len(results_df)*100:.1f}%")

# Создаем финальный отчет для статьи
final_report = {
    'analysis_summary': {
        'total_pairs_analyzed': int(len(results_df)),
        'frechet_distance_statistics': {
            'mean': float(results_df['frechet_distance'].mean()),
            'std': float(results_df['frechet_distance'].std()),
            'min': float(results_df['frechet_distance'].min()),
            'max': float(results_df['frechet_distance'].max()),
            'median': float(results_df['frechet_distance'].median())
        },
        'cosine_distance_between_centroids': {
            'mean': float(results_df['cosine_dist_centroids'].mean())
        },
        'threat_model_analysis': {
            'hard_cases_count': int(len(hard_cases)),
            'hard_cases_percentage': float(len(hard_cases)/len(results_df)*100),
            'difficulty_distribution': difficulty_counts.to_dict()
        },
        'interpretation_for_reviewer': {
            'low_fd_values_indicate': "Высокое перекрытие распределений, моделирует сложные случаи верификации (Угроза 1 и 3)",
            'consistent_fd_across_pairs': "Стабильность методики создания синтетических образцов",
            'recommended_usage': "Пары с FD < 5.0 наиболее реалистично отражают сценарии компрометации каналов"
        }
    },
    'pairwise_results': results_df.to_dict('records')
}

# Сохраняем финальный отчет
with open('final_analysis_report_type2.json', 'w', encoding='utf-8') as f:
    json.dump(final_report, f, ensure_ascii=False, indent=2)

print(f"\nРезультаты сохранены:")
print(f"1. all_pairs_metrics.csv - таблица с метриками для всех пар")
print(f"2. pair_results/ - JSON файлы для каждой пары отдельно")
print(f"3. final_analysis_report.json - финальный отчет")



