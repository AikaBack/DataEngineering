import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt


def process_dataset(file_path):
    def convert_to_builtin_type(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        raise TypeError(f"Object of {type(obj)} is not JSON serializable")

    # Шаг 1: Загрузка набора данных из файла
    df = pd.read_csv(file_path, low_memory=False)

    # Шаг 2: Анализ объема памяти и типов данных
    file_memory_usage = df.memory_usage(deep=True).sum()
    memory_usage = df.memory_usage(deep=True).sum()
    column_memory_usage = df.memory_usage(deep=True)
    column_memory_percentage = column_memory_usage / memory_usage * 100
    column_data_types = df.dtypes

    # Создание словаря с анализом для каждой колонки
    column_stats = {
        "memory_usage": column_memory_usage.to_dict(),
        "memory_percentage": column_memory_percentage.to_dict(),
        "data_types": column_data_types.astype(str).to_dict()
    }

    # Шаг 3: Сортировка набора данных по занимаемому объему памяти
    df_sorted = df.copy()
    df_sorted = df_sorted.loc[:, df_sorted.columns[df_sorted.dtypes != "object"]]
    sorted_columns = df_sorted.memory_usage(deep=True).sort_values(ascending=False).index

    # Сохранение статистики в JSON
    stats_json_before_optimization = {
        "file_memory_usage": file_memory_usage,
        "memory_usage": memory_usage,
        "column_stats": column_stats,
        "sorted_columns": sorted_columns.tolist()
    }

    with open("task6_5_stats.json", "w") as json_file:
        json.dump(stats_json_before_optimization, json_file, default=convert_to_builtin_type)

    # Шаг 4: Преобразование всех колонок с типом данных "object" в категориальные
    object_columns = df.select_dtypes(include=["object"]).columns
    for col in object_columns:
        if df[col].nunique() < 0.5 * len(df[col]):
            df[col] = df[col].astype("category")

    # Шаг 5: Понижающее преобразование типов "int" колонок
    int_columns = df.select_dtypes(include=["int"]).columns
    df[int_columns] = df[int_columns].apply(pd.to_numeric, downcast="integer")

    # Шаг 6: Понижающее преобразование типов "float" колонок
    float_columns = df.select_dtypes(include=["float"]).columns
    df[float_columns] = df[float_columns].apply(pd.to_numeric, downcast="float")

    # Шаг 7: Повторный анализ оптимизированного набора данных
    optimized_memory_usage = df.memory_usage(deep=True).sum()
    optimized_column_memory_usage = df.memory_usage(deep=True)
    optimized_column_memory_percentage = optimized_column_memory_usage / optimized_memory_usage * 100

    stats_optimized_json = {
        "optimized_memory_usage": optimized_memory_usage,
        "optimized_column_memory_usage": optimized_column_memory_usage.to_dict(),
        "optimized_column_memory_percentage": optimized_column_memory_percentage.to_dict()
    }

    with open("task6_5_stats_optimized.json", "w") as json_file:
        json.dump(stats_optimized_json, json_file, default=convert_to_builtin_type)

    # Шаг 8: Выбор произвольных 10 колонок с преобразованием и сохранение поднабора в отдельном файле
    selected_columns = df.columns[:10]
    selected_df_list = []

    df_chunks = pd.read_csv(file_path, usecols=selected_columns, chunksize=1000)
    for chunk in df_chunks:
        selected_df_list.append(chunk)

    selected_df = pd.concat(selected_df_list)
    selected_df.to_csv("task6_5_selected_data.csv", index=False)

    # Шаг 9: Построение графиков
    plt.figure()
    df['diameter'].hist(bins=20, color='skyblue', edgecolor='black')
    plt.title("Распределение диаметров")
    plt.xlabel('Диаметр')
    plt.ylabel('Количество')
    plt.tight_layout()
    plt.savefig('task6_5_plot_1_diameter_distribution.png')

    plt.figure()
    df['albedo'].plot(kind='box', vert=False, color='lightcoral')
    plt.title("Распределение альбедо")
    plt.xlabel('Альбедо')
    plt.tight_layout()
    plt.savefig('task6_5_plot_2_albedo_distribution.png')

    plt.figure()
    df['H'].plot(kind='kde', color='orange')
    plt.title("Kernel Density Estimation для параметра H")
    plt.xlabel('H')
    plt.ylabel('Плотность')
    plt.tight_layout()
    plt.savefig('task6_5_plot_3_kde_H.png')

    plt.figure()
    df.plot.scatter(x='diameter', y='albedo', alpha=0.5, color='green', edgecolor='black')
    plt.title("График зависимости альбедо от диаметра")
    plt.xlabel('Диаметр')
    plt.ylabel('Альбедо')
    plt.tight_layout()
    plt.savefig('task6_5_plot_4_scatter_diameter_albedo.png')

    plt.figure()
    df['class'].value_counts().plot(kind='bar', color='purple', edgecolor='black')
    plt.title("Распределение по классам")
    plt.xlabel('Класс')
    plt.ylabel('Количество')
    plt.tight_layout()
    plt.savefig('task6_5_plot_5_class_distribution.png')


process_dataset("dataset.csv")
