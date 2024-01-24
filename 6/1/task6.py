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
    df = pd.read_csv(file_path, nrows=5000, low_memory=False)

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
    df_sorted = df_sorted.loc[:, df_sorted.columns[df_sorted.dtypes != "O"]]
    sorted_columns = df_sorted.memory_usage(deep=True).sort_values(ascending=False).index

    # Сохранение статистики в JSON
    stats_json_before_optimization = {
        "file_memory_usage": file_memory_usage,
        "memory_usage": memory_usage,
        "column_stats": column_stats,
        "sorted_columns": sorted_columns.tolist()
    }

    with open("../4/task6_4_stats.json", "w") as json_file:
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

    with open("../4/task6_4_stats_optimized.json", "w") as json_file:
        json.dump(stats_optimized_json, json_file, default=convert_to_builtin_type)

    # Шаг 8: Выбор произвольных 10 колонок с преобразованием и сохранение поднабора в отдельном файле
    selected_columns = df.columns[:10]
    selected_df_list = []

    df_chunks = pd.read_csv(file_path, usecols=selected_columns, chunksize=1000)
    for chunk in df_chunks:
        selected_df_list.append(chunk)

    selected_df = pd.concat(selected_df_list)
    selected_df.to_csv("task6_4_selected_data.csv", index=False)

    # Шаг 9: Построение графиков
    plt.figure()
    df['experience_name'].value_counts().plot(kind='bar', color='skyblue', edgecolor='black')
    plt.title("Распределение по опыту работы")
    plt.xlabel('Опыт работы')
    plt.ylabel('Количество вакансий')
    plt.tight_layout()
    plt.savefig('task6_4_plot_1_experience_distribution.png')

    plt.figure()
    df['schedule_name'].value_counts().plot(kind='pie', autopct='%1.1f%%', colors=['lightcoral', 'lightgreen', 'lightblue', 'lightyellow', 'lightsalmon'])
    plt.title("Распределение по графику работы")
    plt.ylabel('')  # Убираем надпись count
    plt.tight_layout()
    plt.savefig('task6_4_plot_2_schedule_distribution.png')

    plt.figure()
    df['salary_currency'].value_counts().plot(kind='bar', color='orange', edgecolor='black')
    plt.title("Распределение по валюте зарплаты")
    plt.xlabel('Валюта')
    plt.ylabel('Количество вакансий')
    plt.tight_layout()
    plt.savefig('task6_4_plot_3_salary_currency_distribution.png')

    plt.figure()
    df.plot.scatter(x='salary_from', y='salary_to', alpha=0.5, color='green', edgecolor='black')
    plt.title("График зависимости от зарплатного диапазона")
    plt.xlabel('Зарплата от')
    plt.ylabel('Зарплата до')
    plt.tight_layout()
    plt.savefig('task6_4_plot_4_salary_range.png')

    plt.figure()
    df['employment_name'].value_counts().plot(kind='barh', color='purple', edgecolor='black')
    plt.title("Распределение по типу занятости")
    plt.xlabel('Количество вакансий')
    plt.ylabel('Тип занятости')
    plt.tight_layout()
    plt.savefig('task6_4_plot_5_employment_distribution.png')


process_dataset("vacancies_2020.csv")
