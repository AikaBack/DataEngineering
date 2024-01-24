import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt


def process_flights_data(file_path):
    df = pd.read_csv(file_path, low_memory=False)

    # Шаг 2: Анализ набора данных
    file_memory_usage = df.memory_usage(deep=True).sum()
    memory_usage = df.memory_usage(deep=True).sum()
    column_memory_usage = df.memory_usage(deep=True)
    column_memory_percentage = column_memory_usage / memory_usage * 100
    column_data_types = df.dtypes

    # Шаг 3: Сортировка набора данных по занимаемому объему памяти
    df_sorted = df.copy()
    df_sorted = df_sorted.loc[:, df_sorted.columns[df_sorted.dtypes != "object"]]
    df_sorted = df_sorted.loc[:, df_sorted.columns[df_sorted.dtypes != "O"]]
    sorted_columns = df_sorted.memory_usage(deep=True).sort_values(ascending=False).index

    # Функция для преобразования объектов, несовместимых с JSON, в строки
    def convert_to_builtin_type(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        raise TypeError(f"Object of {type(obj)} is not JSON serializable")

    # Сохранение статистики в JSON (пункт 3)
    stats_json = {
        "file_memory_usage": file_memory_usage,
        "memory_usage": memory_usage,
        "column_memory_usage": column_memory_usage.to_dict(),
        "column_memory_percentage": column_memory_percentage.to_dict(),
        "column_data_types": column_data_types.astype(str).to_dict(),
        "sorted_columns": sorted_columns.tolist()
    }

    with open("task6_3_stats.json", "w") as json_file:
        json.dump(stats_json, json_file, default=convert_to_builtin_type)

    # Шаг 4: Преобразование колонок с типом данных "object" в категориальные
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

    with open("task6_3_stats_optimized.json", "w") as json_file:
        json.dump(stats_optimized_json, json_file, default=convert_to_builtin_type)

    # Шаг 8: Выбор произвольных 10 колонок с преобразованием и сохранение поднабора в отдельном файле
    selected_columns = df.columns[:10]
    selected_df_list = []

    df_chunks = pd.read_csv(file_path, usecols=selected_columns, chunksize=1000, low_memory=False)
    for chunk in df_chunks:
        selected_df_list.append(chunk)

    selected_df = pd.concat(selected_df_list)
    selected_df.to_csv("task6_3_selected_data.csv", index=False)

    # График 1: Распределение задержек вылетов (DEPARTURE_DELAY)
    plt.figure()
    df['DEPARTURE_DELAY'].hist(bins=20, color='skyblue', edgecolor='black')
    plt.title("task6_3_Plot_1: Распределение задержек вылетов")
    plt.xlabel('Задержка вылета (DEPARTURE_DELAY)')
    plt.ylabel('Количество полетов')
    plt.savefig('task6_3_plot_1_histogram_departure_delays.png')

    # График 2: Распределение задержек прибытия (ARRIVAL_DELAY)
    plt.figure()
    df['ARRIVAL_DELAY'].hist(bins=20, color='orange', edgecolor='black')
    plt.title("task6_3_Plot_2: Распределение задержек прибытия")
    plt.xlabel('Задержка прибытия (ARRIVAL_DELAY)')
    plt.ylabel('Количество полетов')
    plt.savefig('task6_3_plot_2_histogram_arrival_delays.png')

    # График 3: График корреляции между временем вылета и задержкой вылета
    plt.figure()
    plt.scatter(df['SCHEDULED_DEPARTURE'], df['DEPARTURE_DELAY'], alpha=0.5, color='green')
    plt.title("График корреляции между временем вылета и задержкой вылета")
    plt.xlabel('Запланированное время вылета (SCHEDULED_DEPARTURE)')
    plt.ylabel('Задержка вылета (DEPARTURE_DELAY)')
    plt.savefig('task6_3_plot_3_scatter_scheduled_departure_departure_delay.png')

    # График 4: График корреляции между расстоянием и временем полета
    plt.figure()
    plt.scatter(df['DISTANCE'], df['SCHEDULED_TIME'], alpha=0.5, color='red')
    plt.title("График корреляции между расстоянием и временем полета")
    plt.xlabel('Расстояние (DISTANCE)')
    plt.ylabel('Запланированное время полета (SCHEDULED_TIME)')
    plt.savefig('task6_3_plot_4_scatter_distance_scheduled_time.png')

    # График 5: Box plot для распределения задержек прибытия по авиакомпаниям (AIRLINE)
    plt.figure()
    df.boxplot(column='ARRIVAL_DELAY', by='AIRLINE', vert=False, patch_artist=True, medianprops={'color': 'black'})
    plt.title("Box plot распределения задержек прибытия по авиакомпаниям")
    plt.xlabel('Задержка прибытия (ARRIVAL_DELAY)')
    plt.ylabel('Авиакомпании (AIRLINE)')
    plt.savefig('task6_3_plot_5_box_plot_arrival_delays_by_airline.png')


process_flights_data("[3]flights.csv")
