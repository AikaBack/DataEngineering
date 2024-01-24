import os
import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt


def process_partial_data(file_path):

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

    with open("task6_2_stats.json", "w") as json_file:
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

    with open("task6_2_stats_optimized.json", "w") as json_file:
        json.dump(stats_optimized_json, json_file, default=convert_to_builtin_type)

    # Шаг 8: Выбор произвольных 10 колонок с преобразованием и сохранение поднабора в отдельном файле
    selected_columns = df.columns[:10]
    selected_df_list = []

    df_chunks = pd.read_csv(file_path, chunksize=10000, low_memory=False)  # Используем чанки размером 1000 строк
    for chunk in df_chunks:
        chunk[int_columns] = chunk[int_columns].apply(pd.to_numeric, downcast="integer")

        float_columns = chunk.select_dtypes(include=["float"]).columns
        chunk[float_columns] = chunk[float_columns].apply(pd.to_numeric, downcast="float")

        # Сохранение поднабора
        selected_df_list.append(chunk[selected_columns])

    selected_df = pd.concat(selected_df_list)
    selected_df.to_csv("task6_2_selected_data.csv", index=False)

    # График 1: Гистограмма распределения цен (askPrice)
    plt.figure()
    df['askPrice'].hist(bins=20, color='skyblue', edgecolor='black')
    plt.title("task6_2_Plot_1: Гистограмма распределения цен")
    plt.xlabel('Цены (askPrice)')
    plt.ylabel('Количество автомобилей')
    plt.savefig('task6_2_plot_1_histogram_prices.png')

    # График 2: Scatter plot между годом выпуска (modelYear) и пробегом (mileage)
    plt.figure()
    df_partial = df.copy()  # Добавлено
    df_partial['lastSeen'] = pd.to_datetime(df_partial['lastSeen'])
    plt.scatter(df_partial['vf_ModelYear'], df_partial['mileage'], alpha=0.5, color='orange')
    plt.title("task6_2_Plot_2: Scatter plot между годом выпуска и пробегом")
    plt.xlabel('Год выпуска (modelYear)')
    plt.ylabel('Пробег (mileage)')
    plt.savefig('task6_2_plot_2_scatter_modelYear_mileage.png')

    # График 3: Круговая диаграмма для цветов автомобилей (color)
    plt.figure()

    # Подсчет вхождений каждого цвета
    color_counts = df['color'].value_counts()
    threshold_percentage = 2.5
    mask = color_counts / color_counts.sum() * 100 < threshold_percentage
    other_colors = color_counts.index[mask]

    # Объединение цветов, составляющих менее 2.5%, в категорию 'Other Colors'
    df['color'] = np.where(df['color'].isin(other_colors), 'Other Colors', df['color'])

    # Повторный подсчет вхождений с учетом объединенной категории
    color_counts = df['color'].value_counts()

    # Используйте уникальные значения и их количество для построения круговой диаграммы
    labels = color_counts.index
    sizes = color_counts.values

    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    plt.title("task6_2_Plot_3: Круговая диаграмма цветов автомобилей")
    plt.savefig('task6_2_plot_3_pie_chart_colors.png')

    # График 4: Линейный график для изменения цен во времени (lastSeen)
    plt.figure()
    df['lastSeen'] = pd.to_datetime(df['lastSeen'])
    price_by_time = df.groupby(df['lastSeen'].dt.date)['askPrice'].mean()
    price_by_time.plot(color='green')
    plt.title("task6_2_Plot_4: Линейный график изменения цен во времени")
    plt.xlabel('Дата последнего наблюдения (lastSeen)')
    plt.ylabel('Средние цены')
    plt.savefig('task6_2_plot_4_line_chart_prices_over_time.png')

    # График 5: Box plot для распределения цен по маркам автомобилей (brandName)
    plt.figure()
    df.boxplot(column='askPrice', by='brandName', vert=False, patch_artist=True, medianprops={'color': 'black'})
    plt.title("task6_2_Plot_5: Box plot распределения цен по маркам автомобилей")
    plt.xlabel('Цены (askPrice)')
    plt.ylabel('Марки автомобилей (brandName)')
    plt.savefig('task6_2_plot_5_box_plot_prices_by_brand.png')


process_partial_data("CIS_Automotive_Kaggle_Sample.csv")
