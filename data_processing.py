# -----------------------------------------------------
# Файл содержит функции для обработки собранных данных.
# -----------------------------------------------------

import pandas as pd


def process_companies(file_path):
    """
        Функция для загрузки первоначальных данных о компаниях из файла.
    """
    companies = pd.read_csv(file_path)
    print("[#] Количество компаний до обработки:", len(companies))
    companies = companies.dropna(subset=['revenue'])
    companies['revenue'] = companies['revenue'].astype(int)
    companies = companies[companies['revenue'] >= 200_000_000]
    int_fills = {'revenue': 0, 'employees': 0}
    companies.fillna(int_fills, inplace=True)
    str_fills = {'okved_main': 'Неизвестно', 'site': 'Неизвестно', 'phone': 'Неизвестно', 'email': 'Неизвестно'}
    companies.fillna(str_fills, inplace=True)
    print("[#] Количество компаний после обработки и фильтрации:", len(companies))
    companies.to_csv('data/processed/companies.csv', index=False)


def main_preprocessing():
    print("Начинаем обработку данных о компаниях")
    process_companies('data/raw/output_companies.csv')
    print("Завершаем обработку данных о компаниях")


if __name__ == '__main__':
    main_preprocessing()
