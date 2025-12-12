# ------------------------------------------------------------------
# Файл содержит функции для сбора данных о российских BTL агенствах.
# ------------------------------------------------------------------

import requests
import pickle
import re
import csv
import time
import urllib3
from bs4 import BeautifulSoup
from utils import safe_text, safe_find, safe_find_all, parse_number


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}


def fetch_btl_agents_phones_list():
    """
        Получаем список номеров телефона российских BTL агенств, входящих в рейтинг РРАР 2025.
        Источник: http://www.all20.ru/
    """
    agents = []
    agents_phones = set()
    # Получаем общее количество страниц
    url = 'http://www.all20.ru/btl/?list=all'
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')

    if not soup:
        return set()

    pages = soup.find_all('div', class_='pager')
    if pages:
        try:
            total_pages = int(pages[-1].text.strip())
        except ValueError:
            total_pages = 1
    else:
        total_pages = 1


    for page in range(1, total_pages + 1):
        url = f'http://www.all20.ru/btl/?list=all&page={page}'
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')
        if not soup:
            continue

        agents_list = soup.find_all('div', class_='ratingname')
        for agent in agents_list:
            link = agent.find('a')
            if not link:
                continue

            name = safe_text(link)
            href = link.get('href')
            if name and href:
                agents.append((name, href))

    # Парсинг телефонов
    phone_pattern = re.compile(
        r'(\+7|7|8)[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}'
    )
    i = 1
    for agent in agents:
        if i % 20 == 0:
            print(f"Было спарсено {i} агентов")
        name, href = agent
        url = f'http://www.all20.ru/btl/{href}'
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')
        contacts_block = soup.find('div', id='box')
        if contacts_block:
            for info in contacts_block.stripped_strings:
                phone = phone_pattern.search(info)
                if phone:
                    agents_phones.add(phone.group())
        i += 1

    with open('data/raw/agents_phones.pickle', 'wb') as f:
        pickle.dump(agents_phones, f)

    return agents_phones


def fetch_btl_agents_INN(agents_phones):
    """
        Функция для поиска ИНН агенства через номер телефона, используя сайт https://spark-interfax.ru/
    """
    session = requests.Session()
    INNs = set()
    for i, phone in enumerate(agents_phones):
        if (i + 1) % 10 == 0:
            print(f"Проверено {i + 1} телефонов")
            time.sleep(20)
        url = f'https://www.list-org.com/search?val={phone}&type=phone'
        try:
            # Ключевое изменение: используем verify=False для игнорирования SSL-ошибок
            response = session.get(url, headers=HEADERS, verify=False, timeout=15)
            response.raise_for_status()  # Проверка на HTTP-ошибки (4xx, 5xx)

            soup = BeautifulSoup(response.text, 'html.parser')
            org_list = soup.find('div', class_='org_list')

            if not org_list:
                continue

            orgs = org_list.find_all('label')
            for org in orgs:
                components = list(org.stripped_strings)
                for j, component in enumerate(components):
                    if 'инн' in component.lower():
                        inn_data = components[j+1].replace(':', '').split('/')[0].strip()
                        INNs.add(inn_data)
                        break

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе для телефона {phone}: {e}")

        time.sleep(3 + (i % 4))


    with open('data/raw/INNs.pickle', 'wb') as f:
        pickle.dump(INNs, f)

    return INNs


def fetch_btl_agents_info(INNs):
    """
        Функция для поиска информации об агентстве через ИНН, используя сайт https://datanewton.ru/contragents
    """
    info = []
    session = requests.Session()
    for i, inn in enumerate(INNs):
        if (i + 1) % 10 == 0:
            time.sleep(10)
            print(f"Обрабатываю {i+1} по счёту ИНН")
        while True:
            url = f'https://checko.ru/search/?query={inn}'
            response = session.get(url, headers=HEADERS)
            if response.status_code == 200:
                break
            print("Появилась капча. Перейдите по ссылке https://checko.ru/ и нажмите кнопку 'Я не робот'")
            time.sleep(15)
        soup = BeautifulSoup(response.text, 'html.parser')
        if not(soup.find('div', class_='text-success')) or not('Действующая компания' in soup.find('div', class_='text-success').stripped_strings):
            time.sleep(6)
            continue

        # Название компании
        company_name = safe_text(soup.find('h1', id='cn'))

        # Выручка
        revenue_year = 2024
        revenue_block = safe_find(soup, 'a', string='Выручка')
        revenue_value = None
        if revenue_block:
            revenue_block = revenue_block.parent
            if revenue_block:
                revenue_value = parse_number(list(revenue_block.stripped_strings))
                if revenue_value != None:
                    revenue_value = int(revenue_value)

        # ОКВЭД
        okved_main = None
        okved_block = safe_find(soup, 'section', id='activity')
        tbody = safe_find(okved_block, 'tbody')

        rows = safe_find_all(tbody, 'tr')
        if rows:
            cols = safe_find_all(rows[0], 'td')
            if len(cols) > 1:
                okved_link = safe_find(cols[1], 'a')
                okved_main = safe_text(okved_link)

        # Сотрудники
        employees_count = None
        employees_block = soup.find('div', string='Среднесписочная численность работников')
        if employees_block:
            employees_block = employees_block.parent
            employees_count = parse_number(list(employees_block.stripped_strings))
            if employees_count != None:
                employees_count = int(employees_count)

        # Сайт
        site = None
        site_block = soup.find('strong', string='Веб-сайт')
        if site_block:
            site_block = site_block.parent
            parts = list(site_block.stripped_strings)
            if 'Веб-сайт' in parts:
                idx = parts.index('Веб-сайт')
                if idx + 1 < len(parts) and parts[idx + 1] != '—':
                    site = parts[idx + 1].strip()

        # Телефон
        phone = None
        phone_block = soup.find('strong', string='Телефоны')
        if phone_block:
            phone_block = phone_block.parent
            parts = list(phone_block.stripped_strings)
            if 'Телефоны' in parts:
                idx = parts.index('Телефоны')
                if idx + 1 < len(parts) and parts[idx + 1] != '—':
                    phone = parts[idx + 1].strip()

        # Email
        email = None
        email_block = soup.find('strong', string='Электронная почта')
        if email_block:
            email_block = email_block.parent
            parts = list(email_block.stripped_strings)
            if 'Электронная почта' in parts:
                idx = parts.index('Электронная почта')
                if idx + 1 < len(parts) and parts[idx + 1] != '—':
                    email = parts[idx + 1].strip()

        segment_tag = "BTL"
        source = "checko_custom_parser"

        info.append([
            inn,
            company_name,
            revenue_year,
            revenue_value,
            segment_tag,
            source,
            okved_main,
            employees_count,
            site,
            phone,
            email
        ])

        time.sleep(8 + (i % 4))

    with open('data/raw/output_companies.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['ИНН', 'Наименование компании', 'Год выручки', 'Выручка', 'Сегмент', 'Источник', 'Основной ОКВЭД', 'Количество сотрудников', 'Сайт', 'Телефон', 'Email'])
        writer.writerows(info)

    return info


def load_data_from_pickle(input_path):
    """
        Функция для загрузки данных из файла pickle.
    """
    with open(input_path, 'rb') as f:
        return pickle.load(f)


def data_collection_main():
    """
        Функция для сбора данных о компаниях.
        1) Список из рейтинга -> 2) Список ИНН -> 3) Информация о компании
    """
    agents_phones = fetch_btl_agents_phones_list() # 1
    INNs = fetch_btl_agents_INN(agents_phones) # 2
    companies_info = fetch_btl_agents_info(INNs) # 3

if __name__ == '__main__':
    data_collection_main()
