# ------------------------------------------------------------------
# Файл содержит функции для сбора данных о российских BTL агенствах.
# ------------------------------------------------------------------

import requests
import pickle
import re
import csv
import time
import urllib3
import os
from bs4 import BeautifulSoup
from utils import safe_text, safe_find, safe_find_all, parse_number


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.makedirs('data/processed', exist_ok=True)
os.makedirs('data/raw', exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}


def fetch_btl_agents_list():
    """
        Получаем список российских BTL агенств, входящих в рейтинг РРАР 2025 (название/Телефон).
        Источник: http://www.all20.ru/
    """
    agents = []
    search_result = []
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

            article_name = safe_text(link)
            href = link.get('href')
            if article_name and href:
                agents.append((article_name, href))

    # Парсинг телефонов
    phone_pattern = re.compile(
        r'(\+7|7|8)[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}'
    )
    i = 1
    for agent in agents:
        if i % 20 == 0:
            print(f"Было спарсено {i} агентов")
            time.sleep(5)
        article_name, href = agent
        name = None
        url = f'http://www.all20.ru/btl/{href}'
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Берем город, где расположено агентство
        city = soup.find_all('div', class_='moreinfo')[0].text.strip()
        # Пытаемся отыскать название агентсва
        cominfo = soup.find('div', class_='cominfo')
        if cominfo:
            em_tags = cominfo.find_all('em')
            if em_tags:
                probably_name = em_tags[0]
                prev_text = ''
                if probably_name.previous_sibling:
                    prev_text = probably_name.previous_sibling.text.strip()
                next_text = ''
                if probably_name.next_sibling:
                    next_text = probably_name.next_sibling.text.strip()
                if prev_text.endswith("«") and next_text.startswith("»") or \
                    ('«' in probably_name.text and '»' in probably_name.text):
                    name = probably_name.text.strip()
                    if '«' in name and '»' in name:
                        left = name.find('«')
                        right = name.find('»')
                        name = name[left+1:right]

        # Пытаемся отыскать телефон агенства
        contacts_block = soup.find('div', id='box')
        phones = []
        if contacts_block:
            for info in contacts_block.stripped_strings:
                phone = phone_pattern.search(info)
                if phone:
                    phones.append(phone.group())
        search_result.append([article_name, name, phones, city])
        i += 1
        time.sleep(1.5)

    with open('data/raw/search_result.pickle', 'wb') as f:
        pickle.dump(search_result, f)

    return search_result


def parse_inn_from_list_org(name, session, url):
    inns = []
    while True:
        response = session.get(url, headers=HEADERS, verify=False)
        if response.status_code != 200:
            print(f"Появилась капча, перейдите по ссылке {url} и нажмите 'Я не робот'")
            time.sleep(20)
            continue
        break

    soup = BeautifulSoup(response.text, 'html.parser')
    org_list = soup.find('div', class_='org_list')

    if not org_list:
        return None

    orgs = org_list.find_all('label')
    for org in orgs:
        components = [x.lower() for x in org.stripped_strings]
        company_name = components[0]

        if '"' in company_name:
            if not(name.lower() == company_name.split('"')[1]):
                continue
        else:
            if not(name.lower() in company_name):
                continue

        inn_data = ''
        for i, component in enumerate(components):
            if 'инн' in component:
                inn_data = components[i+1].replace(':', '').split('/')[0].strip()

        if inn_data:
            inns.append(inn_data)

    return inns


def fetch_btl_agents_INN_and_city(search_result):
    """
        Функция для поиска ИНН агенства через номер телефона, используя сайт https://www.list-org.com/
    """
    session = requests.Session()
    INNs = set() # Хранит пары (ИНН, город)
    for i, agents_info in enumerate(search_result):
        if (i + 1) % 10 == 0:
            print(f"Проверено {i + 1} компаний")
            time.sleep(20)

        article_name = agents_info[0]
        name = agents_info[1]
        phones = agents_info[2]
        city = agents_info[3]

        # Пробуем искать по альтернативному названию
        if name:
            url = f'https://www.list-org.com/search?val={name}&type=name&work=on&okved=58%2C70%2C73'
            name_result = parse_inn_from_list_org(name, session, url)
            if name_result:
                for inn in name_result:
                    INNs.add((inn, city))
                    time.sleep(4 + (i % 4))
                    continue

        # Пробуем искать по названию артикля
        url = f'https://www.list-org.com/search?val={article_name}&type=name&work=on&okved=58%2C70%2C73'
        name_result = parse_inn_from_list_org(article_name, session, url)
        if name_result:
            for inn in name_result:
                INNs.add((inn, city))
                time.sleep(4 + (i % 4))
                continue

        # Пробуем искать по номеру телефона
        for phone in phones:
            url = f'https://www.list-org.com/search?val={phone}&type=phone&work=on&okved=58%2C70%2C73'
            if name:
                phone_result = parse_inn_from_list_org(name, session, url)
            else:
                phone_result = parse_inn_from_list_org(article_name, session, url)
            if phone_result:
                for inn in phone_result:
                    INNs.add((inn, city))
            time.sleep(4 + (i % 4))

    with open('data/raw/INNs.pickle', 'wb') as f:
        pickle.dump(INNs, f)

    return INNs


def fetch_btl_agents_info(INNs):
    """
        Функция для поиска информации об агентстве через ИНН, используя сайт https://checko.ru/
    """
    info = []
    session = requests.Session()
    for i, (inn, city) in enumerate(INNs):
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
        # Регион
        region = city

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
            region,
            site,
            phone,
            email
        ])

        time.sleep(8 + (i % 4))

    with open('data/raw/output_companies.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source', 'okved_main', 'employees', 'region', 'site', 'phone', 'email'])
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
    print("Начинаем сбор данных")
    print("[1] Получаем список btl агентов")
    agents_list = fetch_btl_agents_list() # 1
    print("[2] Ищем ИНН компаний")
    INNs = fetch_btl_agents_INN_and_city(agents_list) # 2
    print("[3] Получаем информацию о компаниях")
    companies_info = fetch_btl_agents_info(INNs) # 3
    print("Сбор данных завершен")

if __name__ == '__main__':
    data_collection_main()
