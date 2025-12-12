import re


def safe_text(element):
    """
        Функция для безопасного получения текста из элемента html.
    """
    return element.text.strip() if element else None


def safe_find(parent, *args, **kwargs):
    """
        Функция для нахождения без исключений заданного тега из html документа,
        соответствующий заданным аргументам.
    """
    if parent is None:
        return None
    return parent.find(*args, **kwargs)


def safe_find_all(parent, *args, **kwargs):
    """
        Функция для нахождения без исключений всех тегов из html документа,
        соответствующих заданным аргументам.
    """
    if parent is None:
        return []
    return parent.find_all(*args, **kwargs)


def parse_number(lst):
    """
        Функция для нахождения лексической формы числа и его преобразования в числовую форрму.
    """
    for component in lst:
        text = component.lower().replace(".", "").replace("руб", "").strip()
        text = text.replace("человек", "").replace("человека", "").replace("человеков", "").replace("людей", "").strip()
        searching_result = re.search(r"(\d[\d\s]*(?:,\d+)?(?:\.\d+)?)\s*(млрд|млн|тыс)?", text)
        if not searching_result:
            continue

        num_str = num_result if (num_result := searching_result.group(1).replace(" ", "").replace(",", ".")) else ""
        suffix = suffix_result if (suffix_result := searching_result.group(2)) else ""
        if not num_str:
            continue
        number = float(num_str)

        if suffix == "тыс":
            number *= 1_000
        elif suffix == "млн":
            number *= 1_000_000
        elif suffix == "млрд":
            number *= 1_000_000_000
        if not suffix:
            if num_str.replace('.', ',') == text:
                return number
        elif ' '.join([num_str.replace('.', ','), suffix]) == text:
            return number

    return None
