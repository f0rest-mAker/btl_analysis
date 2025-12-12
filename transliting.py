import re


EXCEPTIONS = {
    "idea": "идея",
    "fix": "фикс",
    "business": "бизнес",
    "boss": "босс",
    "media": "медиа",
    "shop": "шоп",
    "brand": "бренд",
    "promo": "промо",
    "office": "офис",
    "smart": "смарт",
    "marketing": "маркетинг",
    "club": "клуб",
    "manager": "менеджер",
}

ABBREVIATIONS = {
    "AI": "ай",
    "IA": "ай",
    "IQ": "ай кью",
    "IT": "айти",
    "PR": "пиар",
    "CEO": "си-и-о",
    "NGO": "эн-джи-о",
    "USA": "ю-эс-эй",
    "UK": "ю-кей",
    "EU": "и-ю",
}

CLUSTERS = [
    ("tion", "шн"),
    ("sion", "жн"),
    ("sch", "ш"),
    ("tch", "ч"),
    ("sh", "ш"),
    ("ch", "ч"),
    ("ph", "ф"),
    ("ck", "к"),
    ("qu", "кв"),
    ("ng", "нг"),
    ("ee", "и"),
    ("oo", "у"),
    ("ea", "и"),
    ("ou", "ау"),
]

LETTERS = {
    "a": "а", "b": "б", "c": "к", "d": "д",
    "e": "е", "f": "ф", "g": "г", "h": "х",
    "i": "и", "j": "дж", "k": "к", "l": "л",
    "m": "м", "n": "н", "o": "о", "p": "п",
    "q": "кью", "r": "р", "s": "с", "t": "т",
    "u": "у", "v": "в", "w": "в", "x": "кс",
    "y": "и", "z": "з"
}

def translit_th(word: str) -> str:
    result = ""
    i = 0
    while i < len(word):
        if word[i:i+2] == "th":
            if i > 0 and i < len(word)-2 and word[i-1] in "aeiou" and word[i+2] in "aeiou":
                result += "з"
            else:
                result += "с"
            i += 2
        else:
            result += word[i]
            i += 1
    return result


def translit_word(word: str) -> str:
    original = word
    w = word.lower()

    if original in ABBREVIATIONS:
        return ABBREVIATIONS[original]

    if w in EXCEPTIONS:
        return EXCEPTIONS[w]

    w = translit_th(w)

    for en, ru in CLUSTERS:
        w = w.replace(en, ru)

    result = ""
    for ch in w:
        result += LETTERS.get(ch, ch)

    if original and original[0].isupper():
        result = result.capitalize()

    return result

def smart_translit(text: str) -> str:
    parts = re.findall(r"[A-Za-z]+|[^A-Za-z]+", text)

    out = []
    for p in parts:
        if p.isalpha():
            out.append(translit_word(p))
        else:
            out.append(p)

    return "".join(out)
