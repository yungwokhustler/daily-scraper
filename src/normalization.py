import re

from cleantext import clean

_LOW_VALUE_PHRASES = {
    "selamat pagi", "selamat siang", "selamat sore", "selamat malam",
    "gm", "gn", "gmn", "hai", "halo", "hello", "hi", "ok", "oke", "yes", "no",
    "good morning", "good night", "good evening", "pagi", "siang", "sore", "malam",
    "hehe", "haha", "wkwk", "hmm", "hm", "ya", "iya", "enggak", "nggak",
    "mantap", "sip"
}


def filter_text(text: str) -> str:
    cleaned_content = clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        lower=False,
        no_line_breaks=True,
        normalize_whitespace=True,
        no_emoji=True,
        no_urls=True,
        no_emails=False,
        no_phone_numbers=False,
        no_numbers=False,
        no_digits=False,
        no_currency_symbols=False,
        no_punct=False,
        replace_with_url="",
        lang="en",
    )

    return cleaned_content

def is_low_value_message(text: str) -> bool:
    if not text or not text.strip():
        return True

    cleaned = re.sub(r"[^\w\s]", "", text.strip().lower())
    words = cleaned.split()
    if not words:
        return True

    full_phrase = " ".join(words)

    if len(words) == 1 and words[0] in _LOW_VALUE_PHRASES:
        return True

    if 2 <= len(words) <= 3 and full_phrase in _LOW_VALUE_PHRASES:
        return True

    return False
