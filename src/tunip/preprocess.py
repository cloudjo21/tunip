import re
import emoji

emojis = ''.join(emoji.UNICODE_EMOJI.keys())

latin_supplement = [c for c in range(0x0080, 0x00ff)]
punctuations = [c for c in range(0x2000, 0x206f)]
math_operators = [c for c in range(0x2200, 0x22ff)]
cjk_symbols_and_puncs = [c for c in range(0x3000, 0x303f)]
misc_symbols = [c for c in range(0x2600, 0x26ff)]
half_full_sized_char = [c for c in range(0xff00, 0xffef)]

candi_char_codes = [c for block in [latin_supplement, punctuations, math_operators, cjk_symbols_and_puncs, misc_symbols, half_full_sized_char] for c in block]

prohibit_char_codes = [
    range(0x0080, 0x00a0),
    range(0x00ad, 0x00ad),
    range(0x2000, 0x200f),
    range(0x2028, 0x202f),
    range(0x205f, 0x206f),
    range(0x3000, 0x3000),
    range(0x3002, 0x3002),
    range(0x300c, 0x300d),
    range(0x302a, 0x302f),
    range(0xff00, 0xff0f),
    range(0xff1a, 0xff1e),
    range(0xff20, 0xff20),
    range(0xff3b, 0xff40),
    range(0xff5b, 0xff63),
    range(0xff9e, 0xffef),
]

valid_char_codes = [c for c in candi_char_codes if not any([c in p_codes for p_codes in prohibit_char_codes])]

pattern4korean = re.compile(f'[^ .,?!/@$%~％·∼()\x00-\x7Fㄱ-ㅣ가-힣{emojis}{valid_char_codes}]+')
url_pattern = re.compile(r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)')


def preprocess_korean(text):
    text = pattern4korean.sub(' ', text)
    text = url_pattern.sub('', text)
    text = text.strip()
    return text

# text = '∀Twitch Plays Pokémon/시즌 1/2주차'
# preprocessed = preprocess_korean(text)
# print(preprocessed)
