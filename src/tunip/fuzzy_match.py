from fuzzywuzzy import fuzz, process
from pprint import pprint, pformat

from tunip.Hangulpy import decompose, is_hangul

# https://github.com/seatgeek/fuzzywuzzy
# https://www.datacamp.com/community/tutorials/fuzzy-string-python


def ratios_between(x, y):
    ratio_dict = dict()

    ratio_dict['ratio'] = fuzz.ratio(x, y)
    # ratio_dict['partial_ratio'] = fuzz.partial_ratio(x, y)
    ratio_dict['token_sort_ratio'] = fuzz.token_sort_ratio(x, y)
    # ratio_dict['token_set_ratio'] = fuzz.token_set_ratio(x, y)

    return ratio_dict


def thresholded_ratio_between(uttr0, uttr1, avg_thrshold=80.0, min_token_ratio=69.0):
    syls0 = []
    syls1 = []
    for ch in uttr0:
        if is_hangul(ch):
            cho, joong, jong = decompose(ch)
            syls0.extend([cho, joong, jong])
        else:
            syls0.append(ch)
    for ch in uttr1:
        if is_hangul(ch):
            cho, joong, jong = decompose(ch)
            syls1.extend([cho, joong, jong])
        else:
            syls1.append(ch)

    # ratio is symmetric metric, so we don't need to compare the opposed direction
    ratios = ratios_between(syls0, syls1)
    ratio = float(ratios['ratio'])
    token_sort_ratio = float(ratios['token_sort_ratio'])

    if (ratio + token_sort_ratio) / 2.0 > avg_thrshold and token_sort_ratio > min_token_ratio:
        # print(pformat(f'{uttr0} VS {uttr1}'))

        ratios_dict = {}
        for r in ratios:
            # print(pformat(f'{r}: {ratios[r]}'))

            ratios_dict.update({r: ratios[r]})
    
        ratio_entry = {
            'sent0': uttr0,
            'sent1': uttr1,
            'ratios': ratios_dict
        }
        return ratio_entry
    else:
        return None


def ratios_among(uttrs, avg_thrshold=80.0, min_token_ratio=69.0):
    """
    get the ratio statstics by doing the cartesian comparisons among utterances
    """

    for i, uttr0 in enumerate(uttrs):
        for _, uttr1 in enumerate(uttrs[i+1:]):
            if uttr0 == uttr1:
                continue

            r = thresholded_ratio_between(uttr0, uttr1, avg_thrshold, min_token_ratio)
            if r is not None:
                yield r
