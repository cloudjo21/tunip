def get_words_and_spaces(tokens):
    """
    :param tokens:   [begin, end, pos, surface] object for a sentence
    :return:         space double-array; one space array of tokens for a sentence
    """
    # for sent in sents_json['sentences']:
    prev_end = -9999999
    sent_spaces = []
    sent_tokens = []
    for token in tokens:
        begin, end, pos, surface = token
        if prev_end > 0:
            if prev_end >= begin:
                sent_spaces.append(False)
            else:  # prev_end < token['begin']:
                sent_spaces.append(True)
        prev_end = end
        sent_tokens.append(surface)
    if len(tokens) > 0:
        sent_spaces.append(False)

    return sent_tokens, sent_spaces


def filter_overlapped(tokens):
    """
    :param tokens:   [begin, end, pos, surface] object for a sentence
    :return:         updated
    """
    beg, end = -1, -1
    updated = []
    for token in tokens:
        if (end >= token[1] and end <= token[0]) or \
           (end > token[0] and end <= token[1]) or \
           (token[0] == token[1]):
            continue
        else:
            updated.append(token)
        beg, end = token[0], token[1]
    return updated
    
def strip_spaces(tokens):
    """
    :param tokens:   [begin, end, pos, surface] object for a sentence
    :return:         updated
    """
    updated = []
    for token in tokens:
        if token[3][0]==' ':
            token[3] = token[3].lstrip()
            token[0] += 1
        if token[3][-1]==' ':
            token[3] = token[3].rstrip()
            token[1] -= 1
        updated.append(token)
    return updated


def get_tokens_and_spaces(sents_json):
    """
    :param sents_json:   sentence json object
    :return:            space double-array; one space array of tokens for each sentence
    """
    sents_spaces = []
    sents_tokens = []

    for sent in sents_json['sentences']:
        prev_end = -9999999
        sent_spaces = []
        sent_tokens = []
        for token in sent['tokens']:
            if prev_end > 0:
                if prev_end >= token['begin']:
                    sent_spaces.append(False)
                else:  # prev_end < token['begin']:
                    sent_spaces.append(True)
            prev_end = token['end']
            sent_tokens.append(token['surface'])
        if len(sent['tokens']) > 0:
            sent_spaces.append(False)

        sents_tokens.append(sent_tokens)
        sents_spaces.append(sent_spaces)

    return sents_tokens, sents_spaces


def get_spaces(sents_json):
    """
    :param sents_json:   sentence json object
    :return:            space double-array; one space array of tokens for each sentence
    """
    sents_spaces = []

    for sent in sents_json['sentences']:
        prev_end = -9999999
        sent_spaces = []
        for token in sent['tokens']:
            if prev_end > 0:
                if prev_end >= token['begin']:
                    sent_spaces.append(False)
                else:  # prev_end < token['begin']:
                    sent_spaces.append(True)
            prev_end = token['end']
        if len(sent['tokens']) > 0:
            sent_spaces.append(False)
        sents_spaces.append(sent_spaces)

    return sents_spaces
