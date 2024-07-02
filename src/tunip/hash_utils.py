HASH_SEED = 1159241


def hash_func(key, hash_seed=HASH_SEED):
    """
    To avoid using randomized built-int hash() function in python,
    we define customized hash function
    :param key:
    :param hash_seed:
    :return:
    """
    h = hash_seed

    for c in key:
        h = h ^ ((h << 5) + ord(c) + (h >> 2))

    return h & 0x7fffffff
