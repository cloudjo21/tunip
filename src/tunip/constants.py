from datasets import load_metric

from tunip.env import NAUTS_HOME


TOKEN_CLASSIFICATION_METRIC = load_metric(f"{NAUTS_HOME}/ner/metrics/iob_seq_scores.py")
SEQUENCE_CLASSIFICATION_METRIC = load_metric(f"{NAUTS_HOME}/ner/metrics/label_only_scores.py")
# TODO
# QUESTION_ANSWERING_METRIC = load_metric("")


B_PREF = "B-"
I_PREF = "I-"
S_PREF = "S-"
E_PREF = "E-"
OUTSIDE = "O"
NA_LABEL = "-"

BOS = "[BOS]"
EOS = "[EOS]"
PAD = "[PAD]"
UNK = "[UNK]"
CLS = "[CLS]"
SEP = "[SEP]"
MASK = "[MASK]"

BOS_IDX = 0
EOS_IDX = 1
PAD_IDX = 2
UNK_IDX = 3
SEP_IDX = 4
CLS_IDX = 5
MASK_IDX = 6


SPECIAL_TOKENS = [BOS, EOS, PAD, UNK]

SPECIAL_TOKENS_BERT = [BOS, EOS, PAD, UNK, SEP, CLS, MASK]


DATASET_FAMILIES = ["train", "dev", "test"]


# TODO move it to config. for the corresponding task
entity_black_list = [
    "CARDINAL",
    "DATE",
    "EVENT",
    "FAC",
    "GPE",
    "LANGUAGE",
    "LAW",
    "LOC",
    "MONEY",
    "NORP",
    "ORDINAL",
    "ORG",
    "PERCENT",
    "PERSON",
    # "PRODUCT",
    "QUANTITY",
    "TIME",
    "WORK_OF_ART",
]

GROUP_SEPARATOR = u"\u241D"
RECORD_SEPARATOR = u"\u241E"
UNIT_SEPARATOR = u"\u241F"
SPACE = u"\u2420"

SAFE_SYMBOLS_FOR_HTTP = "/.+=:&?"