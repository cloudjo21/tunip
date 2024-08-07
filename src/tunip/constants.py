#from datasets import load_metric

from tunip.env import NAUTS_HOME


# TOKEN_CLASSIFICATION_METRIC = load_metric(f"{NAUTS_HOME}/ner/metrics/iob_seq_scores.py")
# SEQUENCE_CLASSIFICATION_METRIC = load_metric(f"{NAUTS_HOME}/ner/metrics/label_only_scores.py")
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

SPECIAL_TOKENS_FOR_NSP = [CLS, SEP, SEP]


DATASET_FAMILIES = ["train", "dev", "test"]


GROUP_SEPARATOR = u"\u241D"
RECORD_SEPARATOR = u"\u241E"
UNIT_SEPARATOR = u"\u241F"
SPACE = u"\u2420"
PRETRAIN_SENT_SEPARATOR = u'\u2028'

SAFE_SYMBOLS_FOR_HTTP = "/.+=:&?"

# elasticsearch product type
ELASTICSEARCH_ORIGIN = 'elasticsearch'
ELASTICSEARCH_AWS = 'opensearch'

INDEX_ALIAS_SNAPSHOT_PATTERN = "^{alias}-\d{{8,}}_\d{{6}}_\d{{6}}$"

TIME_ZONE = "Asia/Seoul"

DATA_POOL_PERIOD_INTERVAL = "4m"
