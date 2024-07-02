import numpy as np


def op_xor_but_and_first_between(onehot_vec0, onehot_vec1):
    """
    v0 ^ v1 and (v0)
    """
    v0 = np.array(onehot_vec0).astype(np.short)
    v1 = np.array(onehot_vec1).astype(np.short)
    xor_res = (v0 ^ v1)
    op_xor_res = [int(x0 and x1) for x0, x1 in zip(xor_res.tolist(), v0.tolist())]
    return op_xor_res


def op_element_wise_and_between(onehot_vec0, onehot_vec1):
    """
    v0 & v1
    """
    v0 = np.array(onehot_vec0).astype(np.short)
    v1 = np.array(onehot_vec1).astype(np.short)
    return (v0 & v1).any().item()


def sigmoid(x):
    return 1/(1+np.exp(-x))

def norm_x(x, mean, std):
    return (x-mean)/std

def op_normalized_sigmoid_inverse_score(x, mean, std, min_cut, sqrt_val=0.333):
    if x < min_cut:
        return 0.0
    return (1-sigmoid(norm_x(x, mean, std)))**(sqrt_val)


def normalized_embeddings(embeddings):
    # cosine
    normalized_embeddings = embeddings / np.linalg.norm(embeddings, axis=1)[:, np.newaxis]
    return normalized_embeddings
