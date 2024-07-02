import unittest
from pathlib import Path

from tunip.config import Config

from tunip.path_utils import (
    NautsPathFactory,
    EvalCorpusConditionalPath,
)


class PathUtilsTest(unittest.TestCase):

    def test_eval_corpus_path(self):
        
        user_name = "nauts"
        domain_name = "wiki"
        snapshot_dt = "20211110_204407_813454"
        checkpoint = "checkpoint-2700"
        target_task = "ner"
        condition_name = "default"
        target_corpus_dt = "20220119_000000_000000"
        
        domain_path = NautsPathFactory.create_training_family(
            user_name=user_name,
            domain_name=domain_name,
            snapshot_dt=snapshot_dt
        )
        assert repr(domain_path) == f"/user/{user_name}/domains/{domain_name}/{snapshot_dt}"
        
        task_path = NautsPathFactory.create_training_family(
            user_name=user_name,
            domain_name=domain_name,
            snapshot_dt=snapshot_dt,
            task_name=target_task
        )
        assert repr(task_path) == f"/user/{user_name}/domains/{domain_name}/{snapshot_dt}/model/{target_task}"
        
        eval_corpus_cond_path = EvalCorpusConditionalPath(
            user_name=user_name,
            domain_name=domain_name,
            snapshot_dt=snapshot_dt,
            checkpoint=checkpoint,
            task_name=target_task,
            condition_name=condition_name,
            target_corpus_dt=target_corpus_dt
        )
        assert repr(eval_corpus_cond_path) == f"/user/{user_name}/domains/{domain_name}/{snapshot_dt}/eval/model/{checkpoint}/{target_task}/{condition_name}/{target_corpus_dt}"
        