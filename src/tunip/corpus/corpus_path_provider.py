import os

from abc import ABC
from pathlib import Path

from tunip.env import JSONL_SUFFIX


class CorpusPathProvider(ABC):
    pass


class CorpusPathProviderFactory:
    @staticmethod
    def create(corpus_type, input_dir, extensions=[JSONL_SUFFIX], split_path="split"):
        if corpus_type == "dir":
            cp_provider = SplitCorpusPathProvider(
                input_dir, extensions, downstream_path=split_path
            )
        elif corpus_type == "fixed":
            cp_provider = FixedCorpusPathProvider(input_dir, extensions)
        else:
            ValueError(f"Not Supported corpus_type:{corpus_type}")
        return cp_provider


class SplitCorpusPathProvider(CorpusPathProvider):
    def __init__(self, corpus_dir, extensions, downstream_path="split"):
        self.corpus_path = Path(corpus_dir)
        self.downstream_path = downstream_path
        self.extensions = extensions

    def __call__(self):
        print((self.corpus_path / self.downstream_path).absolute())
        for root, dirs, files in os.walk(
            (self.corpus_path / self.downstream_path).absolute()
        ):
            for name in files:
                filepath = Path(os.path.join(root, name))
                print(filepath)
                if filepath.suffix not in self.extensions:
                    continue
                yield filepath


class FixedCorpusPathProvider(CorpusPathProvider):
    def __init__(self, corpus_dir):
        self.corpus_path = Path(corpus_dir)
        self.corpus_filenames = [
            "train_corpus.jsonl",
            "dev_corpus.jsonl",
            "test_corpus.jsonl",
        ]

    def __call__(self):
        for cp_filename in self.corpus_filenames:
            filepath = self.corpus_path / cp_filename
            yield filepath
