import argparse
import os
import pathlib

from benchmarks import benchmark


class MarkJoinBench(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "markjoin"

    @property
    def description(self) -> str:
        return "MarkJoin"

    @property
    def unique_name(self) -> str:
        return "markjoin"

    @property
    def data_dir(self) -> str:
        return "markjoin"

    def dbgen(self):
        self._load_with_command(os.path.join(self.path, "dbgen.sh"))


class MarkJoinDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "markjoin"

    @staticmethod
    def get_description() -> str:
        return "MarkJoin"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return MarkJoinBench(base_dir, args, included_queries, excluded_queries)
