from abc import ABC, abstractmethod

from queryplan.queryplan import QueryPlan


class PlanParser(ABC):

    def __init__(self):
        self.shared_pipelines = {}
        self.temp_scan_nodes = []

    @abstractmethod
    def parse_json_plan(self, query: str, json_plan: dict) -> QueryPlan:
        pass

    @abstractmethod
    def create_empty_operator(self, operator_name: str, operator_id: int):
        pass

    @abstractmethod
    def is_leaf_operator(self, json_plan) -> bool:
        pass
