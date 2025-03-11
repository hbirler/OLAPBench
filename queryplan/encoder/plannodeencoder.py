from abc import ABC, abstractmethod

from queryplan.plannode import PlanNode


class PlanNodeEncoder(ABC):

    @abstractmethod
    def encode_plan_node(self, plan_node: PlanNode) -> any:
        pass
