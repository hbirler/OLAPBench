from abc import ABC, abstractmethod
from typing import Union

from queryplan.queryoperator import QueryOperator


class PlanNode(ABC):

    def __init__(self, operator: QueryOperator, estimated_cardinality: int | None, exact_cardinality: int | None, system_representation: any):
        self.operator = operator
        self.estimated_cardinality = estimated_cardinality
        self.exact_cardinality = exact_cardinality
        self.system_representation = [system_representation]


class LeafNode(PlanNode):

    def __init__(self, operator: QueryOperator, estimated_cardinality: int | None, exact_cardinality: int | None, system_representation: any):
        super().__init__(operator, estimated_cardinality, exact_cardinality, system_representation)

class InnerNode(PlanNode):

    def __init__(self, operator: QueryOperator, estimated_cardinality: Union[None, int],
                 exact_cardinality: Union[None, int], children, system_representation: any):
        super().__init__(operator, estimated_cardinality, exact_cardinality, system_representation)
        self.children = children