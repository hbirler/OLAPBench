from abc import ABC, abstractmethod

from queryplan.plannode import PlanNode


class Cleaner(ABC):

    @abstractmethod
    def clean(self, plan_node: PlanNode) -> PlanNode:
        """Return the new root"""
        pass

    def replace_node(self, old: PlanNode, new: PlanNode) -> PlanNode:
        """Copy system representation to retain information"""
        new.system_representation = old.system_representation + new.system_representation
        new.exact_cardinality = old.exact_cardinality
        new.estimated_cardinality = old.estimated_cardinality
        return new
