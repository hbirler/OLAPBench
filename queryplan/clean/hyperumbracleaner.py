import logging

from queryoperators.operatortype import OperatorType
from queryplan.clean.cleaner import Cleaner
from queryplan.innernode import InnerNode
from queryplan.plannode import PlanNode


class HyperUmbraCleaner(Cleaner):

    def clean(self, plan_node: PlanNode) -> PlanNode:

        # Remove mappings
        if isinstance(plan_node, InnerNode):

            # Clean children recursively first
            plan_node.children = list(map(lambda child: self.clean(child), plan_node.children))

            match plan_node.operator.operator_type:
                case OperatorType.Map | OperatorType.EarlyExecution | OperatorType.AssertSingle:
                    assert len(plan_node.children) == 1

                    # copy cardinalities
                    only_child = plan_node.children[0]
                    logging.debug(
                        f"Fold {plan_node.operator.operator_type.name} into {only_child.operator.operator_type.name}")
                    return self.replace_node(plan_node, only_child)
                case OperatorType.Select:
                    assert len(plan_node.children) == 1

                    only_child = plan_node.children[0]
                    match only_child.operator.operator_type:
                        case OperatorType.Join | OperatorType.TableScan:
                            logging.debug(
                                f"Fold Select into {only_child.operator.operator_type.name}")
                            return self.replace_node(plan_node, only_child)
                case OperatorType.PipelineBreakerScan if len(plan_node.children) > 0:
                    assert len(plan_node.children) == 1

                    only_child = plan_node.children[0]
                    if only_child.exact_cardinality > 0 or only_child.estimated_cardinality > 0:
                        logging.debug("Reduce PipelineBreakerScan child cardinality to zero")
                        only_child.exact_cardinality, only_child.estimated_cardinality = (0, 0)
        return plan_node
