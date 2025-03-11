import logging

from queryplan.clean.cleaner import Cleaner
from queryplan.plannode import InnerNode, PlanNode
from queryplan.queryoperator import OperatorType, Sort


class DuckCleaner(Cleaner):

    def clean(self, plan_node: PlanNode) -> PlanNode:

        if isinstance(plan_node, InnerNode):

            # Clean children recursively first
            plan_node.children = list(map(lambda child: self.clean(child), plan_node.children))

            match plan_node.operator.operator_type:
                case OperatorType.Join if len(plan_node.children) == 2:
                    logging.debug("Switch build and probe")
                    # switch probe and build for duckdb
                    plan_node.children.reverse()
                    return plan_node
                # Remove projections
                case OperatorType.Projection:
                    assert len(plan_node.children) == 1
                    # update cardinalities
                    only_child = plan_node.children[0]
                    logging.debug(f"Fold projection into {only_child.operator.operator_type.name}")
                    return self.replace_node(plan_node, only_child)
                case OperatorType.Select:
                    assert len(plan_node.children) == 1

                    only_child = plan_node.children[0]
                    match only_child.operator.operator_type:
                        case OperatorType.Join | OperatorType.TableScan:
                            logging.debug(f"Fold Select into {only_child.operator.operator_type.name}")
                            # update cardinalities
                            return self.replace_node(plan_node, only_child)
                case OperatorType.Limit | OperatorType.TopN:
                    # Just rename
                    logging.debug(f"Rename {plan_node.operator.operator_type.name} to Sort")
                    sort_operator = Sort(OperatorType.Sort, plan_node.operator.operator_id)

                    match plan_node.operator.operator_type:
                        case OperatorType.TopN:
                            limit = int(plan_node.system_representation[0]["extra_info"].split("\n")[0].split(" ")[1])
                            sort_operator.limit = limit
                        case OperatorType.Limit:
                            # can't extract anything from here yet
                            pass
                    plan_node.operator = sort_operator

        return plan_node
