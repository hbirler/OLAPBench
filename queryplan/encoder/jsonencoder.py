import json

from queryplan.encoder.plannodeencoder import PlanNodeEncoder
from queryplan.encoder.serdeskeys import *
from queryplan.plannode import InnerNode, PlanNode


class QueryPlanJsonEncoder(PlanNodeEncoder):

    def encode_plan_node(self, plan_node: PlanNode) -> dict:
        return self.transform_plan_node(plan_node)

    def transform_plan_node(self, plan_node: PlanNode) -> dict:
        operator = plan_node.operator

        attrs = {}
        children = []
        json_dict = {JX_LABEL_KEY: operator.operator_type.name, JX_ATTRS_KEY: attrs, JX_CHILDREN_KEY: children}

        if operator.operator_id:
            attrs[OPERATOR_ID_KEY] = operator.operator_id

        # more operator OR plan node attributes?
        for attr, val in dict(operator.__dict__, **plan_node.__dict__).items():
            if attr not in EXCLUDE_ATTRS:
                # This would end up as "None", which is very misleading
                if isinstance(val, list) or isinstance(val, dict):
                    attrs[attr] = json.dumps(val)
                elif val is not None:
                    # ensure all attribute values are strings
                    attrs[attr] = val

        if isinstance(plan_node, InnerNode):
            for child in plan_node.children:
                children.append(self.transform_plan_node(child))

        return json_dict
