import json
from xml.etree.ElementTree import Element, tostring

from queryplan.encoder.plannodeencoder import PlanNodeEncoder
from queryplan.encoder.serdeskeys import *
from queryplan.plannode import InnerNode, PlanNode


class QueryPlanXmlEncoder(PlanNodeEncoder):

    def encode_plan_node(self, plan_node: PlanNode) -> str:
        return tostring(self.transform_plan_node(plan_node), encoding="unicode")

    def transform_plan_node(self, plan_node: PlanNode) -> Element:
        operator = plan_node.operator

        element = Element(operator.operator_type.name)
        if operator.operator_id:
            element.attrib[OPERATOR_ID_KEY] = str(operator.operator_id)

        # more operator OR plan node attributes?
        for attr, val in dict(operator.__dict__, **plan_node.__dict__).items():
            if attr not in EXCLUDE_ATTRS:
                # This would end up as "None", which is very misleading
                if isinstance(val, list) or isinstance(val, dict):
                    element.attrib[attr] = json.dumps(val)
                elif val is not None:
                    # ensure all attribute values are strings
                    element.attrib[attr] = str(val)

        if isinstance(plan_node, InnerNode):
            for child in plan_node.children:
                element.append(self.transform_plan_node(child))

        return element
