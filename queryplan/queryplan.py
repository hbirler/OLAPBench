import decimal
import json
from dataclasses import dataclass

from queryplan.encoder.jsonencoder import QueryPlanJsonEncoder
from queryplan.encoder.serdeskeys import *
from queryplan.encoder.xmlencoder import QueryPlanXmlEncoder
from queryplan.plannode import PlanNode


@dataclass(kw_only=True)
class QueryPlan:
    """
    A query plan including the query text and the actual plan
    """

    text: str
    plan: PlanNode


def encode_query_plan(query_plan: QueryPlan, format: str = "json") -> str:
    match format:
        case "json":
            query_plan_encoder = QueryPlanJsonEncoder()
        case "xml":
            query_plan_encoder = QueryPlanXmlEncoder()
        case other:
            raise NotImplementedError()

    # split the query plan text into lines and remove leading and trailing whitespaces
    text = " ".join([line.strip() for line in query_plan.text.split("\n")])
    plan = query_plan_encoder.encode_plan_node(query_plan.plan)

    json_dict = {
        QUERY_TEXT_KEY: text,
        QUERY_PLAN_KEY: plan
    }
    # major hack to convert NaNs to strings
    return json.dumps(json_dict, cls=DecimalEncoder)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal) or isinstance(o, datetime.date) or math.isnan(o):
            return str(o)
        return super(DecimalEncoder, self).default(o)
