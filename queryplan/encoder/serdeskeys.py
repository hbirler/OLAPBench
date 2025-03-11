# python attribute names
EXCLUDE_ATTRS = {
    "operator", "operator_type", "operator_id", "children", "scan_discount_factor", "total_runtime", "compilation_time",
    "active", "index_lookup_cost"
}

# keys
OPERATOR_KEY = "operator"
OPERATOR_NAME_KEY = "name"
OPERATOR_ID_KEY = "operator_id"
INNER_NODE_TAG_VALUE = "InnerNode"
LEAF_NODE_TAG_VALUE = "LeafNode"

ESTIMATED_CARDINALITY_KEY = "estimated_cardinality"
EXACT_CARDINALITY_KEY = "exact_cardinality"

# plan metadata keys
QUERY_TEXT_KEY = "queryText"
QUERY_PLAN_KEY = "queryPlan"

# JSON keys to simulate XML
JX_LABEL_KEY = "_label"
JX_ATTRS_KEY = "_attrs"
JX_CHILDREN_KEY = "_children"
