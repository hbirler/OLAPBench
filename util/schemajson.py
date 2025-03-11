import os
import pathlib
import re

import jsonschema
import yaml
import simplejson as json


def parse_yaml(path: str, env: dict = None) -> dict:
    """
    Load a yaml configuration file and resolve any environment variables
    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.
    E.g.:
    database:
        host: !ENV ${HOST}
        port: !ENV ${PORT}
    app:
        log_path: !ENV '/var/${LOG_PATH}'
        something_else: !ENV '${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}'
    :param str path: the path to the yaml file
    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for
    :return: the dict configuration
    :rtype: dict[str, T]
    """
    # pattern for global vars: look for ${word}
    pattern = re.compile(r'.*?\${(\w+)}.*?')
    tag = "!ENV"
    loader = yaml.SafeLoader

    if env is None:
        env = dict(os.environ)

    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(f"${{{g}}}", env[g] if g in env.keys() else f"${{{g}}}")
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    with open(path) as file:
        return yaml.load(file, Loader=loader)


def parse_json(path: str):
    with open(path) as file:
        return json.load(file, allow_nan=True)


def load(path: str, schema: str) -> dict:
    schema = parse_json(os.path.join(pathlib.Path(__file__).parent.resolve(), "..", "schemas", schema))
    instance = parse_yaml(path=path) if path.endswith(".yaml") else parse_json(path=path)

    jsonschema.validate(instance=instance, schema=schema)
    return instance
