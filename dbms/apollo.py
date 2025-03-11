from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS
from dbms.sqlserver import SQLServer, SQLServerDescription
from util import sql


class Apollo(SQLServer):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return 'apollo'

    def _create_table_statements(self, schema: dict) -> [str]:
        return sql.create_table_statements_apollo(schema)


class ApolloDescription(SQLServerDescription):
    @staticmethod
    def get_name() -> str:
        return 'apollo'

    @staticmethod
    def get_description() -> str:
        return 'Apollo'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return Apollo(benchmark, db_dir, data_dir, params, settings)
