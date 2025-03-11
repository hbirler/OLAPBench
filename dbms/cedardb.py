import tempfile

import docker
import docker.types

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS
from dbms.dbms import DBMSDescription
from dbms.postgres import Postgres


class CedarDB(Postgres):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "cedardb"

    @property
    def docker_image(self) -> str:
        return f"gitlab.db.in.tum.de:5005/schmidt/olapbench/cedardb:{self._version}"

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        docker_params = {
            "ulimits": [docker.types.Ulimit(name="memlock", soft=2 ** 30, hard=2 ** 30)],
        }
        self._start_container({}, 5432, 54324, self.host_dir.name, "/var/lib/cedardb/data", docker_params=docker_params)
        self._connect("postgres", "postgres", "postgres", 54324)

        return self


class CedarDBDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'cedardb'

    @staticmethod
    def get_description() -> str:
        return 'CedarDB'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return CedarDB(benchmark, db_dir, data_dir, params, settings)
