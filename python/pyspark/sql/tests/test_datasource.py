#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import unittest
from typing import Any, Dict, Iterator, Optional

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StructField, StructType, LongType, StringType
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql import SparkSession
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataSourceTestsMixin:
    spark: SparkSession

    def test_simple_data_source(self):
        class InMemDataSource(DataSource):

            def getSchema(self):
                return "c1 int, c2 int"

            def getReader(self) -> DataSourceReader:
                return Reader(self.options)

        class Reader(DataSourceReader):
            def getPartitions(self):
                return iter([])

            def read(self, partition=None):
                return [(1, 2), (2, 3)]

        df = self.spark.read.format(InMemDataSource).load()

        df.show()

    @unittest.skip
    def test_simple_data_source_with_partitions(self):
        class InMemoryDataSource(DataSource):

            def getSchema(self):
                return "c1 int, c2 int"

            def getReader(self) -> DataSourceReader:
                return InMemoryDataSourceReader(self.options)

        class InMemoryDataSourceReader(DataSourceReader):
            def getPartitions(self):
                return iter([1, 2])

            def read(self, partition: int):
                return [(partition, 0), (partition, 1)]

        df = self.spark.read.format(InMemoryDataSource).load()

        df.show()

    @unittest.skip
    def test_github_data_source(self):
        from pyspark.sql.types import StructType, ArrayType, IntegerType, StringType
        from typing import Any, Iterator
        import requests
        import json

        class GithubPRDataSourceReader(DataSourceReader):

            def getPartitions(self) -> Iterator[Any]:
                """Return an iterator of years for partitioning the data."""
                # TODO: support partitions
                return iter(range(2014, 2023))

            def read(self, year: int) -> Iterator[Any]:
                """Fetches the pull requests for a specific year and yields them."""
                repo = self.options["path"]
                # TODO: support partitions
                year = 2023
                url = f"https://api.github.com/search/issues?q=repo:{repo}+type:pr+created:{year}-01-01..{year}-12-31"
                response = requests.get(url)
                prs = json.loads(response.text)
                for pr in prs.get('items', []):
                    yield (
                        pr["number"],
                        pr["state"],
                        pr["title"],
                        pr["user"]["login"],
                        pr["created_at"],
                        pr["closed_at"],
                        [label["name"] for label in pr["labels"]]
                    )

        # A data source to read Github pull request data.
        class GithubPRDataSource(DataSource):

            def getSchema(self) -> StructType:
                """Returns the schema for Github pull requests."""
                return StructType() \
                    .add("number", IntegerType()) \
                    .add("state", StringType()) \
                    .add("title", StringType()) \
                    .add("user", StringType()) \
                    .add("created_at", StringType()) \
                    .add("closed_at", StringType()) \
                    .add("labels", ArrayType(StringType()))

            def getReader(self) -> DataSourceReader:
                """Returns the Github pull requests data source reader."""
                return GithubPRDataSourceReader(self.options)

        df = self.spark.read.format(GithubPRDataSource).load("apache/spark")

        df.show()


class DataSourcesTest(DataSourceTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_datasource import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
