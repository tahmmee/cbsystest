import math

from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesViewsTests(QueryTests):
    def setUp(self):
        super(QueriesViewsTests, self).setUp()

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            view_name = "my_index"
            self.query = "CREATE INDEX %s ON %s(name) " % (
                                    view_name, bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self._verify_view_is_present(view_name)
            self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_primary_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self._verify_view_is_present("#primary")
            self.assertTrue(self._is_index_in_list(bucket, "#primary"), "Index is not in list")

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            view_name = "my_index"
            self.query = "CREATE INDEX %s ON %s(name) " % (view_name, bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.test_case()
            self.query = "DROP INDEX %s.%s" % (bucket.name,view_name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.test_case()

    def test_explain(self):
        for bucket in self.buckets:
            try:
                self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
                self.run_cbq_query()
            except CBQError as ex:
                if str(ex).find("Primary index already exists") == -1:
                    raise ex
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.assertTrue(res["resultset"][0]["input"]["type"] == "fetch",
                            "Type should be fetch, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["type"] == "scan",
                            "Type should be scan, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["index"] == "#primary",
                            "Type should be #alldocs, but is: %s" % res["resultset"])

    def test_explain_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = "EXPLAIN SELECT * FROM %s WHERE name = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["type"] == "filter",
                                "Type should be fetch, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["type"] == "scan",
                                "Type should be scan, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_non_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["type"] == "filter",
                                "Type should be fetch, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["type"] == "scan",
                                "Type should be scan, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def _verify_view_is_present(self, view_name):
        ddoc, _ = RestConnection(self.master).get_ddoc("default", "ddl_%s" % view_name)
        self.assertTrue(view_name in ddoc["views"], "View %s wasn't created" % view_name)

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM :system.indexes"
        res = self.run_cbq_query(query)
        for item in res['resultset']:
            if item['bucket_id'] == bucket.name and item['name'] == index_name:
                return True
        return False