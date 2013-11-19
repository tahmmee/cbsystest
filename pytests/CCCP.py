import json
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from couchbase.document import View


class CCCP(BaseTestCase):

    def setUp(self):
        super(CCCP, self).setUp()
        self.map_fn = 'function (doc){emit([doc.join_yr, doc.join_mo],doc.name);}'
        self.ddoc_name = "cccp_ddoc"
        self.view_name = "cccp_view"
        self.default_view = View(self.view_name, self.map_fn, None, False)
        self.ops = self.input.param("ops", None)
        self.clients = {}
        try:
            for bucket in self.buckets:
                self.clients[bucket.name] =\
                  MemcachedClientHelper.direct_client(self.master, bucket.name)
        except:
            self.tearDown()

    def tearDown(self):
        super(CCCP, self).tearDown()

    def test_get_config_client(self):
        tasks = self.run_ops()
        for bucket in self.buckets:
            _, _, config = self.clients[bucket.name].get_config()
            self.verify_config(json.loads(config), bucket)
        for task in tasks:
            task.result()

    def test_get_config_rest(self):
        tasks = self.run_ops()
        for bucket in self.buckets:
            config = RestConnection(self.master).get_bucket_CCCP(bucket)
            self.verify_config(config, bucket)
        for task in tasks:
            task.result()

    def test_set_config(self):
        tasks = self.run_ops()
        config_expected = 'abcabc'
        for bucket in self.buckets:
            self.clients[bucket.name].set_config(config_expected)
            _, _, config = self.clients[bucket.name].get_config()
            self.assertEquals(config_expected, config, "Expected config: %s, actual %s" %(
                                                      config_expected, config))
            self.log.info("Config was set correctly. Bucket %s" % bucket.name)
        for task in tasks:
            task.result()

    def verify_config(self, config_json, bucket):
        expected_params = ["nodeLocator", "rev", "uuid", "bucketCapabilitiesVer",
                           "bucketCapabilities"]
        for param in expected_params:
            self.assertTrue(param in config_json, "No %s in config" % param)
        self.assertTrue("name" in config_json and config_json["name"] == bucket.name,
                        "No bucket name in config")
        self.assertTrue(len(config_json["nodes"]) == self.nodes_init,
                        "Number of nodes expected %s, actual %s" % (
                                        self.nodes_init, len(config_json["nodes"])))
        for node in config_json["nodes"]:
            self.assertTrue("couchApiBase" in node and "hostname" in node,
                            "No hostname name in config")
            self.assertTrue(node["ports"]["proxy"] == 11211 and node["ports"]["direct"] == 11210,
                            "ports are incorrect: %s" % node)
        self.assertTrue(config_json["ddocs"]["uri"] == ("/pools/default/buckets/%s/ddocs" % bucket.name),
                        "Ddocs uri is incorrect: %s " % "/pools/default/buckets/default/ddocs")
        self.assertTrue(config_json["vBucketServerMap"]["numReplicas"] == self.num_replicas,
                        "Num replicas is incorrect: %s " % config_json["vBucketServerMap"]["numReplicas"])
        for param in ["hashAlgorithm", "serverList", "vBucketMap"]:
            self.assertTrue(param in config_json["vBucketServerMap"],
                            "%s in vBucketServerMap" % param)
        self.log.info("Bucket %s .Config was checked" % bucket.name)

    def run_ops(self):
        tasks = []
        if not self.ops:
            return tasks
        if self.ops == 'rebalance_in':
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                self.servers[self.nodes_init:self.nodes_in], []))
        elif self.ops == 'rebalance_out':
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                    [], self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
        elif self.ops == 'failover':
            tasks.append(self.cluster.failover(self.servers[:self.nodes_init],
                    [], self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
        if self.ops == 'create_views':
            views_num = 10
            views = self.make_default_views(self.view_name, views_num, different_map=True)
            tasks.extend(self.async_create_views(self.master, self.ddoc_name, views))
        return tasks
        