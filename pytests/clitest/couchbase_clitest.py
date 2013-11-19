import json
import logger

from threading import Thread

from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from remote.remote_util import RemoteMachineShellConnection
from pprint import pprint

help = {'CLUSTER': '--cluster=HOST[:PORT] or -c HOST[:PORT]',
 'COMMAND': {'bucket-compact': 'compact database and index data',
             'bucket-create': 'add a new bucket to the cluster',
             'bucket-delete': 'delete an existing bucket',
             'bucket-edit': 'modify an existing bucket',
             'bucket-flush': 'flush all data from disk for a given bucket',
             'bucket-list': 'list all buckets in a cluster',
             'cluster-edit': 'modify cluster settings',
             'cluster-init': 'set the username,password and port of the cluster',
             'failover': 'failover one or more servers',
             'help': 'show longer usage/help and examples',
             'node-init': 'set node specific parameters',
             'rebalance': 'start a cluster rebalancing',
             'rebalance-status': 'show status of current cluster rebalancing',
             'rebalance-stop': 'stop current cluster rebalancing',
             'server-add': 'add one or more servers to the cluster',
             'server-info': 'show details on one server',
             'server-list': 'list all servers in a cluster',
             'server-readd': 'readd a server that was failed over',
             'setting-alert': 'set email alert settings',
             'setting-autofailover': 'set auto failover settings',
             'setting-compaction': 'set auto compaction settings',
             'setting-notification': 'set notification settings',
             'setting-xdcr': 'set xdcr related settings',
             'user-manage': 'manage read only user',
             'xdcr-replicate': 'xdcr operations',
             'xdcr-setup': 'set up XDCR connection'},
 'EXAMPLES': {'Add a node to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator --server-add-password=password',
              'Add a node to a cluster, but do not rebalance': 'couchbase-cli server-add -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator --server-add-password=password',
              'Change the data path': 'couchbase-cli node-init -c 192.168.0.1:8091 --node-init-data-path=/tmp',
              'Create a couchbase bucket and wait for bucket ready': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1 --wait',
              'Create a new dedicated port couchbase bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1',
              'Create a new sasl memcached bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=memcached --bucket-password=password --bucket-ramsize=200 --enable-flush=1 --enable-index-replica=1',
              'Delete a bucket': 'couchbase-cli bucket-delete -c 192.168.0.1:8091 --bucket=test_bucket',
              'Flush a bucket': 'couchbase-cli xdcr-replicate -c 192.168.0.1:8091 --delete --xdcr-replicator=f4eb540d74c43fd3ac6d4b7910c8c92f/default/default',
              'List buckets in a cluster': 'couchbase-cli bucket-list -c 192.168.0.1:8091',
              'List servers in a cluster': 'couchbase-cli server-list -c 192.168.0.1:8091',
              'Modify a dedicated port bucket': 'couchbase-cli bucket-edit -c 192.168.0.1:8091 --bucket=test_bucket --bucket-port=11222 --bucket-ramsize=400 --enable-flush=1 --enable-index-replica=1',
              'Remove a node from a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2:8091',
              'Remove and add nodes from/to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2 --server-add=192.168.0.4 --server-add-username=Administrator --server-add-password=password',
              'Server information': 'couchbase-cli server-info -c 192.168.0.1:8091',
              'Set data path for an unprovisioned cluster': 'couchbse-cli node-init -c 192.168.0.1:8091 --node-init-data-path=/tmp/data --node-init-index-path=/tmp/index',
              'Set the username, password, port and ram quota': 'couchbase-cli cluster-init -c 192.168.0.1:8091 --cluster-init-username=Administrator --cluster-init-password=password --cluster-init-port=8080 --cluster-init-ramsize=300',
              'Stop the current rebalancing': 'couchbase-cli rebalance-stop -c 192.168.0.1:8091',
              'change the cluster username, password, port and ram quota': 'couchbase-cli cluster-edit -c 192.168.0.1:8091 --cluster-username=Administrator --cluster-password=password --cluster-port=8080 --cluster-ramsize=300',
              'create/modify a read only user in a cluster':'couchbase-cli user-manage -c 192.168.0.1:8091 --set --ro-username=readonlyuser --ro-password=readonlypassword'},
 'OPTIONS': {'-o KIND, --output=KIND': 'KIND is json or standard\n-d, --debug',
             '-p PASSWORD, --password=PASSWORD': 'admin password of the cluster',
             '-u USERNAME, --user=USERNAME': 'admin username of the cluster'},
 'bucket-* OPTIONS': {'--bucket-password=PASSWORD': 'standard port, exclusive with bucket-port',
                      '--bucket-port=PORT': 'supports ASCII protocol and is auth-less',
                      '--bucket-ramsize=RAMSIZEMB': 'ram quota in MB',
                      '--bucket-replica=COUNT': 'replication count',
                      '--bucket-type=TYPE': 'memcached or couchbase',
                      '--bucket=BUCKETNAME': ' bucket to act on',
                      '--data-only': ' compact datbase data only',
                      '--enable-flush=[0|1]': 'enable/disable flush',
                      '--enable-index-replica=[0|1]': 'enable/disable index replicas',
                      '--force': ' force to execute command without asking for confirmation',
                      '--view-only': ' compact view data only',
                      '--wait': 'wait for bucket create to be complete before returning'},
 'cluster-* OPTIONS': {'--cluster-password=PASSWORD': ' new admin password',
                       '--cluster-port=PORT': ' new cluster REST/http port',
                       '--cluster-ramsize=RAMSIZEMB': ' per node ram quota in MB',
                       '--cluster-username=USER': ' new admin username'},
 'description': 'couchbase-cli - command-line cluster administration tool',
 'failover OPTIONS': {'--server-failover=HOST[:PORT]': ' server to failover'},
 'node-init OPTIONS': {'--node-init-data-path=PATH': 'per node path to store data',
                       '--node-init-index-path=PATH': ' per node path to store index'},
 'rebalance OPTIONS': {'--server-add*': ' see server-add OPTIONS',
                       '--server-remove=HOST[:PORT]': ' the server to be removed'},
 'server-add OPTIONS': {'--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                        '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                        '--server-add=HOST[:PORT]': 'server to be added'},
 'server-readd OPTIONS': {'--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                          '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                          '--server-add=HOST[:PORT]': 'server to be added'},
 'setting-alert OPTIONS': {'--alert-auto-failover-cluster-small': " node wasn't auto fail over as cluster was too small",
                           '--alert-auto-failover-max-reached': ' maximum number of auto failover nodes was reached',
                           '--alert-auto-failover-node': 'node was auto failover',
                           '--alert-auto-failover-node-down': " node wasn't auto failover as other nodes are down at the same time",
                           '--alert-disk-space': 'disk space used for persistent storgage has reached at least 90% capacity',
                           '--alert-ip-changed': 'node ip address has changed unexpectedly',
                           '--alert-meta-oom': 'bucket memory on a node is entirely used for metadata',
                           '--alert-meta-overhead': ' metadata overhead is more than 50%',
                           '--alert-write-failed': 'writing data to disk for a specific bucket has failed',
                           '--email-host=HOST': ' email server host',
                           '--email-password=PWD': 'email server password',
                           '--email-port=PORT': ' email server port',
                           '--email-recipients=RECIPIENT': 'email recipents, separate addresses with , or ;',
                           '--email-sender=SENDER': ' sender email address',
                           '--email-user=USER': ' email server username',
                           '--enable-email-alert=[0|1]': 'allow email alert',
                           '--enable-email-encrypt=[0|1]': 'email encrypt'},
 'setting-autofailover OPTIONS': {'--auto-failover-timeout=TIMEOUT (>=30)': 'specify timeout that expires to trigger auto failover',
                                  '--enable-auto-failover=[0|1]': 'allow auto failover'},
 'setting-compacttion OPTIONS': {'--compaction-db-percentage=PERCENTAGE': ' at which point database compaction is triggered',
                                 '--compaction-db-size=SIZE[MB]': ' at which point database compaction is triggered',
                                 '--compaction-period-from=HH:MM': ' allow compaction time period from',
                                 '--compaction-period-to=HH:MM': ' allow compaction time period to',
                                 '--compaction-view-percentage=PERCENTAGE': ' at which point view compaction is triggered',
                                 '--compaction-view-size=SIZE[MB]': ' at which point view compaction is triggered',
                                 '--enable-compaction-abort=[0|1]': ' allow compaction abort when time expires',
                                 '--enable-compaction-parallel=[0|1]': ' allow parallel compaction for database and view'},
 'setting-notification OPTIONS': {'--enable-notification=[0|1]': ' allow notification'},
 'setting-xdcr OPTIONS': {'--checkpoint-interval=[1800]': ' intervals between checkpoints, 60 to 14400 seconds.',
                          '--doc-batch-size=[2048]KB': 'document batching size, 10 to 100000 KB',
                          '--failure-restart-interval=[30]': 'interval for restarting failed xdcr, 1 to 300 seconds\n--optimistic-replication-threshold=[256] document body size threshold (bytes) to trigger optimistic replication',
                          '--max-concurrent-reps=[32]': ' maximum concurrent replications per bucket, 8 to 256.',
                          '--worker-batch-size=[500]': 'doc batch size, 500 to 10000.'},
 'usage': ' couchbase-cli COMMAND CLUSTER [OPTIONS]',
 'xdcr-replicate OPTIONS': {'--create': ' create and start a new replication',
                            '--delete': ' stop and cancel a replication',
                            '--xdcr-clucter-name=CLUSTERNAME': 'remote cluster to replicate to',
                            '--xdcr-from-bucket=BUCKET': 'local bucket name to replicate from',
                            '--xdcr-to-bucket=BUCKETNAME': 'remote bucket to replicate to'},
 'xdcr-setup OPTIONS': {'--create': ' create a new xdcr configuration',
                        '--delete': ' delete existed xdcr configuration',
                        '--edit': ' modify existed xdcr configuration',
                        '--xdcr-cluster-name=CLUSTERNAME': 'cluster name',
                        '--xdcr-hostname=HOSTNAME': ' remote host name to connect to',
                        '--xdcr-password=PASSWORD': ' remtoe cluster admin password',
                        '--xdcr-username=USERNAME': ' remote cluster admin username'},
 'user-manage OPTIONS': {'--set': ' create/set a read only user',
                        '--list': ' list any read only user',
                        '--edit': ' modify existed read only user',
                        '--delete': 'delete read only user',
                        '--ro-username=USERNAME': ' readonly user name',
                        '--ro-password=PASSWORD': ' readonly user password'}}


help_short = {'COMMANDs include': {'bucket-compact': 'compact database and index data',
                      'bucket-create': 'add a new bucket to the cluster',
                      'bucket-delete': 'delete an existing bucket',
                      'bucket-edit': 'modify an existing bucket',
                      'bucket-flush': 'flush all data from disk for a given bucket',
                      'bucket-list': 'list all buckets in a cluster',
                      'cluster-edit': 'modify cluster settings',
                      'cluster-init': 'set the username,password and port of the cluster',
                      'failover': 'failover one or more servers',
                      'help': 'show longer usage/help and examples',
                      'node-init': 'set node specific parameters',
                      'rebalance': 'start a cluster rebalancing',
                      'rebalance-status': 'show status of current cluster rebalancing',
                      'rebalance-stop': 'stop current cluster rebalancing',
                      'server-add': 'add one or more servers to the cluster',
                      'server-info': 'show details on one server',
                      'server-list': 'list all servers in a cluster',
                      'server-readd': 'readd a server that was failed over',
                      'setting-alert': 'set email alert settings',
                      'setting-autofailover': 'set auto failover settings',
                      'setting-compaction': 'set auto compaction settings',
                      'setting-notification': 'set notification settings',
                      'setting-xdcr': 'set xdcr related settings',
                      'xdcr-replicate': 'xdcr operations',
                      'xdcr-setup': 'set up XDCR connection'},
 'description': 'CLUSTER is --cluster=HOST[:PORT] or -c HOST[:PORT]',
 'usage': ' couchbase-cli COMMAND CLUSTER [OPTIONS]'}


class CouchbaseCliTest(CliBaseTest):
    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(CouchbaseCliTest, self).setUp()

    def tearDown(self):
        super(CouchbaseCliTest, self).tearDown()


    def _get_dict_from_output(self, output):
        result = {}
        if output[0].startswith("couchbase-cli"):
            result["description"] = output[0]
            result["usage"] = output[2].split("usage:")[1]
        else:
            result["usage"] = output[0].split("usage:")[1]
            result["description"] = output[2]

        upper_key = ""
        for line in output[3:]:
            line = line.strip()
            if line == "":
                if not upper_key == "EXAMPLES":
                    upper_key = ""
                continue
            # line = ""
            if line.endswith(":") and upper_key != "EXAMPLES":
                upper_key = line[:-1]
                result[upper_key] = {}
                continue
            elif line == "COMMANDs include":
                upper_key = line
                result[upper_key] = {}
                continue
            elif upper_key in ["COMMAND", "COMMANDs include"] :
                result[upper_key][line.split()[0]] = line.split(line.split()[0])[1].strip()
            elif upper_key in ["CLUSTER"]:
                result[upper_key] = line
            elif upper_key.endswith("OPTIONS"):
                # for u=instance:"   -u USERNAME, --user=USERNAME      admin username of the cluster"
                temp = line.split("  ")
                temp = [item for item in temp if item != ""]
                if len(temp) > 1:
                    result[upper_key][temp[0]] = temp[1]
                    previous_key = temp[0]
                else:
                    result[upper_key][previous_key] = result[upper_key][previous_key] + "\n" + temp[0]
            elif upper_key in ["EXAMPLES"] :
                if line.endswith(":"):
                    previous_key = line[:-1]
                    result[upper_key][previous_key] = ""
                else:
                    line = line.strip()
                    if line.startswith("couchbase-cli"):
                        result[upper_key][previous_key] = line.replace("\\", "")
                    else:
                        result[upper_key][previous_key] = (result[upper_key][previous_key] + line).replace("\\", "")
                    previous_line = result[upper_key][previous_key]
            elif line == "The default PORT number is 8091.":
                continue
            else:
                self.fail(line)
        return result

    def _get_cluster_info(self, remote_client, cluster_host="localhost", cluster_port=None, user="Administrator", password="password"):
        command = "server-info"
        output, error = remote_client.execute_couchbase_cli(command, cluster_host=cluster_host, cluster_port=cluster_port, user=user, password=password)
        if not error:
            content = ""
            for line in output:
                content += line
            return json.loads(content)
        else:
            self.fail("server-info return error output")

    def _create_bucket(self, remote_client, bucket="default", bucket_type="couchbase", bucket_port=11211, bucket_password=None, \
                        bucket_ramsize=200, bucket_replica=1, wait=False, enable_flush=None, enable_index_replica=None):
        options = "--bucket={0} --bucket-type={1} --bucket-port={2} --bucket-ramsize={3} --bucket-replica={4}".\
            format(bucket, bucket_type, bucket_port, bucket_ramsize, bucket_replica)
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica), "")[enable_index_replica is None]
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --bucket-password={0}".format(bucket_password), "")[bucket_password is None]
        options += (" --wait", "")[wait]
        cli_command = "bucket-create"

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertTrue("SUCCESS: bucket-create" in output[0])

    def _teardown_xdcr(self):
        for server in self.servers:
            rest = RestConnection(server)
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
            rest.remove_all_recoveries()

    def testHelp(self):
        remote_client = RemoteMachineShellConnection(self.master)
        options = self.input.param('options', "")
        output, error = remote_client.execute_couchbase_cli(cli_command="", cluster_host=None, user=None, password=None, options=options)
        result = self._get_dict_from_output(output)
        expected_result = help_short
        if "-h" in options:
            expected_result = help
        pprint(result)
        if result == expected_result:
            self.log.info("Correct help info was found")
        else:
            self.fail(set(result.keys()) - set(help.keys()))
        remote_client.disconnect()

    def testInfoCommands(self):
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "server-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        server_info = self._get_cluster_info(remote_client)
        result = server_info["otpNode"] + " " + server_info["hostname"] + " " + server_info["status"] + " " + server_info["clusterMembership"]
        self.assertEqual(result, "ns_1@{0} {0}:8091 healthy active".format("127.0.0.1"))

        cli_command = "bucket-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual([], output)
        remote_client.disconnect()

    def testAddRemoveNodes(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "server-add"
        for num in xrange(nodes_add):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: server-add {0}:8091".format(self.servers[num + 1].ip)])

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertTrue("INFO: rebalancing" in output[0])
            self.assertEqual(output[1], "SUCCESS: rebalanced cluster")

        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])


        cli_command = "failover"
        for num in xrange(nodes_failover):
            options = "--server-failover={0}:8091".format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: failover ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip)])

        cli_command = "server-readd"
        for num in xrange(nodes_readd):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[nodes_add - nodes_rem - num ].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num ].ip)])

        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])
        remote_client.disconnect()

    def testStartStopRebalance(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'none', None)"])

        cli_command = "server-add"
        for num in xrange(nodes_add):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: server-add {0}:8091".format(self.servers[num + 1].ip)])

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'none', None)"])

        self._create_bucket(remote_client)

        cli_command = "rebalance"
        t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", '', None, "Administrator", "password"))
        t.start()
        self.sleep(5)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'running', None)"])

        t.join()

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", options, None, "Administrator", "password"))
            t.start()
            self.sleep(5)
            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'running', None)"])


            cli_command = "rebalance-stop"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")

            t.join()

            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[1], "SUCCESS: rebalanced cluster")


            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'none', None)"])
        remote_client.disconnect()

    def testNodeInit(self):
        server = self.servers[-1]
        remote_client = RemoteMachineShellConnection(server)
        prefix = ''
        type = remote_client.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            prefix_path = "C:"

        data_path = self.input.param("data_path", None)
        index_path = self.input.param("index_path", None)

        if data_path is not None:
            data_path = prefix + data_path.replace('|', "/")
        if index_path is not None:
            index_path = prefix + index_path.replace('|', "/")

        server_info = self._get_cluster_info(remote_client, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
        data_path_before = server_info["storage"]["hdd"][0]["path"]
        index_path_before = server_info["storage"]["hdd"][0]["index_path"]

        try:
            rest = RestConnection(server)
            rest.force_eject_node()
            self.sleep(5)
            cli_command = "node-init"
            options = ""
            options += ("--node-init-data-path={0} ".format(data_path), "")[data_path is None]
            options += ("--node-init-index-path={0} ".format(index_path), "")[index_path is None]
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: init localhost")
            rest.init_cluster()
            server_info = self._get_cluster_info(remote_client, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            data_path_after = server_info["storage"]["hdd"][0]["path"]
            index_path_after = server_info["storage"]["hdd"][0]["index_path"]
            self.assertEqual((data_path, data_path_before)[data_path is None], data_path_after)
            self.assertEqual((index_path, index_path_before)[index_path is None], index_path_after)
        finally:
            rest = RestConnection(server)
            rest.force_eject_node()
            self.sleep(5)
            rest.init_cluster()

    def testClusterInit(self):
        cluster_init_username = self.input.param("cluster_init_username", "Administrator")
        cluster_init_password = self.input.param("cluster_init_password", "password")
        cluster_init_port = self.input.param("cluster_init_port", 8091)
        cluster_init_ramsize = self.input.param("cluster_init_ramsize", 300)
        command_init = self.input.param("command_init", "cluster-init")
        param_prefix = self.input.param("param_prefix", "--cluster-init")
        server = self.servers[-1]
        remote_client = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        rest.force_eject_node()
        self.sleep(5)

        try:
            cli_command = command_init
            options = "--cluster-init-username={0} {1}-password={2} {3}-port={4} {5}-ramsize={6}".\
                format(cluster_init_username, param_prefix, cluster_init_password, param_prefix, cluster_init_port, param_prefix, cluster_init_ramsize)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: init localhost")

            options = "{0}-username={1} {2}-password={3} {4}-port={5}".\
                format(param_prefix, cluster_init_username + "1", param_prefix, cluster_init_password + "1", param_prefix, str(cluster_init_port)[:-1] + "9")
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user=cluster_init_username, password=cluster_init_password)
            # MB-8202 cluster-init/edit doesn't provide status
            self.assertTrue(output == [])
            server.rest_username = cluster_init_username + "1"
            server.rest_password = cluster_init_password + "1"
            server.port = str(cluster_init_port)[:-1] + "9"


            cli_command = "server-list"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", cluster_port=str(cluster_init_port)[:-1] + "9", user=cluster_init_username + "1", password=cluster_init_password + "1")
            self.assertTrue("{0} healthy active".format(str(cluster_init_port)[:-1] + "9") in output[0])
            server_info = self._get_cluster_info(remote_client, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            result = server_info["otpNode"] + " " + server_info["hostname"] + " " + server_info["status"] + " " + server_info["clusterMembership"]
            self.assertTrue("{0} healthy active".format(str(cluster_init_port)[:-1] + "9") in result)

            cli_command = command_init
            options = "{0}-username={1} {2}-password={3} {4}-port={5}".\
                format(param_prefix, cluster_init_username, param_prefix, cluster_init_password, param_prefix, cluster_init_port)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", cluster_port=str(cluster_init_port)[:-1] + "9", user=(cluster_init_username + "1"), password=cluster_init_password + "1")
            # MB-8202 cluster-init/edit doesn't provide status
            self.assertTrue(output == [])

            server.rest_username = cluster_init_username
            server.rest_password = cluster_init_password
            server.port = cluster_init_port
            remote_client = RemoteMachineShellConnection(server)
            cli_command = "server-list"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user=cluster_init_username, password=cluster_init_password)
            self.assertTrue("{0} healthy active".format(str(cluster_init_port)) in output[0])
            server_info = self._get_cluster_info(remote_client, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            result = server_info["otpNode"] + " " + server_info["hostname"] + " " + server_info["status"] + " " + server_info["clusterMembership"]
            self.assertTrue("{0} healthy active".format(str(cluster_init_port)) in result)
            remote_client.disconnect()
        finally:
            rest = RestConnection(server)
            rest.force_eject_node()
            self.sleep(5)
            rest.init_cluster()

    def testClusterInitNegative(self):
        cluster_init_username = self.input.param("cluster_init_username", None)
        cluster_init_password = self.input.param("cluster_init_password", None)
        cluster_init_port = self.input.param("cluster_init_port", None)
        cluster_init_ramsize = self.input.param("cluster_init_ramsize", None)
        command_init = self.input.param("command_init", "cluster-init")
        server = self.servers[-1]
        remote_client = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        rest.force_eject_node()
        self.sleep(5)

        try:
            cli_command = command_init
            options = ""
            if  cluster_init_username is not None:
                options += "--cluster-init-username={0} ".format(cluster_init_username)
            if cluster_init_password is not None:
                options += "--cluster-init-password={0} ".format(cluster_init_password)
            if cluster_init_port is not None:
                options += "--cluster-init-port={0} ".format(cluster_init_port)
            if cluster_init_ramsize is None:
                options += "--cluster-init-ramsize={0} ".format(cluster_init_ramsize)

            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user=None, password=None)
            self.assertEqual(output[0], 'ERROR: unable to init localhost (400) Bad Request')
            self.assertTrue(output[1] == "[u'Username and password are required.']" or output[1] == "[u'The password must be at least six characters.']")
            remote_client.disconnect()
        finally:
            rest = RestConnection(server)
            rest.force_eject_node()
            self.sleep(5)
            rest.init_cluster()

    def testBucketCreation(self):
        bucket_name = self.input.param("bucket", "default")
        bucket_type = self.input.param("bucket_type", "couchbase")
        bucket_port = self.input.param("bucket_port", 11211)
        bucket_replica = self.input.param("bucket_replica", 1)
        bucket_password = self.input.param("bucket_password", None)
        bucket_ramsize = self.input.param("bucket_ramsize", 200)
        wait = self.input.param("wait", False)
        enable_flush = self.input.param("enable_flush", None)
        enable_index_replica = self.input.param("enable_index_replica", None)

        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        self._create_bucket(remote_client, bucket=bucket_name, bucket_type=bucket_type, bucket_port=bucket_port, bucket_password=bucket_password, \
                        bucket_ramsize=bucket_ramsize, bucket_replica=bucket_replica, wait=wait, enable_flush=enable_flush, enable_index_replica=enable_index_replica)
        buckets = rest.get_buckets()
        result = True
        if len(buckets) != 1:
            self.log.error("Expected to ge only 1 bucket")
            result = False
        bucket = buckets[0]
        if bucket_name != bucket.name:
            self.log.error("Bucket name is not correct")
            result = False
        if not (bucket_port == bucket.nodes[0].moxi or bucket_port == bucket.port):
            self.log.error("Bucket port is not correct")
            result = False
        if bucket_type == "couchbase" and "membase" != bucket.type or\
            (bucket_type == "memcached" and "memcached" != bucket.type):
            self.log.error("Bucket type is not correct")
            result = False
        if bucket_type == "couchbase" and bucket_replica != bucket.numReplicas:
            self.log.error("Bucket num replica is not correct")
            result = False
        if bucket.saslPassword != (bucket_password, "")[bucket_password is None]:
            self.log.error("Bucket password is not correct")
            result = False
        if bucket_ramsize * 1048576 not in range(int(int(buckets[0].stats.ram) * 0.95), int(int(buckets[0].stats.ram) * 1.05)):
            self.log.error("Bucket RAM size is not correct")
            result = False

        if not result:
            self.fail("Bucket was created with incorrect properties")

        remote_client.disconnect()


    def testBucketModification(self):
        cli_command = "bucket-edit"
        bucket_password = self.input.param("bucket_password", None)
        bucket_port = self.input.param("bucket_port", 11211)
        enable_flush = self.input.param("enable_flush", None)
        self.testBucketCreation()
        bucket_port_new = self.input.param("bucket_port_new", None)
        bucket_password_new = self.input.param("bucket_password_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        enable_flush_new = self.input.param("enable_flush_new", None)
        enable_index_replica_new = self.input.param("enable_index_replica_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        bucket = self.input.param("bucket", "default")
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)

        options = "--bucket={0}".format(bucket)
        options += (" --enable-flush={0}".format(enable_flush_new), "")[enable_flush_new is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica_new), "")[enable_index_replica_new is None]
        options += (" --bucket-port={0}".format(bucket_port_new), "")[bucket_port_new is None]
        options += (" --bucket-password={0}".format(bucket_password_new), "")[bucket_password_new is None]
        options += (" --bucket-ramsize={0}".format(bucket_ramsize_new), "")[bucket_ramsize_new is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-edit'])

        if bucket_password_new is not None:
            bucket_password_new = bucket_password
        if bucket_port_new is not None:
            bucket_port = bucket_port_new
        if bucket_ramsize_new is not None:
            bucket_ramsize = bucket_ramsize_new

        buckets = rest.get_buckets()
        result = True
        if len(buckets) != 1:
            self.log.error("Expected to ge only 1 bucket")
            result = False
        bucket = buckets[0]
        if not (bucket_port == bucket.nodes[0].moxi or bucket_port == bucket.port):
            self.log.error("Bucket port is not correct")
            result = False
        if bucket.saslPassword != (bucket_password, "")[bucket_password is None]:
            self.log.error("Bucket password is not correct")
            result = False
        if bucket_ramsize * 1048576 not in range(int(int(buckets[0].stats.ram) * 0.95), int(int(buckets[0].stats.ram) * 1.05)):
            self.log.error("Bucket RAM size is not correct")
            result = False

        if not result:
            self.fail("Bucket was created with incorrect properties")

        options = ""
        cli_command = "bucket-flush"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['Running this command will totally PURGE database data from disk.Do you really want to do it? (Yes/No)TIMED OUT: command: bucket-flush: localhost:8091, most likely bucket is not flushed'])
        if not enable_flush:
            cli_command = "bucket-flush --force"
            options = "--bucket={0}".format(bucket)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ['Database data will be purged from disk ...', 'ERROR: unable to bucket-flush; please check your username (-u) and password (-p); (400) Bad Request', "{u'_': u'Flush is disabled for the bucket'}"])
            cli_command = "bucket-edit"
            options = "--bucket={0} --enable-flush=1 --bucket-ramsize=500".format(bucket)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ['SUCCESS: bucket-edit'])

        cli_command = "bucket-flush --force"
        options = "--bucket={0}".format(bucket)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-edit'])

        cli_command = "bucket-delete"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-delete'])

        remote_client.disconnect()

    #MB-8566
    def testSettingCompacttion(self):
        '''setting-compacttion OPTIONS:
        --compaction-db-percentage=PERCENTAGE     at which point database compaction is triggered
        --compaction-db-size=SIZE[MB]             at which point database compaction is triggered
        --compaction-view-percentage=PERCENTAGE   at which point view compaction is triggered
        --compaction-view-size=SIZE[MB]           at which point view compaction is triggered
        --compaction-period-from=HH:MM            allow compaction time period from
        --compaction-period-to=HH:MM              allow compaction time period to
        --enable-compaction-abort=[0|1]           allow compaction abort when time expires
        --enable-compaction-parallel=[0|1]        allow parallel compaction for database and view'''
        compaction_db_percentage = self.input.param("compaction-db-percentage", None)
        compaction_db_size = self.input.param("compaction-db-size", None)
        compaction_view_percentage = self.input.param("compaction-view-percentage", None)
        compaction_view_size = self.input.param("compaction-view-size", None)
        compaction_period_from = self.input.param("compaction-period-from", None)
        compaction_period_to = self.input.param("compaction-period-to", None)
        enable_compaction_abort = self.input.param("enable-compaction-abort", None)
        enable_compaction_parallel = self.input.param("enable-compaction-parallel", None)
        bucket = self.input.param("bucket", "default")
        output = self.input.param("output", '')
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.testBucketCreation()
        cli_command = "setting-compacttion"
        options = "--bucket={0}".format(bucket)
        options += (" --compaction-db-percentage={0}".format(compaction_db_percentage), "")[compaction_db_percentage is None]
        options += (" --compaction-db-size={0}".format(compaction_db_size), "")[compaction_db_size is None]
        options += (" --compaction-view-percentage={0}".format(compaction_view_percentage), "")[compaction_view_percentage is None]
        options += (" --compaction-view-size={0}".format(compaction_view_size), "")[compaction_view_size is None]
        options += (" --compaction-period-from={0}".format(compaction_period_from), "")[compaction_period_from is None]
        options += (" --compaction-period-to={0}".format(compaction_period_to), "")[compaction_period_to is None]
        options += (" --enable-compaction-abort={0}".format(enable_compaction_abort), "")[enable_compaction_abort is None]
        options += (" --enable-compaction-parallel={0}".format(enable_compaction_parallel), "")[enable_compaction_parallel is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-edit'])
        cluster_status = rest.cluster_status()
        remote_client.disconnect()

    def testXDCRSetup(self):
        '''xdcr-setup OPTIONS:
        --create                           create a new xdcr configuration
        --edit                             modify existed xdcr configuration
        --delete                           delete existed xdcr configuration
        --xdcr-cluster-name=CLUSTERNAME    cluster name
        --xdcr-hostname=HOSTNAME           remote host name to connect to
        --xdcr-username=USERNAME           remote cluster admin username
        --xdcr-password=PASSWORD           remtoe cluster admin password'''
        remote_client = RemoteMachineShellConnection(self.master)
        try:
            #rest = RestConnection(self.master)
            #xdcr_cluster_name & xdcr_hostname=the number of server in ini file to add to master as replication
            xdcr_cluster_name = self.input.param("xdcr-cluster-name", None)
            xdcr_hostname = self.input.param("xdcr-hostname", None)
            xdcr_username = self.input.param("xdcr-username", None)
            xdcr_password = self.input.param("xdcr-password", None)
            output_error = ""
            ip = None
            if xdcr_cluster_name is not None:
                ip = self.servers[xdcr_cluster_name].ip
#            if ip is not None:
#                output_error = 'SUCCESS: init {0}'.format(ip)
            output_error = self.input.param("output_error", 'SUCCESS: init HOSTIP').replace(";", ",")
            if ip is not None:
                output_error = output_error.replace("HOSTIP", ip)
            cli_command = "xdcr-setup"
            options = "--create"
            options += (" --xdcr-cluster-name={0}".format(ip), "")[xdcr_cluster_name is None]
            if xdcr_hostname is not None:
                options += " --xdcr-hostname={0}".format(self.servers[xdcr_hostname].ip)
            options += (" --xdcr-username={0}".format(xdcr_username), "")[xdcr_username is None]
            options += (" --xdcr-password={0}".format(xdcr_password), "")[xdcr_password is None]
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual([s.rstrip() for s in output], [s for s in output_error.split(',')])

            if "SUCCESS: init" in output_error:
                #MB-8570 add verification when will be fixed
                options = options.replace("--create ", "--edit ")
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")

            if "SUCCESS: init" in output_error and xdcr_cluster_name is None:
                #MB-8573 couchbase-cli: quotes are not supported when try to remove remote xdcr cluster that has white spaces in the name
                options = "--delete --xdcr-cluster-name={0}".format("remote%20cluster")
            else:
                options = "--delete --xdcr-cluster-name={0}".format(self.servers[xdcr_cluster_name].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")

            if "SUCCESS: init" in output_error:
                self.assertEqual(output, ["SUCCESS: delete {0}".format(self.servers[xdcr_cluster_name].ip)])
            else:
                self.assertEqual(output, ["ERROR: unable to delete xdcr remote site localhost (404) Object Not Found", "unknown remote cluster"])


        finally:
            remote_client.disconnect()
            self._teardown_xdcr()

