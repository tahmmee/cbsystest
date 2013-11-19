from xdcr.xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from random import randrange
import time
import paramiko

class CBRbaseclass(XDCRReplicationBaseTest):
    def _autofail_enable(self, _rest_):
        status = _rest_.update_autofailover_settings(True, self._timeout / 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def _autofail_disable(self, _rest_):
        status = _rest_.update_autofailover_settings(False, self._timeout / 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, False)

    def wait_for_failover_or_assert(self, master, autofailover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + 300
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            self.sleep(30)

        if failover_count != autofailover_count:
            rest = RestConnection(master)
            self.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
            self.fail("{0} node(s) failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))
        else:
            self.log.info("{0} node(s) failed over as expected".format(failover_count))

    def get_failover_count(self, master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            self.log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count

    def wait_for_catchup(self, _healthy_, _compromised_, bucket):
        start = time.time()
        _flag = False
        rest1 = RestConnection(_healthy_)
        rest2 = RestConnection(_compromised_)
        while time.time() - start < 180:
            _count1 = rest1.fetch_bucket_stats(bucket=bucket)["op"]["samples"]["curr_items"][-1]
            _count2 = rest2.fetch_bucket_stats(bucket=bucket)["op"]["samples"]["curr_items"][-1]
            if _count1 == _count2:
                self.log.info("Cbrecovery caught up .. {0} == {1}".format(_count1, _count2))
                _flag = True
                break
            self.log.info("Waiting for cbrecovery to catch up .. {0} != {1}".format(_count1, _count2))
            self.sleep(self._timeout)
        return _flag

    def cbr_routine(self, _healthy_, _compromised_):
        shell = RemoteMachineShellConnection(_healthy_)
        info = shell.extract_remote_info()
        _ssh_client = paramiko.SSHClient()
        _ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        _ssh_client.connect(hostname=_healthy_.ip,username=_healthy_.ssh_username,password=_healthy_.ssh_password)
        for bucket in self.buckets:
            source_ = "http://{0}:{1}@{2}:{3}".format(_healthy_.rest_username, _healthy_.rest_password,
                                                     _healthy_.ip, _healthy_.port)
            sink_ = "http://{0}:{1}@{2}:{3}".format(_compromised_.rest_username, _compromised_.rest_password,
                                                   _compromised_.ip, _compromised_.port)
            bucket_ = "-b {0} -B {0}".format(bucket.name)

            if info.type.lower() == "linux":
                command = "/opt/couchbase/bin/cbrecovery"
            elif info.type.lower() == "windows":
                command = "C:/Program\ Files/Couchbase/Server/bin/cbrecovery.exe"

            command += " " + source_ + " " + sink_ + " " + bucket_
            self.log.info("Running command .. {0}".format(command))
            _ssh_client.exec_command(command)
            self.sleep(self._timeout)

            _check = self.wait_for_catchup(_healthy_, _compromised_, bucket.name)
            _attempts = 1
            while not _check:
                _attempts += 1
                _ssh_client.exec_command(command)
                _check = self.wait_for_catchup(_healthy_, _compromised_, bucket.name)
                if _attempts > 4:
                    self.log.error("Cbrecovery didn't complete in time, ending test ..")
                    rest = RestConnection(_compromised_)
                    rest.remove_all_recoveries()
                    raise Exception("Unable to complete test, cbrecovery failed to complete in expected time")
                    break
        shell.disconnect()

    def trigger_rebalance(self, rest):
        _nodes_ = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in _nodes_], ejectedNodes=[])
        RestHelper(rest).rebalance_reached()

    def auto_fail_over(self, master):
        _count_ = 1
        rest = RestConnection(master)
        if "stop_server" in self.failover_reason:
            for node in self.failed_nodes:
                """
                Autofailover will not auto failover nodes, if it could
                result in data loss, so force failover
                """
                if _count_ > self._num_replicas:
                    time.sleep(10)
                    for item in rest.node_statuses():
                        if node.ip == item.ip:
                            rest.fail_over(item.id)
                            break
                    _count_ += 1
                    continue
                shell = RemoteMachineShellConnection(node)
                shell.stop_couchbase()
                self.wait_for_failover_or_assert(master, _count_, self._timeout)
                shell.disconnect()
                rest.reset_autofailover()
                _count_ += 1

        elif "firewall_block" in self.failover_reason:
            for node in self.failed_nodes:
                """
                Autofailover will not auto failover nodes, if it could
                result in data loss, so force failover
                """
                if _count_ > self._num_replicas:
                    time.sleep(10)
                    for item in rest.node_statuses():
                        if node.ip == item.ip:
                            rest.fail_over(item.id)
                            break
                    _count_ += 1
                    continue
                shell = RemoteMachineShellConnection(node)
                o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
                self.wait_for_failover_or_assert(master, _count_, self._timeout)
                shell.disconnect()
                rest.reset_autofailover()
                _count_ += 1

    def vbucket_map_checker(self, map_before, map_after, initial_set, final_set):
        """
        map_before: initial vbucket_map
        map_after: changed vbucket_map
        initial_set: no. of nodes in the cluster when initial vbucket_map considered
        final_set: no. of nodes in the cluster when changed vbucket_map considered
        """
        pre_dict = {}
        post_dict = {}

        """
        pre_dict and post_dict:
        Dictionaries that are to contain information about
        number of vbuckets on each node in cluster
        """
        pre_dict = dict((i, 0) for i in xrange(initial_set))
        post_dict = dict((i, 0) for i in xrange(final_set))

        for i in map_before:
            for j in range(initial_set):
                if i[0] == j:
                    pre_dict[j] += 1

        for i in map_after:
            for j in range(final_set):
                if i[0] == j:
                    post_dict[j] += 1

        if len(pre_dict)!=len(post_dict):
            return False

        for i in pre_dict:
            for j in post_dict:
                if i == j:
                    if pre_dict[i] != post_dict[j]:
                        return False
        return True

#Assumption that at least 4 nodes on every cluster
class cbrecovery(CBRbaseclass, XDCRReplicationBaseTest):
    def setUp(self):
        super(cbrecovery, self).setUp()
        self._failover_count = self._input.param("fail_count", 0)
        self._add_count = self._input.param("add_count", 0)
        self.failover_reason = self._input.param("failover_reason", "stop_server")     # or firewall_block
        self.flag_val = self._input.param("setflag", 0)
        self.failed_nodes = []
        self._ifautofail = 0
        for server in self._servers:
            rest = RestConnection(server)
            rest.reset_autofailover()
            shell = RemoteMachineShellConnection(server)
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            shell.disconnect()

    def tearDown(self):
        if self._ifautofail == 1:
            if "stop_server" in self.failover_reason:
                for node in self.failed_nodes:
                    shell = RemoteMachineShellConnection(node)
                    shell.start_couchbase()
                    shell.disconnect()
            elif "firewall_block" in self.failover_reason:
                for node in self.failed_nodes:
                    shell = RemoteMachineShellConnection(node)
                    o, r = shell.execute_command("iptables -F")
                    shell.log_command_output(o, r)
                    o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j ACCEPT")
                    shell.log_command_output(o, r)
                    shell.disconnect()
            self.sleep(20)
        super(cbrecovery, self).tearDown()

    def cbrecover_multiple_failover_swapout_reb_routine(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0, flag=self.flag_val)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()
        """
        Tracking vbucket movement just on the default bucket for now
        """
        vbucket_map_before = []
        initial_node_count = 0
        vbucket_map_after = []
        final_node_count = 0

        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    initial_node_count = len(self.src_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes)-self._failover_count):len(self.src_nodes)]
                self.cluster.failover(self.src_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.src_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                self.sleep(self._timeout / 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    final_node_count = len(self.src_nodes)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    initial_node_count = len(self.dest_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes)-self._failover_count):len(self.dest_nodes)]
                self.cluster.failover(self.dest_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.dest_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                self.sleep(self._timeout / 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    final_node_count = len(self.dest_nodes)

            #TOVERIFY: Check if vbucket map unchanged if swap rebalance
            if self._default_bucket:
                if self._failover_count == self._add_count:
                    _flag_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after, initial_node_count, final_node_count)
                    if _flag_:
                        self.log.info("vbucket_map same as earlier")
                    else:
                        self.log.info("vbucket_map differs from earlier")

        self.sleep(self._timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results(verify_src=True)

    def cbrecover_multiple_autofailover_swapout_reb_routine(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0, flag=self.flag_val)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()
        """
        Tracking vbucket movement just on the default bucket for now
        """
        vbucket_map_before = []
        initial_node_count = 0
        vbucket_map_after = []
        final_node_count = 0

        if self._failover is not None:
            self._ifautofail = 1
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    initial_node_count = len(self.src_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on source ..".format(self.failover_reason, self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes)-self._failover_count):len(self.src_nodes)]
                self.auto_fail_over(self.src_master)
                for node in self.failed_nodes:
                    self.src_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                self.sleep(self._timeout / 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    initial_node_count = len(self.src_nodes)

                self._autofail_disable(rest)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    initial_node_count = len(self.dest_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on destination ..".format(self.failover_reason, self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes)-self._failover_count):len(self.dest_nodes)]
                self.auto_fail_over(self.dest_master)
                for node in self.failed_nodes:
                    self.dest_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                self.sleep(self._timeout / 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    final_node_count = len(self.dest_nodes)

                self._autofail_disable(rest)

            #TOVERIFY: Check if vbucket map unchanged if swap rebalance
            if self._default_bucket:
                if self._failover_count == self._add_count:
                    _flag_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after, initial_node_count, final_node_count)
                    if _flag_:
                        self.log.info("vbucket_map same as earlier")
                    else:
                        self.log.info("vbucket_map differs from earlier")

        self.sleep(self._timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results(verify_src=True)

    def cbrecover_multiple_failover_addback_routine(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0, flag=self.flag_val)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()
        """
        Tracking vbucket movement just on the default bucket for now
        """
        vbucket_map_before = []
        initial_node_count = 0
        vbucket_map_after = []
        final_node_count = 0

        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    initial_node_count = len(self.src_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes)-self._failover_count):len(self.src_nodes)]
                self.cluster.failover(self.src_nodes, self.failed_nodes)
                self.sleep(self._timeout / 4)
                self.log.info("Adding back the {0} nodes that were failed over ..".format(self._failover_count))
                for node in self.failed_nodes:
                    self.adding_back_a_node(self.src_master, node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                self.sleep(self._timeout / 4)
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    final_node_count = len(self.src_nodes)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    initial_node_count = len(self.dest_nodes)
                    vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes)-self._failover_count):len(self.dest_nodes)]
                self.cluster.failover(self.dest_nodes, self.failed_nodes)
                self.sleep(self._timeout / 4)
                self.log.info("Adding back the {0} nodes that were failed over ..".format(self._failover_count))
                for node in self.failed_nodes:
                    self.adding_back_a_node(self.dest_master, node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                self.sleep(self._timeout / 4)
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    vbucket_map_after = rest.fetch_vbucket_map()
                    final_node_count = len(self.dest_nodes)

            #TOVERIFY: Check if vbucket map unchanged if swap rebalance
            if self._default_bucket:
                if self._failover_count == self._add_count:
                    _flag_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after, initial_node_count, final_node_count)
                    if _flag_:
                        self.log.info("vbucket_map same as earlier")
                    else:
                        self.log.info("vbucket_map differs from earlier")

        self.sleep(self._timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results(verify_src=True)
