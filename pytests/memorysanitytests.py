import unittest
import datetime
from TestInput import TestInputSingleton
import time
import sys
import uuid
import logger
import string
import random
import math
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedClient, MemcachedError
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from couchbase.stats_tools import StatsCommon

class MemorySanity(BaseTestCase):

    def setUp(self):
        super(MemorySanity, self).setUp()
        self.kv_verify = self.input.param('kv_verify', True)
        self.log.info("==============  MemorySanityTest setup was started for test #{0} {1}=============="\
                      .format(self.case_number, self._testMethodName))
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)

    def tearDown(self):
        self.log.info("==============  teardown was started for test #{0} {1}=============="\
                      .format(self.case_number, self._testMethodName))
        super(MemorySanity, self).tearDown()

    """
        This test creates a bucket, adds an initial front end load,
        checks memory stats, deletes the bucket, and recreates the
        same scenario repetitively for the specified number of times,
        and checks after the last repetition if the memory usage is
        the same that was at the end of the very first front end load.
    """
    def repetitive_create_delete(self):
        self.repetitions = self.input.param("repetition_count", 1)
        self.bufferspace = self.input.param("bufferspace", 100000)
        #the first front end load
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=5, timeout_secs=100)
        self._wait_for_stats_all_buckets(self.servers)
        rest = RestConnection(self.servers[0])
        max_data_sizes = {}
        initial_memory_usage = {}
        for bucket in self.buckets:
            max_data_sizes[bucket.name] = rest.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["ep_max_size"][-1]
            self.log.info("Initial max_data_size of bucket '{0}': {1}".format(bucket.name, max_data_sizes[bucket.name]))
            initial_memory_usage[bucket.name] = rest.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["mem_used"][-1]
            self.log.info("initial memory consumption of bucket '{0}' with load: {1}".format(bucket.name, initial_memory_usage[bucket.name]))
        mem_usage = {}
        time.sleep(10)
        #the repetitions
        for i in range(0, self.repetitions):
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            del self.buckets[:]
            self._bucket_creation()
            self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=5, timeout_secs=100)
            self._wait_for_stats_all_buckets(self.servers)
            for bucket in self.buckets:
                mem_usage[bucket.name] = rest.fetch_bucket_stats(bucket.name)["op"]["samples"]["mem_used"][-1]
                self.log.info("Memory used after attempt {0} = {1}, Difference from initial snapshot: {2}"\
                              .format(i + 1, mem_usage[bucket.name], (mem_usage[bucket.name] - initial_memory_usage[bucket.name])))
            time.sleep(10)
        if (self.repetitions > 0):
            self.log.info("After {0} repetitive deletion-creation-load of the buckets, the memory consumption difference is .."\
                          .format(self.repetitions));
            for bucket in self.buckets:
                self.log.info("{0} :: Initial: {1} :: Now: {2} :: Difference: {3}"\
                              .format(bucket.name, initial_memory_usage[bucket.name], mem_usage[bucket.name],
                                  (mem_usage[bucket.name] - initial_memory_usage[bucket.name])))
                msg = "Memory used now, much greater than initial usage!"
                assert mem_usage[bucket.name] <= initial_memory_usage[bucket.name] + self.bufferspace, msg
        else:
            self.log.info("Verification skipped, as there weren't any repetitions..");

    '''
    Test created based on MB-8432
    steps:
    create a bucket size of 100M
    load data enough to create a DGM
    check the "tc_malloc_allocated" stat is within 100M bound.
    '''
    def memory_quota_default_bucket(self):
        resident_ratio = self.input.param("resident_ratio", 50)
        delta_items = 200000
        mc = MemcachedClientHelper.direct_client(self.master, self.default_bucket_name)

        self.log.info("LOAD PHASE")
        end_time = time.time() + self.wait_timeout * 30
        while (int(mc.stats()["vb_active_perc_mem_resident"]) == 0 or\
               int(mc.stats()["vb_active_perc_mem_resident"]) > resident_ratio) and\
              time.time() < end_time:
            self.log.info("Resident ratio is %s" % mc.stats()["vb_active_perc_mem_resident"])
            gen = DocumentGenerator('test_docs', '{{"age": {0}}}', xrange(5),
                                    start=self.num_items, end=(self.num_items + delta_items))
            self._load_all_buckets(self.master, gen, 'create', 0)
            self.num_items += delta_items
            self.log.info("Resident ratio is %s" % mc.stats()["vb_active_perc_mem_resident"])
        memory_mb = int(mc.stats("memory")["total_allocated_bytes"]) / (1024 * 1024)
        self.log.info("total_allocated_bytes is %s" % memory_mb)
        self.assertTrue(memory_mb <= self.quota, "total_allocated_bytes %s should be within %s" % (
                                                  memory_mb, self.quota))


    def random_str_generator(self, size=4, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    """
    Test to load items of a specified size.
    Append a selected list of keys, until
    the items are match a desired size.
    """
    def test_items_append(self):
        self.desired_item_size = self.input.param("desired_item_size", 2048)
        self.append_size = self.input.param("append_size", 1024)
        self.fixed_append_size = self.input.param("fixed_append_size", True)
        self.append_ratio = self.input.param("append_ratio", 0.5)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=5, timeout_secs=100)

        for bucket in self.buckets:
            verify_dict = {}
            vkeys, dkeys = bucket.kvs[1].key_set()

            key_count = len(vkeys)
            app_ratio = self.append_ratio * key_count
            selected_keys = []
            i = 0
            for key in vkeys:
                i += 1
                if i >= app_ratio:
                    break
                selected_keys.append(key)

            awareness = VBucketAwareMemcached(RestConnection(self.master), bucket.name)
            if self.kv_verify:
                for key in selected_keys:
                    value = awareness.memcached(key).get(key)[2]
                    verify_dict[key] = value

            self.log.info("Bucket: {0}".format(bucket.name))
            self.log.info("Appending to have items whose initial size was "
                            + "{0} to equal or cross a size of {1}".format(self.value_size, self.desired_item_size))
            self.log.info("Item-appending of {0} items starting ..".format(len(selected_keys) + 1))

            index = 3
            while self.value_size < self.desired_item_size:
                str_len = self.append_size
                if not self.fixed_append_size:
                    str_len = int(math.pow(2, index))

                for key in selected_keys:
                    random_string = self.random_str_generator(str_len)
                    awareness.memcached(key).append(key, random_string)

                    if self.kv_verify:
                        verify_dict[key] = verify_dict[key] + random_string
                self.log.info("for {0} items size was increased to {1}".format(len(selected_keys) + 1, self.value_size))
                self.value_size += str_len
                index += 1

            self.log.info("The appending of {0} items ended".format(len(selected_keys) + 1))

            msg = "Bucket:{0}".format(bucket.name)
            self.log.info("VERIFICATION <" + msg + ">: Phase 0 - Check the gap between "
                      + "mem_used by the bucket and total_allocated_bytes")
            stats = StatsCommon()
            mem_used_stats = stats.get_stats(self.servers, bucket, 'memory', 'mem_used')
            total_allocated_bytes_stats = stats.get_stats(self.servers, bucket, 'memory', 'total_allocated_bytes')
            total_fragmentation_bytes_stats = stats.get_stats(self.servers, bucket, 'memory', 'total_fragmentation_bytes')

            for server in self.servers:
                self.log.info("In {0} bucket {1}, total_fragmentation_bytes + the total_allocated_bytes = {2}"
                              .format(server.ip, bucket.name, (int(total_fragmentation_bytes_stats[server]) + int(total_allocated_bytes_stats[server]))))
                self.log.info("In {0} bucket {1}, mem_used = {2}".format(server.ip, bucket.name, mem_used_stats[server]))
                self.log.info("In {0} bucket {1}, the difference between actual memory used by memcached and mem_used is {2} times"
                              .format(server.ip, bucket.name, float(int(total_fragmentation_bytes_stats[server]) + int(total_allocated_bytes_stats[server])) / float(mem_used_stats[server])))

            self.log.info("VERIFICATION <" + msg + ">: Phase1 - Check if any of the "
                    + "selected keys have value less than the desired value size")
            for key in selected_keys:
                value = awareness.memcached(key).get(key)[2]
                if len(value) < self.desired_item_size:
                    self.fail("Failed to append enough to make value size surpass the "
                                + "size, for key {0}".format(key))

            if self.kv_verify:
                self.log.info("VERIFICATION <" + msg + ">: Phase2 - Check if the content "
                        + "after the appends match what's expected")
                for k in verify_dict:
                    if awareness.memcached(k).get(k)[2] != verify_dict[k]:
                        self.fail("Content at key {0}: not what's expected.".format(k))
                self.log.info("VERIFICATION <" + msg + ">: Successful")

