development branch of the systemtest framework for couchbase

Requires:
----
    pip install gevent
    pip install argparse
    pip install librabbitmq
    pip install pyrabbit

for running as standalone:
----
    python cbsystest.py run workload --create 100 --ops 80000  --standalone 

saslbucket:
----
    python cbsystest.py run workload --create 100 --ops 80000  --password password  --bucket saslbucket --standalone 


distribute connection among host:
----
    python cbsystest.py run workload --create 100 --ops 80000 --hosts  10.20.331.21 10.20.331.22 10.20.331.23 10.20.331.24


what else can I do?
----
    python cbsystest.py run workload --help
    

TODO: 
----
doc general usage
