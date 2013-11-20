
for running as standalone:
----
    python cbsystest.py run workload --create 100 --ops 80000 --cluster default --standalone 

saslbucket:
----
    python cbsystest.py run workload --create 100 --ops 80000 --cluster default --password password  --bucket saslbucket --standalone 

specifying a host:
----
    python cbsystest.py run workload --create 100 --ops 80000 --cluster default --password password  --bucket saslbucket --standalone --hosts  10.20.331.21

distribute connection among host:
----
    python cbsystest.py run workload --create 100 --ops 80000 --cluster default --password password  --bucket saslbucket --standalone --hosts  10.20.331.21 10.20.331.22 10.20.331.23 10.20.331.24


TODO: 
----
doc general usage
