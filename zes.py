import requests
import json
import sys
import os
import time

sender = '/usr/bin/zabbix_sender'      # path zabbix_sender
cfg = '/etc/zabbix/zabbix_agentd.conf' # path to zabbix-agent config
tmp = '/tmp/es_stats.tmp'              # temp file to use

# keys for health page
traps1 = {
    "status",
    "active_primary_shards",
    "active_shards",
    "unassigned_shards"
}

# keys for cluster stats page
traps2 = {
    "indices.docs.count",
    "indices.docs.deleted",
    "indices.flush.total",
    "os.mem.actual_used_in_bytes"
}

# read specified keys from json data
def getKeys(stats,traps):
    out=''
    for t in traps:
        c=t.split('.')
        s=stats
        while len(c): s=s.get(c.pop(0),{})
        if s=={}: continue
        out += "- es.{0} {1}\n".format(t,s)
    return out

def main():
    # load json data
    try:
        f = requests.get("http://localhost:9200/_cluster/health")
        health = f.json()
        f = requests.get("http://localhost:9200/_nodes/_local/stats?all=true")
        all = f.json()

        # only for current node
        for node_id in all['nodes']:
            if all['nodes'][node_id]['host'].startswith(os.uname()[1]):
                node = all['nodes'][node_id]
                if len(sys.argv) == 1:
                    print "node found"
    except:
        print "Unable to load JSON data!"
        sys.exit(1)

    out = getKeys(health,traps1)  #getting health values
    out += getKeys(node,traps2)    #getting stats  values

    # write data for zabbix sender
    if len(sys.argv) == 1:
        print out

    try:
        with open(tmp,'w') as f: f.write(out)
    except:
        print "Unable to save data to send!"
        sys.exit(1)

    # return active check
    if len(sys.argv) > 1 and sys.argv[1] == 'jvm.uptime_in_millis':
        os.system("{0} -c {1} -i {2} >/dev/null 2>&1".format(sender,cfg,tmp))
        print node['jvm']['uptime_in_millis']
    # send data with debug
    else:
        os.system("{0} -c {1} -i {2} -vv".format(sender,cfg,tmp))

    os.remove(tmp)

if __name__ == "__main__":
    main()