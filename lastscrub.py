#!/usr/bin/env python2
import subprocess, json, datetime, sys
days = 1 if len(sys.argv) == 1 else int(sys.argv[1])
stdout, stderr = subprocess.Popen(['ceph', 'pg', 'dump', '--format=json'], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
tcomp = lambda x: (datetime.datetime.strptime(x["last_deep_scrub_stamp"],"%Y-%m-%d %H:%M:%S.%f") < (datetime.datetime.now() - datetime.timedelta(days))) and ("active" in x["state"])
print(len([x for x in json.loads(stdout)["pg_stats"] if tcomp(x)]))
