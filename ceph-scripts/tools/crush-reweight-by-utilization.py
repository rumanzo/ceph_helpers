#!/usr/bin/env python

from cephinfo import cephinfo
import ceph_osds_in_bucket
from optparse import OptionParser
from collections import defaultdict
import os

# don't reweight if we have too few OSDs
MIN_OSDS = 2

# don't reweight if there is less than 100GB per OSD total capacity
mon_reweight_min_bytes_per_osd = 10*1024*1024*1024

# don't reweight if there is less than 1GB used capacity per OSD
mon_reweight_min_bytes_used_per_osd = 1024*1024*1024

# don't reweight if there are less than 10 PGs per OSD
mon_reweight_min_pgs_per_osd = 10

VERBOSE = False

def get_weight(osd, type='reweight'):
  try:
    return osd_weights[osd][type]
  except KeyError:
    return 0.0

def change_weight(osd, new_weight, really):
  cmd = "ceph osd reweight %d %5f &" % (osd, new_weight)
  if VERBOSE: print cmd
  if really:
    os.system(cmd)
  else:
    print "add --really to run '%s'" % cmd

def reweight_by_utilization(options):
  if options.oload <= 100:
    raise Exception("You must give a percentage higher than 100.")

  cephinfo.init_pg()
  pgm = cephinfo.pg_data

  pgs_by_osd = defaultdict(int)

  if options.by_pg:
    weight_sum = 0.0
    num_pg_copies = 0
    num_osds = 0
    for p in pgm['pg_stats']:
      pool = p['pgid'].split('.')[0]
      if options.pools and pool not in options.pools:
         continue
      for q in p['up']:
        if not pgs_by_osd[q]:
          pgs_by_osd[q] = 0
          weight_sum += get_weight(q,'crush_weight')
          num_osds += 1
        pgs_by_osd[q] += 1
        num_pg_copies += 1

    if not num_osds or (num_pg_copies / num_osds < mon_reweight_min_pgs_per_osd):
      raise Exception('Refusing to reweight: we only have %d PGs across %d osds!' % (num_pg_copies, num_osds))

    average_util = num_pg_copies / weight_sum
    if VERBOSE: print "weight_sum: %3f, num_pg_copies: %d, num_osds: %d" % (weight_sum, num_pg_copies, num_osds)

  else:
    num_osd = len(pgm['osd_stats'])

    if num_osd < MIN_OSDS:
      raise Exception("Refusing to reweight: we have only %d OSDs! (%d needed)" % (num_osd, MIN_OSDS))

    # Avoid putting a small number (or 0) in the denominator when calculating average_util
    if pgm['osd_stats_sum']['kb'] * 1024 / num_osd < mon_reweight_min_bytes_per_osd:
      raise Exception("Refusing to reweight: we only have %d GB total space across all osds! (%d GB needed)" % (pgm['osd_stats_sum']['kb'] / 1024 / 1024, mon_reweight_min_bytes_per_osd * num_osd / 1024 / 1024 / 1024))

    if pgm['osd_stats_sum']['kb_used'] * 1024 / num_osd < mon_reweight_min_bytes_used_per_osd:
      raise Exception("Refusing to reweight: we only have %d GB used across all osds! (%d GB needed)" % (pgm['osd_stats_sum']['kb_used'] / 1024 / 1024, mon_reweight_min_bytes_used_per_osd * num_osd / 1024 / 1024 / 1024))

    average_util = float(pgm['osd_stats_sum']['kb_used']) / float(pgm['osd_stats_sum']['kb'])

  if VERBOSE: print "Found %d OSDs in total" % len(pgm['osd_stats'])

  # filter out the empty osds
  nonempty_osds = [ osd for osd in pgm['osd_stats'] if float(osd['kb']) > 0 and get_weight(osd['osd'],type='crush_weight') > 0 ]

  if VERBOSE: print "Found %d non-empty OSDs" % len(nonempty_osds)

  # optionally filter out osds not in the requested bucket
  # and recalculate average_util
  if options.bucket or options.deviceclass:
    bucket_osds = []
    if options.bucket:
      for bucket in options.bucket:
        bucket_osds.extend(ceph_osds_in_bucket.list(bucket))
    if options.deviceclass:
      for devclass in options.deviceclass:
        bucket_osds.extend(ceph_osds_in_bucket.deviceclass(devclass))
    sum_kb = 0
    sum_weight = 0
    sum_kb_used = 0
    filtered_osds = []
    for osd in nonempty_osds:
      name = 'osd.%d' % osd['osd']
      if name in bucket_osds:
        sum_weight += get_weight(osd['osd'], 'crush_weight') * 1024*1024*1024
        sum_kb_used += osd['kb_used']
        filtered_osds.insert(0, osd)
    average_util = float(sum_kb_used) / float(sum_weight)
    if VERBOSE and options.bucket: print "Found %d OSDs after filtering by bucket" % len(filtered_osds)
    if VERBOSE and options.deviceclass: print "Found %d OSDs after filtering by deviceclass" % len(filtered_osds)
  else:
    filtered_osds = nonempty_osds


  # sort osds from most to least deviant from the average_util
  if options.by_pg:
    osds = sorted(filtered_osds, key=lambda osd: -abs(average_util - pgs_by_osd[osd['osd']] / get_weight(osd['osd'],type='crush_weight')))
  else:
    #osds = sorted(filtered_osds, key=lambda osd: -abs(average_util - float(osd['kb_used']) / float(osd['kb'])))
    osds = sorted(filtered_osds, key=lambda osd: -abs(average_util - float(osd['kb_used']) / (get_weight(osd['osd'],type='crush_weight') * 1024*1024*1024)))

  # adjust down only if we are above the threshold
  overload_util = average_util * options.oload / 100.0

  # but aggressively adjust weights up whenever possible
  underload_util = average_util

  if VERBOSE: print "average_util: %04f, overload_util: %04f, underload_util: %04f. " %(average_util, overload_util, underload_util)

  n = 0
  for osd in osds:
    if options.by_pg:
      util = pgs_by_osd[osd['osd']] / get_weight(osd['osd'],type='crush_weight')
    else:
      util = float(osd['kb_used']) / (get_weight(osd['osd'],type='crush_weight') * 1024*1024*1024)

    # skip very empty OSDs
    if util < 0.001:
      continue

    if util >= overload_util:
      # Assign a lower weight to overloaded OSDs. The current weight
      # is a factor to take into account the original weights,
      # to represent e.g. differing storage capacities
      weight = get_weight(osd['osd'])
      new_weight = (average_util / util) * float(weight)
      new_weight = max(new_weight, weight - options.max_change)
      print "osd.%d (%4f >= %4f) [%04f -> %04f]" % (osd['osd'], util, overload_util, weight, new_weight)
      if options.doit: change_weight(osd['osd'], new_weight, options.really)
      n += 1
      if n >= options.num_osds: break
    if not options.no_increasing and util <= underload_util:
      # assign a higher weight.. if we can
      weight = get_weight(osd['osd'])
      new_weight = (average_util / util) * float(weight)
      new_weight = max(new_weight, weight + 0.01)
      new_weight = min(new_weight, weight + options.max_change)
      if new_weight > 1.0:
        new_weight = 1.0
      if new_weight > weight:
        print "osd.%d (%4f <= %4f) [%04f -> %04f]" % (osd['osd'], util, underload_util, weight, new_weight)
        if options.doit: change_weight(osd['osd'], new_weight, options.really)
        n += 1
        if n >= options.num_osds: break

def get_weights():
  cephinfo.init_crush()
  global osd_weights

  osd_weights = dict()
 
  for osd in cephinfo.crush_data['nodes']:
    if osd['type'] == 'osd':
      id = osd['id']
      reweight = float(osd['reweight'])
      crush_weight = float(osd['crush_weight'])
      osd_weights[id] = dict()
      osd_weights[id]['crush_weight'] = crush_weight
      osd_weights[id]['reweight'] = reweight

if __name__ == "__main__":
  import sys
  parser = OptionParser()
  parser.add_option("--overload", dest="oload", type="float", default=120.0,
                  help="The overload threshold percentage, default 120%")
  parser.add_option("--by-pg", dest="by_pg", action="store_true",
                  help="Reweight by num PGs instead of utilization")
  parser.add_option("--pool", dest="pools", action="append",
                  help="Only work on these pools.")
  parser.add_option("--no-increasing", dest="no_increasing", action="store_true",
                  help="Also adjust weights up if OSDs are below ideal weight")
  parser.add_option("--max-change", dest="max_change", type="float", default=0.01,
                  help="Maximum weight change to each OSD (default 0.01)")
  parser.add_option("--num-osds", dest="num_osds", type="int", default=4,
                  help="Number of OSDs to change (default 4)")
  parser.add_option("--doit", dest="doit", action="store_true",
                  help="Do it!")
  parser.add_option("--really", dest="really", action="store_true",
                  help="Really really do it! This will change your crush map.")
  parser.add_option("--bucket", action="append",
                    help="Only reweight OSDs in this CRUSH bucket")
  parser.add_option("--deviceclass", action="append",
                    help="Only reweight OSDs with this device type")
  parser.add_option("--verbose", action="store_true", help="Be verbose")
  (options, args) = parser.parse_args()

  VERBOSE = options.verbose

  if options.bucket and options.by_pg:
    raise Exception("Use of --by-pg and --bucket at the same time is not implemented")

  get_weights()
  try:
    reweight_by_utilization(options)
  except Exception as e:
    if VERBOSE: raise(e)
