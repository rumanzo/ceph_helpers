#!/usr/bin/env python
import json
import subprocess
import re


class Cephrawoverqouted(object):

    def __init__(self, config='/etc/ceph/ceph.conf'):
        self.config = config
        self.tree = self.gettree()
        self.info = self.getpoolsinfo()

    def cephexecjson(self, string):
        stdout, stderr = subprocess.Popen(re.split('\s+', string) + ['-c', self.config, '--format=json'],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if stderr:
            raise ValueError(stderr.decode('utf-8'))
        else:
            try:
                return json.loads(stdout)
            except ValueError:
                stdout = stdout.replace('-nan', '0')
                return json.loads(stdout)

    def gettree(self):
        return self.cephexecjson('ceph osd tree')['nodes']

    def getroots(self):
        return [x for x in self.tree if x['type'] == 'root']

    def __recursein(self, ids):
        newids = set()
        for id in ids:
            item = [item for item in self.tree if item['id'] == id][0]
            if item['type'] == 'osd':
                newids.add(item['id'])
            else:
                newids |= self.__recursein(item['children'])
        return newids

    def rootssumosd(self):
        osdsum = {}
        for row in self.getroots():
            osdsum[row['name']] = self.__recursein(row['children'])
        return osdsum

    def deviceclasssumosd(self):
        osdsum = {}
        types= set([row['device_class'] for row in self.gettree() if 'device_class' in row])
        if not types:
            return None
        for devclass in types:
            osdsum[devclass] = set([row['id'] for row in self.gettree() if 'device_class' in row and row['device_class'] == devclass])
        return osdsum

    def rootrawsize_kb(self, items):
        rootrawsize = {}
        osddf = self.cephexecjson('ceph osd df')
        for root, items in items.items():
            rootrawsize[root] = sum({row['id']: row['kb'] for row in osddf[
                                    'nodes'] if row['id'] in items}.values())
        return rootrawsize

    def getpoolsinfo(self):
        return self.cephexecjson('ceph osd dump')

    def poolfactor(self):
        erasureprofiles = self.info['erasure_code_profiles'].keys()
        poolfactors = {}
        for pool in self.info['pools']:
            if (pool['tier_of'] == -1) and (pool['erasure_code_profile'] not in erasureprofiles):
                poolfactors[pool['pool_name']] = pool['size']
            elif (pool['tier_of'] == -1) and (pool['erasure_code_profile'] in erasureprofiles):
                profile = self.info['erasure_code_profiles'][
                    pool['erasure_code_profile']]
                k, m = float(profile['k']), float(profile['m'])
                poolfactors[pool['pool_name']] = 1 + (m / k)
        return poolfactors

    def rbdsizeperpool(self):
        rbdsizesperpool = {}
        for pool in self.info['pools']:
            if (pool['tier_of'] == -1):
                imagesizes = []
                for image in self.cephexecjson('rbd ls {}'.format(pool['pool_name'])):
                    imagesizes.append(self.cephexecjson(
                        'rbd info {}/{}'.format(pool['pool_name'], image))['size'])
                rbdsizesperpool[pool['pool_name']] = sum(imagesizes)
        return rbdsizesperpool


if __name__ == '__main__':
    main = Cephrawoverqouted(config='/etc/ceph/ceph.conf')
    if main.deviceclasssumosd():
        for root, size in main.rootrawsize_kb(main.deviceclasssumosd()).items():
            print('Size device class root {}: {} GB'.format(root, size / 1024 / 1024))
    for root, size in main.rootrawsize_kb(main.rootssumosd()).items():
        print('Size root {}: {} GB'.format(root, size / 1024 / 1024))
    for key, value in main.rbdsizeperpool().items():
        print('Fullsize rbd images in pool {}: {} GB'.format(
            key, value * main.poolfactor()[key] / 1024 / 1024 / 1024))
    summ = []
    for key, value in main.rbdsizeperpool().items():
        summ.append(value * main.poolfactor()[key] / 1024 / 1024 / 1024)
    print('Summary fullsize rbd in pools: {} GB'.format(sum(summ)))
