def getPort(start, end):
    import os,random  
    pscmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"  
    procs = os.popen(pscmd).read()  
    procarr = procs.split("\n")  
    tt= random.randint(start,end)  
    if tt not in procarr:  
        return tt  
    else:  
        getPort()

def split_n(l, n):
    len_l = len(l)
    num = len_l / n
    k = len_l - num * n
    end = 0
    res = []
    while end < len_l:
        start = end
        end = start + num
        if len(res) < k: end += 1
        res.append(l[start:end])
    return res

def shortByHex(url):
    from hashlib import md5
    _seed = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    _hex = md5(url).hexdigest()
    _hexLen = len(_hex)
    _subHexLen = _hexLen / 8
    _subHex = _hex[0:8]
    _subHex = 0x3FFFFFFF & int(1*('0x%s'%_subHex), 16)
    _o = []
    for n in xrange(0, 6):
        _index = 0x0000003D & _subHex
        _o.append(_seed[int(_index)])
        _subHex = _subHex >> 5
    return ''.join(_o)

from client import StorageClient

class Data(object):
    def __init__(self, data_id, start, end, base_addr):
        self.base_addr = base_addr
        self.data_id = data_id
        self.start = start
        self.end = end
        self.data = None
        self.len = self.end-self.start

    def __len__(self):
        return self.len

    def __getslice__(self, i, j):
        if i < 0: 
            start = self.end + i 
        else:
            start = self.start + i

        if j < 0:
            end = self.end + j
        else:
            end = self.start + j
        return Data(self.data_id, start, end, self.base_addr)

    def __getitem__(self, key):
        if not self.data:
            self.data = StorageClient(*self.base_addr).get_data(self.data_id, self.start, self.end)
        return self.data[key]

    @classmethod
    def warp(cls, source, data):
        data_id = source.add_data(data)
        return Data(data_id, 0, len(data), source.base_addr)