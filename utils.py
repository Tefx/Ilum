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

import marshal
from types import FunctionType
class Fun(object):
    def __init__(self, f):
        self.f_code = marshal.dumps(f.func_code)
        self.f = None

    def __call__(self, *args):
        if not self.f: 
            self.f = FunctionType(marshal.loads(self.f_code), globals())
        return self.f(*args)




