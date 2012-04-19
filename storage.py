import bottle
from hashlib import md5
from inspect import getsource
from ujson import dumps, loads
from marshal import dumps as mdumps
from marshal import loads as mloads
from httplib import HTTPConnection
from types import FunctionType

class StorageSource(object):
    def __init__(self):
        self.func_dict = {}
        self.data_dict = {}

    def shortByHex(self, url):
        _seed = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        _hex = md5(url).hexdigest()
        _hexLen = len(_hex)
        _subHexLen = _hexLen / 8
     
        for i in xrange(0, _subHexLen):
            _subHex = _hex[i*8:i*8+8]
            _subHex = 0x3FFFFFFF&int(1*('0x%s'%_subHex), 16)
            _o = []
            for n in xrange(0, 6):
                _index = 0x0000003D & _subHex
                _o.append(_seed[int(_index)])
                _subHex = _subHex >> 5
            _output = ''.join(_o)
            if _output not in self.data_dict:
                return _output
        return ""

    def get_func(self, id):
        if id in self.func_dict: 
            return self.func_dict[id]

    def add_func(self, id, content):
        if id not in self.func_dict:
            self.func_dict[id] = content
            return id

    def del_func(self, id):
        if id in self.func_dict:
            del self.func_dict[id]
            return id

    def add_data(self, content):
        if len(content) > 512:
            id = self.shortByHex(content[:512])
        else:
            id = self.shortByHex(content)
        self.data_dict[id] = loads(content)
        return id

    def get_data(self, id, start, end):
        if id in self.data_dict:
            if end != 0:
                data = self.data_dict[id][start:end]
            else:
                data = self.data_dict[id][start:]
            return dumps(data)

    def del_data(self, id):
        if id in self.data_dict:
            del self.data_dict[id]
            return id

class StorageClient(object):
    def __init__(self, host, port=8080):
        self.conn = HTTPConnection(host+":"+str(port))

    def add_func(self, func):
        body = mdumps(func.func_code)
        self.conn.request('POST', '/func/'+func.func_name, body)
        return self.conn.getresponse().read()

    def get_func(self, id):
        self.conn.request('GET', '/func/'+id, "")
        code = self.conn.getresponse().read()
        return FunctionType(mloads(code), globals()) 

    def delete_func(self, id):
        self.conn.request('DELETE', '/func/'+id, "")
        return self.conn.getresponse().read()

    def add_data(self, data):
        body = dumps(data)
        self.conn.request('POST', '/data', body)
        return self.conn.getresponse().read()

    def get_data(self, id, start=0, end=0):
        url = "/data/%s/%d/%s" % (id, start, end)
        self.conn.request('GET', url, "")
        return loads(self.conn.getresponse().read())

    def delete_data(self, id):
        self.conn.request('DELETE', '/data/'+id, "")
        return self.conn.getresponse().read()




source = StorageSource()

@bottle.get('/func/<id>')
def get_func(id):
    content = source.get_func(id)
    if content:
        return content
    else:
        return ""

@bottle.post('/func/<id>')
def post_func(id):
    content = bottle.request.body.read()
    if source.add_func(id, content):
        return id
    else:
        return "Existed"

@bottle.delete('/func/<id>')
def delete_func(id):
    if source.del_func(id):
        return id
    else:
        return ""

@bottle.post('/data')
def post_data():
    content = bottle.request.body.read()
    return source.add_data(content)

@bottle.get('/data/<id>/<start>/<end>')
def get_data(id, start, end):
    content = source.get_data(id, int(start), int(end))
    if content:
        return content
    else:
        return ""

@bottle.delete('/data/<id>')
def del_data(id):
    if source.del_data(id):
        return id
    else:
        return ""

if __name__ == '__main__':
    bottle.run(port=8080)