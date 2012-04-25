import bottle
from ujson import dumps, loads
from utils import shortByHex

class StorageSource(object):
    def __init__(self):
        self.func_dict = {}
        self.data_dict = {}

    def get_func(self, id):
        if id in self.func_dict: 
            return self.func_dict[id]

    def add_func(self, id, content):
        self.func_dict[id] = content
        return id

    def del_func(self, id):
        if id in self.func_dict:
            del self.func_dict[id]
            return id

    def add_data(self, content):
        id = shortByHex(content)
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


