import bottle
from ujson import dumps, loads
from utils import shortByHex

class StorageSource(object):
    def __init__(self):
        self.data_dict = {}

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
            return data

    def del_data(self, id):
        if id in self.data_dict:
            del self.data_dict[id]
        return id

source = StorageSource()

@bottle.post('/data')
def post_data():
    return source.add_data(bottle.request.body.read())

@bottle.get('/data/<id>/<start>/<end>')
def get_data(id, start, end):
    content = source.get_data(id, int(start), int(end))
    return dumps(content)

@bottle.delete('/data/<id>')
def del_data(id):
    return source.del_data(id)

if __name__ == '__main__':
    bottle.run(port=8080)


