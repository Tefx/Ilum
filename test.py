from client import Client, StorageClient
from utils import Fun as F
from cPickle import dumps
from zlib import compress

D = StorageClient("localhost")

def fun(item):
    from hashlib import sha512
    res = ""
    for i in range(10000):
        res = sha512(item*i+res).hexdigest()
    return res

e1 = (cmp, 0, (cmp, 4, 6))
e2 = ("map", F(fun), map(str, range(100)))
e3 = (cmp, (cmp, 0, (cmp, 0)), (cmp, 0, 4))
e4 = ("seq", (cmp, 3, 1), (cmp, 6, 8))
e5 = ("local", "seq", (cmp, 3, 1), (cmp, 6, 6))
#e7 = ("map", F(fun), D(['1','2','3','4','5','6','7','8','9']))
e8 = (range, 10)
#e9 = ("seq", e7, e7, e7)

Client("localhost").eval(e2)
