import json
import zmq

ZMQ_SOCKET = 'ipc:///tmp/dataqueue.zmq'

REPLY_NAK = u'NAK'
REPLY_OK = u'OK'

class Messager(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(ZMQ_SOCKET)

    def _send_data(self, data):
        s = json.dumps(data)
        self.socket.send_string(s)

    def _tokenize(self, s):
        return [x.strip() for x in s.split()]

    def _recv_status(self):
        return self.socket.recv_string()

    def post_data(self, data):
        self._send_data(data)
        reply = self._recv_status()
        tokens = self._tokenize(reply)
        if len(tokens) == 0:
            raise Exception("Got empty reply from server")
        if tokens[0] == REPLY_OK:
            return
        elif tokens[0] == REPLY_NAK:
            if len(tokens) == 1:
                raise Exception("Got NAK from server, but no error message")
            raise Exception(reply[len(tokens[0])+1:])
        raise Exception("Unexpected reply from server: %s", reply)

if __name__ == '__main__':
    messager = Messager()
    data = {
            "model": "arome_metcoop_2500",
            "date": "2014-07-24",
            "term": 6,
            "files": [
                {
                    "uri": "opdata:///arome2_5_main/AROME_MetCoOp_06_DEF.nc"
                }
            ]
        }
    messager.post_data(data)
