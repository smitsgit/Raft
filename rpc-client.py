from rpcmodule import RPCProxy
import time
import sys


def main():
    c = RPCProxy(("127.0.0.1", int(sys.argv[1])), authkey=b"peekaboo", timeout=0.2)

    for item in range(100):
        result = c.add(3, 5)
        print(result)
        time.sleep(1)


if __name__ == '__main__':
    main()

"""
To access this server as a client, in another Python invocation, you would simply do this:
>>> from rserver import RPCProxy
>>> c = RPCProxy((“localhost”,17000), authkey=”peekaboo”) >>> c.add(2,3)
5
>>> c.sub(2,3)
-1
>>> c.sub([1,2],4)
Traceback (most recent call last):
File “”, line 1, in
File “rpcserver.py”, line 37, in do_rpc
raise result
TypeError: unsupported operand type(s) for -: ‘list’ and ‘int’ >>>
"""
