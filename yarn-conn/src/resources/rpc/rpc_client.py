import xmlrpc.client
import time
import sys

server_ip = sys.argv[1]
server_port = 9942

with xmlrpc.client.ServerProxy(f"http://{server_ip}:{server_port}/") as proxy:
    print(f"Connected with {server_ip}")
    print("hostname : %s" % str(proxy.get_hostname()))
    print("ip : %s" % str(proxy.get_ip()))
    time.sleep(5)
    print("exiting")
    proxy.kill()

