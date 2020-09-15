from xmlrpc.server import SimpleXMLRPCServer
import socket
import subprocess 
import sys

# master_ip_address = "-1.-1.-1.-1"


class MyServer(SimpleXMLRPCServer):

    def serve_forever(self):
        self.quit = 0
        while not self.quit:
            self.handle_request()

    # def get_request(self):
    #     """
    #     Get the request and client address from the socket.
    #     """
    #     req = self.socket.accept()
    #     master_ip_address = req[1][0]
    #     return req


def kill():
    server.quit = 1
    return 1


def get_hostname():
    return socket.gethostname()


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


def copy_authorized_key(user, key):
    print("copy_authorized_key to user ", user)
    if user == "root":
        path = "/root"
    else:
        path = "/home/{}".format(user)

    with open("{}/.ssh/authorized_keys".format(path), "a") as ssh_file:
        ssh_file.write(key)


def add_ip(host, master_ip_address):
    print("Adding Master Ip/hostname ({}/{})".format(master_ip_address, host))
    with open("/etc/hosts", "a") as hosts:
        hosts.write("{} {} ".format(master_ip_address, host))


if __name__ == '__main__':
    user_name = sys.argv[1]
    public_key = sys.argv[2]
    master_hostname = sys.argv[3]
    master_ip_address = sys.argv[4]

    add_ip(master_hostname, master_ip_address)
    copy_authorized_key(user_name, public_key)

    server = MyServer((get_ip(), 9942))
    starting = """
    Listening on port 9942...
    Container hostname:{}
    ip:{}""".format(get_hostname(), get_ip())
    print(starting)
    subprocess.run(['service', 'ssh', 'restart'])
    open("docker_container.pid", "w").write(starting)

    server.register_function(kill, "kill")
    server.register_function(get_hostname, "get_hostname")
    server.register_function(get_ip, "get_ip")
    server.register_function(add_ip, "add_master_ip")
    server.serve_forever()
