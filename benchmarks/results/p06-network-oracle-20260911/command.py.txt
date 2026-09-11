import array, fcntl, json, os, pathlib, signal, socket, struct, sys, time
marker, mode = sys.argv[1:]
p = pathlib.Path("p06-shared-name.txt")
p.write_text(marker)
def read(name):
    return pathlib.Path(name).read_text().strip()
def network_facts():
    names = sorted(os.listdir("/sys/class/net"))
    addresses = {name: [] for name in names}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        # Linux SIOCGIFCONF returns every IPv4 address, including aliases.
        stride = 40 if struct.calcsize("P") == 8 else 32
        buffer = array.array("B", b"\0" * 16384)
        result = fcntl.ioctl(control.fileno(), 0x8912, struct.pack("iP", len(buffer), buffer.buffer_info()[0]))
        used = struct.unpack_from("i", result)[0]
        if used < 0 or used % stride or used > len(buffer) - stride:
            raise RuntimeError("incomplete IPv4 interface inventory")
        raw = buffer.tobytes()
        for at in range(0, used, stride):
            name = raw[at:at+16].split(b"\0", 1)[0].decode().split(":", 1)[0]
            if name not in addresses or struct.unpack_from("H", raw, at+16)[0] != socket.AF_INET:
                raise RuntimeError("unexpected IPv4 interface record")
            addresses[name].append(socket.inet_ntoa(raw[at+20:at+24]))
        interfaces = []
        for name in names:
            request = struct.pack("256s", name.encode())
            flags = struct.unpack_from("H", fcntl.ioctl(control.fileno(), 0x8913, request), 16)[0]
            interfaces.append({"name": name, "flags": flags,
                "operstate": read("/sys/class/net/" + name + "/operstate"),
                "ipv4": sorted(set(addresses[name]))})
    ipv4_text, ipv6_text = read("/proc/net/route"), read("/proc/net/ipv6_route")
    ipv4 = []
    for line in ipv4_text.splitlines()[1:]:
        fields = line.split()
        if len(fields) != 11:
            raise RuntimeError("malformed IPv4 route")
        decode = lambda value: socket.inet_ntoa(bytes.fromhex(value)[::-1])
        ipv4.append({"interface": fields[0], "destination": decode(fields[1]),
            "gateway": decode(fields[2]), "mask": decode(fields[7]), "flags": int(fields[3], 16)})
    ipv6 = []
    for line in ipv6_text.splitlines():
        fields = line.split()
        if len(fields) != 10:
            raise RuntimeError("malformed IPv6 route")
        decode = lambda value: socket.inet_ntop(socket.AF_INET6, bytes.fromhex(value))
        ipv6.append({"interface": fields[9], "destination": decode(fields[0]),
            "prefix": int(fields[1], 16), "next_hop": decode(fields[4]), "flags": int(fields[8], 16)})
    return {"interfaces": interfaces, "ipv4_routes": ipv4, "ipv6_routes": ipv6,
        "ipv4_route_table": ipv4_text, "ipv6_route_table": ipv6_text}
status = dict(line.split(":", 1) for line in read("/proc/self/status").splitlines() if ":" in line)
facts = {"marker": marker, "uid": os.getuid(), "gid": os.getgid(),
         "memory_max": read("/sys/fs/cgroup/memory.max"), "pids_max": read("/sys/fs/cgroup/pids.max"),
         "cpu_max": read("/sys/fs/cgroup/cpu.max"), "cap_eff": status["CapEff"].strip(),
         "no_new_privs": status["NoNewPrivs"].strip(), "net_devices": sorted(os.listdir("/sys/class/net")),
         "network": network_facts(),
         "docker_socket": os.path.exists("/var/run/docker.sock"),
         "root_readonly": bool(os.statvfs("/").f_flag & os.ST_RDONLY),
         "tmp_bytes": os.statvfs("/tmp").f_blocks * os.statvfs("/tmp").f_frsize}
print("P06_READY " + json.dumps(facts, sort_keys=True), flush=True)
if mode == "cancel":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = os.fork()
    if child == 0:
        print("P06_CHILD " + marker, flush=True)
        time.sleep(45)
        os._exit(0)
until = time.monotonic() + (45 if mode == "cancel" else 1.0)
checks = 0
while time.monotonic() < until:
    if p.read_text() != marker:
        print("P06_ISOLATION_FAILURE " + marker, flush=True)
        sys.exit(31)
    checks += 1
    time.sleep(0.02)
print("P06_DONE " + json.dumps({"marker": marker, "checks": checks}), flush=True)
