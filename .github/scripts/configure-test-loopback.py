#!/usr/bin/env python3
"""Set the private Linux CI test container's loopback MTU before Redis starts."""
import fcntl
import socket
import struct

# MOD-17216: a loopback MSS near 64 KiB can exceed a reduced receive window,
# causing replies to advance only on the TCP probe timer and miss query deadlines.
# Called only by flow-linux.yml in an explicitly private bridge network namespace.
SIOCGIFMTU = 0x8921
SIOCSIFMTU = 0x8922
MTU = 1500
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    request = struct.pack("16si20x", b"lo", 0)
    before = struct.unpack_from("i", fcntl.ioctl(sock, SIOCGIFMTU, request), 16)[0]
    fcntl.ioctl(sock, SIOCSIFMTU, struct.pack("16si20x", b"lo", MTU))
    after = struct.unpack_from("i", fcntl.ioctl(sock, SIOCGIFMTU, request), 16)[0]
    if after != MTU:
        raise RuntimeError(f"Loopback MTU is {after}, expected {MTU}")
    print(f"CI test loopback MTU: {before} -> {after}", flush=True)
