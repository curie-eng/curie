"""Verify private implementation listeners against every Linux task address."""

from __future__ import annotations

import asyncio
import ctypes
import ipaddress
import logging
import socket


class _IfAddrs(ctypes.Structure):
    pass


_IfAddrs._fields_ = [
    ("next", ctypes.POINTER(_IfAddrs)),
    ("name", ctypes.c_char_p),
    ("flags", ctypes.c_uint),
    ("address", ctypes.c_void_p),
    ("netmask", ctypes.c_void_p),
    ("broadcast", ctypes.c_void_p),
    ("data", ctypes.c_void_p),
]


def non_loopback_addresses() -> tuple[str, ...]:
    """Enumerate all IPv4/IPv6 interface addresses, including secondary addresses."""

    libc = ctypes.CDLL(None, use_errno=True)
    libc.getifaddrs.argtypes = [ctypes.POINTER(ctypes.POINTER(_IfAddrs))]
    libc.getifaddrs.restype = ctypes.c_int
    libc.freeifaddrs.argtypes = [ctypes.POINTER(_IfAddrs)]
    head = ctypes.POINTER(_IfAddrs)()
    if libc.getifaddrs(ctypes.byref(head)) != 0:
        raise OSError(ctypes.get_errno(), "cannot enumerate task network addresses")
    addresses: set[str] = set()
    try:
        item = head
        while item:
            pointer = item.contents.address
            if pointer:
                family = ctypes.cast(pointer, ctypes.POINTER(ctypes.c_ushort)).contents.value
                if family in {socket.AF_INET, socket.AF_INET6}:
                    offset, size = (4, 4) if family == socket.AF_INET else (8, 16)
                    address = ipaddress.ip_address(ctypes.string_at(pointer + offset, size))
                    if not address.is_loopback and not address.is_unspecified:
                        text = str(address)
                        if address.version == 6 and address.is_link_local:
                            scope = ctypes.c_uint32.from_address(pointer + 24).value
                            text += f"%{scope}"
                        addresses.add(text)
            item = item.contents.next
    finally:
        libc.freeifaddrs(head)
    return tuple(sorted(addresses))


async def reachable(host: str, port: int) -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1)
    except ConnectionRefusedError:
        return False
    writer.close()
    await writer.wait_closed()
    return True


async def require_loopback_only(ports: tuple[int, ...]) -> None:
    for host in non_loopback_addresses():
        for port in ports:
            if await reachable(host, port):
                logging.getLogger("curie_connector_proxy").error(
                    "implementation listener is reachable outside task loopback"
                )
                raise ValueError("implementation listener must bind only task loopback")
