# -*- coding: utf-8 -*-
"""ESTATS per-connection byte counter re-verification script (an experiment to
confirm or refute key-rules #14).

Background: docs/key-rules.md #14 records a 2026-09-18 experiment — Windows'
only public per-connection byte API `Set/GetPerTcpConnectionEStats` returned
ERROR_NOT_SUPPORTED(50), but that experiment's code was never archived: which
ESTATS class was queried, the struct sizes, the Set→Get order, and whether
enabling was ever tried on **another process's connection** (the panel's real
scenario — the target connection belongs to zcode.exe) are all unverifiable.
This script redoes the experiment rigorously and prints a verdict matrix.

Test matrix (all at normal privileges; the panel does not assume admin):
  1. Export symbol resolution: the 4 EStats functions (v4/v6 × Set/Get, note
     the capital S in the export names);
  2. Own v4 connection: Get first (check whether collection is on by default)
     → Set to enable → transfer data → Get again, verify the counters grow
     with the transfer;
  3. Foreign connections (zcode.exe preferred, any non-self ESTAB as a
     control): Get → Set to enable → sample twice at an interval, see whether
     the counters grow (should be visible while a CLI session connection
     streams);
  4. v6 sibling APIs (any v6 ESTAB row).

Verdict rule: if a foreign connection's Get returns NO_ERROR and its byte
counters grow over time → ESTATS is usable (per-process true speed is
feasible); otherwise key-rules #14 is confirmed.

Usage: python scripts/estats_probe.py
"""

import ctypes
import os
import socket
import struct
import subprocess
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

iphlpapi = ctypes.WinDLL("iphlpapi")

TCP_TABLE_OWNER_PID_ALL = 5
AF_INET = 2
AF_INET6 = 23
MIB_TCP_STATE_ESTAB = 5
TCP_ESTATS_DATA = 0  # TcpConnectionEstatsType enum entry 0 = Data (byte/segment counters)
ROD_LEN = 32  # TCP_ESTATS_DATA_ROD_v0 = 4 × ULONG64
RW_LEN = 1  # TCP_ESTATS_DATA_RW_v0 = 1 × BOOLEAN(EnableCollection)

ERR_NAMES = {
    0: "NO_ERROR",
    5: "ERROR_ACCESS_DENIED",
    13: "ERROR_INVALID_DATA",
    50: "ERROR_NOT_SUPPORTED",
    87: "ERROR_INVALID_PARAMETER",
    122: "ERROR_INSUFFICIENT_BUFFER",
    1168: "ERROR_NOT_FOUND",
}


def err_str(rc):
    return ERR_NAMES.get(rc, "code_%d" % rc)


# ============ EStats function pointers (capital S in the export names; direct linking gives LNK2019 — key-rules #108) ============

class MIB_TCPROW(ctypes.Structure):
    _fields_ = [
        ("state", ctypes.c_uint),
        ("local_addr", ctypes.c_uint),
        ("local_port", ctypes.c_uint),
        ("remote_addr", ctypes.c_uint),
        ("remote_port", ctypes.c_uint),
    ]


class MIB_TCP6ROW(ctypes.Structure):
    _fields_ = [
        ("local_addr", ctypes.c_ubyte * 16),
        ("local_scope", ctypes.c_uint),
        ("local_port", ctypes.c_uint),
        ("remote_addr", ctypes.c_ubyte * 16),
        ("remote_scope", ctypes.c_uint),
        ("remote_port", ctypes.c_uint),
        ("state", ctypes.c_uint),
    ]


def resolve_estats_fns():
    """Returns {name: function or None}. ctypes attribute access goes through GetProcAddress; raises AttributeError when missing."""
    names = [
        "SetPerTcpConnectionEStats",
        "GetPerTcpConnectionEStats",
        "SetPerTcp6ConnectionEStats",
        "GetPerTcp6ConnectionEStats",
    ]
    out = {}
    for n in names:
        try:
            fn = getattr(iphlpapi, n)
            fn.restype = ctypes.c_uint
            fn.argtypes = [
                ctypes.c_void_p,  # Row
                ctypes.c_uint,  # EstatsType
                ctypes.POINTER(ctypes.c_ubyte),  # Rw
                ctypes.c_uint,  # RwVersion
                ctypes.c_uint,  # RwSize
                ctypes.POINTER(ctypes.c_ubyte),  # Rod
                ctypes.c_uint,  # RodVersion
                ctypes.c_uint,  # RodSize
            ]
            out[n] = fn
        except AttributeError:
            out[n] = None
    return out


FNS = resolve_estats_fns()


# ============ Connection table enumeration (GetExtendedTcpTable, OWNER_PID_ALL) ============

class MIB_TCPROW_OWNER_PID(ctypes.Structure):
    _fields_ = MIB_TCPROW._fields_ + [("pid", ctypes.c_uint)]


class MIB_TCP6ROW_OWNER_PID(ctypes.Structure):
    _fields_ = MIB_TCP6ROW._fields_ + [("pid", ctypes.c_uint)]


def ipv4_str(v):
    return "%d.%d.%d.%d" % (v & 0xFF, (v >> 8) & 0xFF, (v >> 16) & 0xFF, (v >> 24) & 0xFF)


def port_str(p):
    return "%d" % (((p & 0xFF) << 8) | ((p >> 8) & 0xFF))


def ipv6_str(b):
    return ":".join("%02x%02x" % (b[i * 2], b[i * 2 + 1]) for i in range(8))


def enum_v4():
    size = ctypes.c_uint(0)
    iphlpapi.GetExtendedTcpTable(None, ctypes.byref(size), 0, AF_INET, TCP_TABLE_OWNER_PID_ALL, 0)
    buf = ctypes.create_string_buffer(size.value)
    rc = iphlpapi.GetExtendedTcpTable(buf, ctypes.byref(size), 0, AF_INET, TCP_TABLE_OWNER_PID_ALL, 0)
    if rc != 0:
        print("GetExtendedTcpTable(v4) failed: %s" % err_str(rc))
        return []
    n = struct.unpack_from("<I", buf, 0)[0]
    rows = (MIB_TCPROW_OWNER_PID * n).from_buffer_copy(buf, 4)
    return [
        {
            "row": MIB_TCPROW(
                state=r.state, local_addr=r.local_addr, local_port=r.local_port,
                remote_addr=r.remote_addr, remote_port=r.remote_port),
            "pid": r.pid,
            "remote": "%s:%s" % (ipv4_str(r.remote_addr), port_str(r.remote_port)),
            "local": "%s:%s" % (ipv4_str(r.local_addr), port_str(r.local_port)),
        }
        for r in rows
    ]


def enum_v6():
    size = ctypes.c_uint(0)
    iphlpapi.GetExtendedTcpTable(None, ctypes.byref(size), 0, AF_INET6, TCP_TABLE_OWNER_PID_ALL, 0)
    buf = ctypes.create_string_buffer(size.value)
    rc = iphlpapi.GetExtendedTcpTable(buf, ctypes.byref(size), 0, AF_INET6, TCP_TABLE_OWNER_PID_ALL, 0)
    if rc != 0:
        print("GetExtendedTcpTable(v6) failed: %s" % err_str(rc))
        return []
    n = struct.unpack_from("<I", buf, 0)[0]
    rows = (MIB_TCP6ROW_OWNER_PID * n).from_buffer_copy(buf, 4)
    out = []
    for r in rows:
        # MIB_TCP6ROW is a prefix of the OWNER_PID row; constructed by byte truncation
        row = MIB_TCP6ROW.from_buffer_copy(bytes(r)[: ctypes.sizeof(MIB_TCP6ROW)])
        out.append({
            "row": row,
            "pid": r.pid,
            "remote": "[%s]:%s" % (ipv6_str(r.remote_addr), port_str(r.remote_port)),
        })
    return out


# ============ ESTATS read/write ============

def estats_get_v4(row):
    rod = (ctypes.c_ubyte * ROD_LEN)()
    rc = FNS["GetPerTcpConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                          None, 0, 0, rod, 0, ROD_LEN)
    if rc != 0:
        return rc, None
    bytes_out, segs_out, bytes_in, segs_in = struct.unpack_from("<4Q", rod, 0)
    return rc, (bytes_out, segs_out, bytes_in, segs_in)


def estats_set_v4(row):
    rw = (ctypes.c_ubyte * RW_LEN)(1)  # EnableCollection = TRUE
    return FNS["SetPerTcpConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                            rw, 0, RW_LEN, None, 0, 0)


def estats_get_v6(row):
    rod = (ctypes.c_ubyte * ROD_LEN)()
    rc = FNS["GetPerTcp6ConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                           None, 0, 0, rod, 0, ROD_LEN)
    if rc != 0:
        return rc, None
    return rc, struct.unpack_from("<4Q", rod, 0)


def estats_set_v6(row):
    rw = (ctypes.c_ubyte * RW_LEN)(1)
    return FNS["SetPerTcp6ConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                             rw, 0, RW_LEN, None, 0, 0)


def fmt_counters(c):
    if c is None:
        return "-"
    return "out=%dB/%dseg in=%dB/%dseg" % c


# ============ Test cases ============

def find_zcode_pids():
    try:
        out = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq zcode.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10).stdout
    except Exception as e:
        print("tasklist failed: %s" % e)
        return set()
    pids = set()
    for line in out.splitlines():
        parts = [p.strip('"') for p in line.split('","')]
        if len(parts) >= 2 and parts[0].lower() == "zcode.exe" and parts[1].isdigit():
            pids.add(int(parts[1]))
    return pids


HTTP_HOSTS = ["www.baidu.com", "www.qq.com", "mirrors.aliyun.com"]


def test_own_v4():
    """Own connection: Get (disabled) → Set to enable → transfer → Get, verify counters grow"""
    print("\n== Test 2: own v4 connection ==")
    sock = None
    host = None
    for h in HTTP_HOSTS:
        try:
            sock = socket.create_connection((h, 80), timeout=10)
            host = h
            break
        except OSError:
            continue
    if sock is None:
        print("Skip: no usable HTTP endpoint")
        return None
    try:
        local_ip, local_port = sock.getsockname()
        want_addr = int.from_bytes(socket.inet_aton(local_ip), "little")
        want_port = ((local_port & 0xFF) << 8) | ((local_port >> 8) & 0xFF)
        row = None
        for r in enum_v4():
            if r["pid"] == os.getpid() and r["row"].local_addr == want_addr and r["row"].local_port == want_port:
                row = r["row"]
                break
        if row is None:
            print("Own row not found in the connection table (%s:%d)" % (local_ip, local_port))
            return None

        rc1, c1 = estats_get_v4(row)
        print("  Get (disabled, checks default collection): %s  %s" % (err_str(rc1), fmt_counters(c1)))
        rc2 = estats_set_v4(row)
        print("  Set (enable Data collection)       : %s" % err_str(rc2))
        # Call-shape variants: rule out misjudgment caused by argument forms
        rwq = (ctypes.c_ubyte * RW_LEN)()
        rodq = (ctypes.c_ubyte * ROD_LEN)()
        rcA = FNS["GetPerTcpConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                               rwq, 0, RW_LEN, rodq, 0, ROD_LEN)
        print("  Variant A: Get with Rw buffer : %s  %s" % (err_str(rcA), fmt_counters(
            struct.unpack_from("<4Q", rodq, 0) if rcA == 0 else None)))
        rod_set = (ctypes.c_ubyte * ROD_LEN)()
        rcB = FNS["SetPerTcpConnectionEStats"](ctypes.byref(row), TCP_ESTATS_DATA,
                                               (ctypes.c_ubyte * RW_LEN)(1), 0, RW_LEN,
                                               rod_set, 0, ROD_LEN)
        print("  Variant B: Set with Rod buffer: %s" % err_str(rcB))
        rc3, c3 = estats_get_v4(row)
        print("  Get (after enabling)               : %s  %s" % (err_str(rc3), fmt_counters(c3)))

        total = 0
        req = ("GET / HTTP/1.1\r\nHost: %s\r\nUser-Agent: estats-probe\r\n"
               "Connection: close\r\n\r\n" % host).encode()
        sock.sendall(req)
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            total += len(chunk)
        print("  Transfer done: HTTP downloaded %d bytes" % total)

        rc4, c4 = estats_get_v4(row)
        print("  Get (after transfer)               : %s  %s" % (err_str(rc4), fmt_counters(c4)))
        if c4 is not None and c3 is not None:
            print("  Delta: in +%dB / out +%dB" % (c4[2] - c3[2], c4[0] - c3[0]))
        ok = rc3 == 0 and c3 is not None and rc4 == 0 and c4 is not None and (c4[2] > c3[2] or c4[0] > c3[0])
        print("  => Own connection verdict: %s" % ("usable (counters grow with transfer)" if ok else "unusable"))
        return ok
    finally:
        sock.close()


def probe_foreign_row_v4(tag, row, remote, sample_s=4):
    """Foreign connection triple: Get → Set → two interval samples to check growth"""
    rc1, c1 = estats_get_v4(row)
    print("  [%s] %s" % (tag, remote))
    print("    Get (disabled): %s  %s" % (err_str(rc1), fmt_counters(c1)))
    rc2 = estats_set_v4(row)
    print("    Set (enable)  : %s" % err_str(rc2))
    rc3, c3 = estats_get_v4(row)
    print("    Get (t0)      : %s  %s" % (err_str(rc3), fmt_counters(c3)))
    time.sleep(sample_s)
    rc4, c4 = estats_get_v4(row)
    print("    Get (t+%ds)   : %s  %s" % (sample_s, err_str(rc4), fmt_counters(c4)))
    grew = c3 is not None and c4 is not None and (c4[2] > c3[2] or c4[0] > c3[0])
    if grew:
        print("    Delta: in +%dB / out +%dB  ** counters are growing **" % (c4[2] - c3[2], c4[0] - c3[0]))
    return {"get": rc3, "set": rc2, "grew": grew, "remote": remote}


def test_foreign_v4():
    print("\n== Test 3: foreign connections (the panel's real scenario) ==")
    estab = [r for r in enum_v4() if r["pid"] != os.getpid() and r["row"].state == MIB_TCP_STATE_ESTAB]
    if not estab:
        print("Skip: no foreign ESTABLISHED connections")
        return None
    zcode = find_zcode_pids()
    print("zcode.exe pid count: %d" % len(zcode))
    zrows = [r for r in estab if r["pid"] in zcode][:5]
    results = []
    for r in zrows:
        results.append(probe_foreign_row_v4("zcode pid=%d" % r["pid"], r["row"], r["remote"]))
    other = next((r for r in estab if r["pid"] not in zcode), None)
    if other is not None:
        results.append(probe_foreign_row_v4("control pid=%d" % other["pid"], other["row"], other["remote"]))
    return results


def test_v6():
    print("\n== Test 4: v6 sibling APIs ==")
    if FNS["SetPerTcp6ConnectionEStats"] is None or FNS["GetPerTcp6ConnectionEStats"] is None:
        print("Skip: v6 export symbols missing")
        return None
    estab = [r for r in enum_v6() if r["row"].state == MIB_TCP_STATE_ESTAB]
    if not estab:
        print("Skip: no v6 ESTABLISHED connections")
        return None
    r = estab[0]
    print("  [%s] pid=%d" % (r["remote"], r["pid"]))
    rc1 = estats_set_v6(r["row"])
    print("    Set (enable)  : %s" % err_str(rc1))
    rc2, c2 = estats_get_v6(r["row"])
    print("    Get          : %s  %s" % (err_str(rc2), fmt_counters(c2)))
    return {"set": rc1, "get": rc2}


def main():
    win = sys.getwindowsversion()
    print("ESTATS re-verification script (key-rules #14)")
    print("Windows: %s (build %d)  process pid=%d  admin=%s" % (
        win.platform_version if hasattr(win, "platform_version") else str(win), win.build,
        os.getpid(), bool(ctypes.windll.shell32.IsUserAnAdmin())))

    print("\n== Test 1: export symbol resolution ==")
    for n, fn in FNS.items():
        print("  %-28s %s" % (n, "resolved" if fn else "missing!"))

    own_ok = test_own_v4()
    foreign = test_foreign_v4()
    test_v6()

    print("\n== Verdict summary ==")
    foreign_ok = None
    if foreign:
        for r in foreign:
            print("  Foreign connection %s: Set=%s Get=%s grew=%s" % (
                r["remote"], err_str(r["set"]), err_str(r["get"]), "yes" if r["grew"] else "no"))
        foreign_ok = any(r["set"] == 0 and r["get"] == 0 for r in foreign)
        grew = any(r["grew"] for r in foreign)
        print("  Own connection: %s" % ("usable" if own_ok else "unusable"))
        verdict = "feasible: per-process true speed can be implemented" if (foreign_ok and grew) else (
            "partially feasible (foreign connections are readable/writable but no growth seen; cross-check with traffic)" if foreign_ok else "infeasible: key-rules #14 confirmed")
        print("  Final verdict: %s" % verdict)


if __name__ == "__main__":
    main()
