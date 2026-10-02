//! Network speed monitoring: system-wide speed / daily totals (real) + session traffic estimate breakdown.
//!
//! ## Layered methodology (why per-process network bytes cannot be measured directly)
//!
//! 2026-09-18 local experiments (see docs/key-rules.md #15):
//! - **Winsock send/receive bytes never enter any `GetProcessIoCounters` counter**
//!   (during a 20MB download, Read was only 7.8KB, and the 12MB of Write was the
//!   on-disk mirror) — process IO counters cover only files/pipes/devices, so
//!   liveio's streaming speed measurement is naturally free of network pollution;
//! - **TCP ESTATS (`SetPerTcpConnectionEStats`) is broken**: it returns
//!   ERROR_NOT_SUPPORTED for every connection (including this process's own),
//!   even when running as administrator;
//! - ETW kernel network events require administrator.
//!
//! => Without admin, neither platform exposes a public primitive for
//! "per-process network send/receive bytes". This module is honestly layered
//! in two tiers (a third tier, "real lower bound from snapshot artifacts",
//! once existed and was removed along with ZCode's retired snapshot feature):
//!
//! 1. **System-wide upload/download (real values)**: sum of interface counters
//!    (Windows `GetIfTable` 32-bit octets with modular delta; macOS
//!    `getifaddrs` ifi_*bytes), loopback excluded in both. Speed = ~1s
//!    sliding-window delta (aligned with Task Manager's ~1s refresh cadence);
//!    daily totals persist across restarts (`speed-panel-net.json`).
//!    Note: if the machine goes through a local proxy (ZCode -> 127.0.0.1 proxy
//!    process -> internet), the system-wide figure includes proxy tunnel
//!    encryption overhead and mixes in other apps' traffic.
//! 2. **Session traffic (estimated ~=)**: usage DB token counts x byte
//!    coefficient (API conversation traffic carried by the CLI process;
//!    request body ~= input x 5 B/token, streaming response ~= output x 8
//!    B/token — order-of-magnitude reference values, marked with ~= on the
//!    frontend).
//! 3. **ZCode connection attribution (real values, Windows only)**: the TCP
//!    connection table (OWNER_PID) grouped by process — CLI processes whose
//!    command line contains `zcode.cjs` = session group (API traffic); other
//!    `zcode.exe` processes (Electron desktop main/renderer/utility processes)
//!    = non-session group (telemetry and other non-conversation traffic).
//!    Each group shows its ESTABLISHED connection count and remotes.

use chrono::{Datelike, Local};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// System-wide speed sliding window (aligned with Task Manager's ~1s refresh
/// cadence; short windows read jumpier than long ones — expected)
const NET_WINDOW_MS: i64 = 1_000;
/// System-wide sampling ring capacity (~4.5min @700ms)
const NET_RING_CAP: usize = 400;
/// Process-group refresh period (Toolhelp + command line reads; not every tick)
const PROC_REFRESH_EVERY: Duration = Duration::from_secs(5);
/// Daily-total disk-save throttle (a forced save also happens on exit)
const NET_SAVE_EVERY: Duration = Duration::from_secs(30);


/// Session upload estimate coefficient (bytes/token): the request body is the
/// JSON-escaped **uncached** prompt delta (measured: with 98% cache hits,
/// system-wide upload was only tens of KB — cached prompt parts are not
/// resent), English/code ~4 chars/token plus escaping overhead, hence 5.
/// Order-of-magnitude reference value (the frontend marks it with ~=)
pub const SESS_UP_BPT: f64 = 5.0;
/// Session download estimate coefficient: SSE event stream density.
/// 2026-09-18 calibration: during streaming, system-wide download / tokens
/// ~= 731 B/token (upper bound, includes other apps' traffic); UI pipeline
/// coefficient bpt~=320 (lower bound); 400 chosen in between.
/// Order-of-magnitude reference value
pub const SESS_DOWN_BPT: f64 = 400.0;

/// Session traffic estimate (pure function). The upload numerator uses
/// **uncached prompt** tokens (input already includes cache-hit tokens, and
/// cache hits are not resent — estimating from the full prompt would inflate
/// the result tens of times, as evidenced by measured daily system-wide
/// upload of only tens of KB); output = output + thinking tokens
pub fn sess_bytes_est(uncached_input_tokens: u64, output_tokens: u64) -> (u64, u64) {
    (
        (uncached_input_tokens as f64 * SESS_UP_BPT) as u64,
        (output_tokens as f64 * SESS_DOWN_BPT) as u64,
    )
}

/// Interface counter delta (pure function, testable): when wrap>0, a
/// wraparound delta modulo the wrap value (Windows 32-bit octets); when
/// wrap=0, a plain delta (mac 64-bit) with regressions clamped to 0
/// (counter resets). A per-interface per-tick delta above 2^31 is treated
/// as anomalous (reset/index reuse) and clamped to 0 to prevent phantom traffic
pub(crate) fn wrap_delta(new: u64, old: u64, wrap: u64) -> u64 {
    let d = if wrap > 0 {
        ((new as i64 - old as i64).rem_euclid(wrap as i64)) as u64
    } else {
        new.saturating_sub(old)
    };
    if d >= (1u64 << 31) {
        0
    } else {
        d
    }
}

/// Network monitoring snapshot produced every tick (build_payload puts it into Snapshot for the frontend)
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetNow {
    /// Whether system-wide interface counters are available (false on stub platforms)
    pub available: bool,
    pub up_bps: f64,
    pub down_bps: f64,
    /// System-wide daily totals (real; persisted and continued across restarts)
    pub up_today: u64,
    pub down_today: u64,
    /// Whether connection attribution is available (Windows only)
    pub conns_available: bool,
    /// Deduplicated ESTABLISHED remote count of the session group (CLI processes)
    pub cli_conns: u32,
    /// Deduplicated remote count of the non-session group (Electron desktop processes)
    pub app_conns: u32,
    /// Connection details of both groups (remote + owning pid + process type
    /// label, deduplicated and sorted by remote+pid; the tooltip lists every
    /// entry showing "which process connected where")
    pub cli_conn_list: Vec<crate::metrics::ConnStat>,
    pub app_conn_list: Vec<crate::metrics::ConnStat>,
}

// ============ Platform primitives (three variants: win / mac / stub, unified externally as netio::platform::*) ============

pub mod platform {
    /// Windows: GetIfTable sums interface octets (32-bit counters; the caller
    /// applies the modular delta); GetExtendedTcpTable (OWNER_PID) enumerates
    /// v4+v6 connections grouped by process; Toolhelp + PEB command line
    /// distinguish CLI (zcode.cjs) from the Electron desktop app.
    /// Process/command line identification matches liveio::platform::win (two
    /// independent implementations: liveio only discovers CLI processes, while
    /// this one also needs "the other zcode.exe" for the non-session group)
    #[cfg(windows)]
    mod win {
        use std::collections::{HashMap, HashSet};
        use std::ffi::c_void;

        #[link(name = "kernel32")]
        extern "system" {
            fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
            fn CloseHandle(h: *mut c_void) -> i32;
            fn ReadProcessMemory(
                h: *mut c_void,
                addr: *const c_void,
                buf: *mut c_void,
                size: usize,
                read: *mut usize,
            ) -> i32;
            fn CreateToolhelp32Snapshot(flags: u32, pid: u32) -> isize;
            fn Process32FirstW(snap: isize, entry: *mut ProcessEntry32W) -> i32;
            fn Process32NextW(snap: isize, entry: *mut ProcessEntry32W) -> i32;
        }
        #[link(name = "iphlpapi")]
        extern "system" {
            fn GetIfTable(table: *mut c_void, size: *mut u32, order: i32) -> u32;
            fn GetExtendedTcpTable(
                table: *mut c_void,
                size: *mut u32,
                order: i32,
                family: u32,
                class: u32,
                reserved: u32,
            ) -> u32;
        }
        #[link(name = "ntdll")]
        extern "system" {
            fn NtQueryInformationProcess(
                h: *mut c_void,
                class: u32,
                info: *mut c_void,
                len: u32,
                ret_len: *mut u32,
            ) -> i32;
        }

        #[repr(C)]
        struct ProcessEntry32W {
            size: u32,
            usage: u32,
            process_id: u32,
            default_heap_id: usize,
            module_id: u32,
            threads: u32,
            parent_process_id: u32,
            pri_class_base: i32,
            flags: u32,
            exe_file: [u16; 260],
        }

        const PROCESS_QUERY_LIMITED: u32 = 0x1410;
        const TH32CS_SNAPPROCESS: u32 = 2;

        /// MIB_IFROW mirror (layout unchanged since NT4; key field offsets
        /// pinned at compile time). Only dwType/dwInOctets/dwOutOctets are
        /// used, but the table layout requires the full sizeof
        #[repr(C)]
        struct MibIfRow {
            wsz_name: [u16; 256],
            dw_index: u32,
            dw_type: u32,
            dw_mtu: u32,
            dw_speed: u32,
            dw_phys_addr_len: u32,
            b_phys_addr: [u8; 8],
            dw_admin_status: u32,
            dw_oper_status: u32,
            dw_last_change: u32,
            dw_in_octets: u32,
            dw_out_octets: u32,
            dw_in_ucast_pkts: u32,
            dw_in_nucast_pkts: u32,
            dw_in_discards: u32,
            dw_in_errors: u32,
            dw_in_unknown_protos: u32,
            dw_out_ucast_pkts: u32,
            dw_out_nucast_pkts: u32,
            dw_out_discards: u32,
            dw_out_errors: u32,
            dw_out_qlen: u32,
            dw_descr_len: u32,
            b_descr: [u8; 256],
        }

        const _: () = {
            assert!(std::mem::offset_of!(MibIfRow, dw_type) == 516);
            assert!(std::mem::offset_of!(MibIfRow, dw_in_octets) == 552);
            assert!(std::mem::offset_of!(MibIfRow, dw_out_octets) == 556);
            assert!(std::mem::size_of::<MibIfRow>() == 860);
        };

        /// IF_TYPE_SOFTWARE_LOOPBACK
        const IF_TYPE_LOOPBACK: u32 = 24;

        /// Interface counter wraparound modulus: dwIn/dwOutOctets are 32-bit; per-interface delta modulo 2^32
        pub const NET_COUNTER_WRAP: u64 = 1 << 32;

        /// Counter rows of all non-loopback interfaces: (interface index,
        /// total uploaded, total downloaded). Wraparound correction is done
        /// by the caller with per-interface deltas (each interface wraps at a
        /// different time; summing first and differencing later would be wrong)
        pub fn net_ifaces() -> Option<Vec<(String, u64, u64)>> {
            unsafe {
                let mut size = 0u32;
                if GetIfTable(std::ptr::null_mut(), &mut size, 0) != 122 || size == 0 {
                    return None;
                }
                let mut buf = vec![0u8; size as usize];
                if GetIfTable(buf.as_mut_ptr().cast(), &mut size, 0) != 0 {
                    return None;
                }
                let n = buf.as_ptr().cast::<u32>().read_unaligned() as usize;
                let rows = buf.as_ptr().add(4).cast::<MibIfRow>();
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let r = rows.add(i).read_unaligned();
                    if r.dw_type == IF_TYPE_LOOPBACK {
                        continue;
                    }
                    out.push((r.dw_index.to_string(), r.dw_out_octets as u64, r.dw_in_octets as u64));
                }
                Some(out)
            }
        }

        #[repr(C)]
        #[derive(Clone, Copy)]
        struct TcpRow {
            state: u32,
            local_addr: u32,
            local_port: u32,
            remote_addr: u32,
            remote_port: u32,
            pid: u32,
        }

        #[repr(C)]
        #[derive(Clone, Copy)]
        struct Tcp6Row {
            state: u32,
            local_addr: [u8; 16],
            local_scope: u32,
            local_port: u32,
            remote_addr: [u8; 16],
            remote_scope: u32,
            remote_port: u32,
            pid: u32,
        }

        const TCP_TABLE_OWNER_PID_ALL: u32 = 5;
        const AF_INET: u32 = 2;
        const AF_INET6: u32 = 23;
        const MIB_TCP_STATE_ESTAB: u32 = 5;

        fn port(p: u32) -> u16 {
            ((p & 0xff) << 8 | (p >> 8) & 0xff) as u16
        }

        fn ipv4(v: u32) -> String {
            format!("{}.{}.{}.{}", v & 0xff, (v >> 8) & 0xff, (v >> 16) & 0xff, (v >> 24) & 0xff)
        }

        /// Uncompressed IPv6 text (::ffff: mapped addresses are also shown in full form — tooltip only)
        fn ipv6(b: &[u8; 16]) -> String {
            let mut s = String::new();
            for i in 0..8 {
                if i > 0 {
                    s.push(':');
                }
                s.push_str(&format!("{:02x}{:02x}", b[i * 2], b[i * 2 + 1]));
            }
            s
        }

        /// Command line -> process type label (the Electron shell's --type
        /// argument distinguishes child processes; the CLI check comes first
        /// because zcode.cjs never appears in a renderer's command line)
        pub(crate) fn proc_label(cmd: &str) -> &'static str {
            if cmd.contains("zcode.cjs") {
                "CLI session process"
            } else if cmd.contains("crashpad") {
                "crash reporter process"
            } else if cmd.contains("--type=renderer") {
                "renderer process"
            } else if cmd.contains("--type=gpu-process") {
                "GPU process"
            } else if cmd.contains("--type=utility") {
                "utility process"
            } else {
                "main process"
            }
        }

        /// ESTABLISHED connections of ZCode-related processes (remote,
        /// owning pid), returned as two groups (cli_pids, app_pids), each
        /// deduplicated and sorted by remote+pid. Returns None when the
        /// connection table cannot be read (connection attribution unavailable)
        pub fn zcode_conns(
            cli_pids: &HashMap<u32, String>,
            app_pids: &HashMap<u32, String>,
        ) -> Option<(Vec<(String, u32)>, Vec<(String, u32)>)> {
            let mut cli: HashSet<(String, u32)> = HashSet::new();
            let mut app: HashSet<(String, u32)> = HashSet::new();
            unsafe {
                let mut size = 0u32;
                GetExtendedTcpTable(std::ptr::null_mut(), &mut size, 0, AF_INET, TCP_TABLE_OWNER_PID_ALL, 0);
                if size > 0 {
                    let mut buf = vec![0u8; size as usize];
                    if GetExtendedTcpTable(buf.as_mut_ptr().cast(), &mut size, 0, AF_INET, TCP_TABLE_OWNER_PID_ALL, 0) == 0 {
                        let n = buf.as_ptr().cast::<u32>().read_unaligned() as usize;
                        let rows = buf.as_ptr().add(4).cast::<TcpRow>();
                        for i in 0..n {
                            let r = rows.add(i).read_unaligned();
                            if r.state != MIB_TCP_STATE_ESTAB {
                                continue;
                            }
                            let target = if cli_pids.contains_key(&r.pid) {
                                &mut cli
                            } else if app_pids.contains_key(&r.pid) {
                                &mut app
                            } else {
                                continue;
                            };
                            target.insert((format!("{}:{}", ipv4(r.remote_addr), port(r.remote_port)), r.pid));
                        }
                    }
                }
                let mut size6 = 0u32;
                GetExtendedTcpTable(std::ptr::null_mut(), &mut size6, 0, AF_INET6, TCP_TABLE_OWNER_PID_ALL, 0);
                if size6 > 0 {
                    let mut buf = vec![0u8; size6 as usize];
                    if GetExtendedTcpTable(buf.as_mut_ptr().cast(), &mut size6, 0, AF_INET6, TCP_TABLE_OWNER_PID_ALL, 0) == 0 {
                        let n = buf.as_ptr().cast::<u32>().read_unaligned() as usize;
                        let rows = buf.as_ptr().add(4).cast::<Tcp6Row>();
                        for i in 0..n {
                            let r = rows.add(i).read_unaligned();
                            if r.state != MIB_TCP_STATE_ESTAB {
                                continue;
                            }
                            let target = if cli_pids.contains_key(&r.pid) {
                                &mut cli
                            } else if app_pids.contains_key(&r.pid) {
                                &mut app
                            } else {
                                continue;
                            };
                            target.insert((format!("[{}]:{}", ipv6(&r.remote_addr), port(r.remote_port)), r.pid));
                        }
                    }
                }
            }
            let sort = |s: HashSet<(String, u32)>| {
                let mut v: Vec<(String, u32)> = s.into_iter().collect();
                v.sort();
                v
            };
            Some((sort(cli), sort(app)))
        }

        /// The same PEB -> ProcessParameters -> CommandLine(UNICODE_STRING
        /// @ 0x70) read chain as liveio::platform::win
        fn process_command_line(pid: u32) -> Option<String> {
            unsafe {
                let h = OpenProcess(PROCESS_QUERY_LIMITED, 0, pid);
                if h.is_null() {
                    return None;
                }
                let rd = |addr: usize, buf: &mut [u8]| -> bool {
                    let mut n = 0usize;
                    ReadProcessMemory(h, addr as *const c_void, buf.as_mut_ptr().cast(), buf.len(), &mut n) != 0
                };
                let read_cmdline = || -> Option<String> {
                    let mut pbi = [0u8; 48];
                    let mut ret: u32 = 0;
                    if NtQueryInformationProcess(h, 0, pbi.as_mut_ptr().cast(), 48, &mut ret) != 0 {
                        return None;
                    }
                    #[cfg(target_pointer_width = "64")]
                    {
                        let peb = usize::from_ne_bytes(pbi[8..16].try_into().ok()?);
                        if peb == 0 {
                            return None;
                        }
                        let mut pp_ptr = [0u8; 8];
                        if !rd(peb + 0x20, &mut pp_ptr) {
                            return None;
                        }
                        let pp = usize::from_ne_bytes(pp_ptr.try_into().ok()?);
                        if pp == 0 {
                            return None;
                        }
                        let mut us = [0u8; 16];
                        if !rd(pp + 0x70, &mut us) {
                            return None;
                        }
                        let len = u16::from_ne_bytes([us[0], us[1]]) as usize;
                        let buf_ptr = usize::from_ne_bytes(us[8..16].try_into().ok()?);
                        if len == 0 || buf_ptr == 0 {
                            return None;
                        }
                        let mut wbuf = vec![0u8; len];
                        if !rd(buf_ptr, &mut wbuf) {
                            return None;
                        }
                        let u16s: Vec<u16> = wbuf
                            .chunks_exact(2)
                            .map(|c| u16::from_ne_bytes([c[0], c[1]]))
                            .collect();
                        Some(String::from_utf16_lossy(&u16s))
                    }
                    #[cfg(not(target_pointer_width = "64"))]
                    {
                        None
                    }
                };
                let out = read_cmdline();
                CloseHandle(h);
                out
            }
        }

        /// Discovers ZCode processes and groups them (pid + process type
        /// label): (CLI processes = session group, other zcode.exe = desktop
        /// group). CLI = exe name zcode.exe (case-insensitive) with a command
        /// line containing zcode.cjs; a zcode.exe without zcode.cjs is the
        /// Electron desktop app (main/renderer/GPU/utility processes — the
        /// carriers of telemetry and other non-session traffic). Both groups
        /// are ZCode's own processes; no other apps are included
        pub fn zcode_pid_groups() -> (Vec<(u32, String)>, Vec<(u32, String)>) {
            let mut cli = Vec::new();
            let mut app = Vec::new();
            unsafe {
                let snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
                if snap == -1 {
                    return (cli, app);
                }
                let mut entry = ProcessEntry32W {
                    size: std::mem::size_of::<ProcessEntry32W>() as u32,
                    usage: 0,
                    process_id: 0,
                    default_heap_id: 0,
                    module_id: 0,
                    threads: 0,
                    parent_process_id: 0,
                    pri_class_base: 0,
                    flags: 0,
                    exe_file: [0; 260],
                };
                if Process32FirstW(snap, &mut entry) != 0 {
                    loop {
                        let exe = String::from_utf16_lossy(
                            &entry.exe_file[..entry.exe_file.iter().position(|c| *c == 0).unwrap_or(260)],
                        );
                        if exe.eq_ignore_ascii_case("zcode.exe") {
                            match process_command_line(entry.process_id) {
                                Some(cmd) => {
                                    let label = proc_label(&cmd).to_string();
                                    if cmd.contains("zcode.cjs") {
                                        cli.push((entry.process_id, label));
                                    } else {
                                        app.push((entry.process_id, label));
                                    }
                                }
                                None => {} // command line unreadable (permissions/race): counted in neither group
                            }
                        }
                        if Process32NextW(snap, &mut entry) == 0 {
                            break;
                        }
                    }
                }
                CloseHandle(snap as *mut c_void);
            }
            (cli, app)
        }
    }

    /// macOS: getifaddrs sums interface ifi_obytes/ifi_ibytes (64-bit, lo0
    /// excluded). Connection attribution (grouping TCP connections by
    /// process) is not implemented on mac — the benefit is concentrated in
    /// the Windows desktop app (separating session/desktop process
    /// connections); the mac panel honestly shows "connection details are
    /// Windows-only"
    #[cfg(target_os = "macos")]
    mod mac {
        use std::collections::{HashMap, HashSet};
        use std::ffi::{c_char, c_int, c_void};

        #[link(name = "System")]
        extern "C" {
            fn getifaddrs(ptr: *mut *mut IfAddrs) -> c_int;
            fn freeifaddrs(ptr: *mut IfAddrs);
        }

        /// struct ifaddrs mirror (flags is 4 bytes; padding follows so the pointers after it are 8-byte aligned)
        #[repr(C)]
        struct IfAddrs {
            next: *mut IfAddrs,
            name: *const c_char,
            flags: u32,
            pad: u32,
            addr: *mut c_void,
            netmask: *mut c_void,
            dstaddr: *mut c_void,
            data: *mut c_void,
            spare: *mut c_void,
        }

        /// struct if_data64 (macOS 64-bit) mirror: ifi_ibytes=64 / ifi_obytes=72
        /// cross-checked against the xnu SDK net/if.h and pinned by asserts;
        /// an SDK layout change fails the build outright — do not delete the asserts
        #[repr(C)]
        struct IfData64 {
            ifi_type: u8,
            ifi_typelen: u8,
            ifi_physical: u8,
            ifi_addrlen: u8,
            ifi_hdrlen: u8,
            ifi_recvquota: u8,
            ifi_xmitquota: u8,
            ifi_unused1: u8,
            ifi_mtu: u32,
            ifi_metric: u32,
            ifi_baudrate: u64,
            ifi_ipackets: u64,
            ifi_ierrors: u64,
            ifi_opackets: u64,
            ifi_oerrors: u64,
            ifi_collisions: u64,
            ifi_ibytes: u64,
            ifi_obytes: u64,
        }

        const _: () = {
            assert!(std::mem::offset_of!(IfData64, ifi_ibytes) == 64);
            assert!(std::mem::offset_of!(IfData64, ifi_obytes) == 72);
        };

        /// Interface counter wraparound modulus: ifi_*bytes are 64-bit and never wrap in practice (0 = plain delta)
        pub const NET_COUNTER_WRAP: u64 = 0;

        /// Counter rows of all non-loopback interfaces: (interface name,
        /// total uploaded, total downloaded). getifaddrs returns multiple
        /// rows per interface (one per address family), so they must be
        /// deduplicated by interface name (otherwise bytes double);
        /// loopback lo0 excluded
        pub fn net_ifaces() -> Option<Vec<(String, u64, u64)>> {
            unsafe {
                let mut head: *mut IfAddrs = std::ptr::null_mut();
                if getifaddrs(&mut head) != 0 {
                    return None;
                }
                let mut seen: HashSet<String> = HashSet::new();
                let mut out = Vec::new();
                let mut p = head;
                while !p.is_null() {
                    let ifa = &*p;
                    if !ifa.name.is_null() && !ifa.data.is_null() {
                        let name = std::ffi::CStr::from_ptr(ifa.name).to_string_lossy().into_owned();
                        if name != "lo0" && seen.insert(name.clone()) {
                            let d = &*(ifa.data as *const IfData64);
                            out.push((name, d.ifi_obytes, d.ifi_ibytes));
                        }
                    }
                    p = ifa.next;
                }
                freeifaddrs(head);
                Some(out)
            }
        }

        /// Connection attribution is implemented on Windows only; mac returns None (the panel shows "unavailable")
        pub fn zcode_conns(
            _cli_pids: &HashMap<u32, String>,
            _app_pids: &HashMap<u32, String>,
        ) -> Option<(Vec<(String, u32)>, Vec<(String, u32)>)> {
            None
        }

        pub fn zcode_pid_groups() -> (Vec<(u32, String)>, Vec<(u32, String)>) {
            (Vec::new(), Vec::new())
        }
    }

    /// Other platforms: neither interface counters nor connection attribution is available (the panel shows "not supported")
    #[cfg(not(any(windows, target_os = "macos")))]
    mod stub {
        use std::collections::HashMap;

        pub const NET_COUNTER_WRAP: u64 = 0;

        pub fn net_ifaces() -> Option<Vec<(String, u64, u64)>> {
            None
        }
        pub fn zcode_conns(
            _cli: &HashMap<u32, String>,
            _app: &HashMap<u32, String>,
        ) -> Option<(Vec<(String, u32)>, Vec<(String, u32)>)> {
            None
        }
        pub fn zcode_pid_groups() -> (Vec<(u32, String)>, Vec<(u32, String)>) {
            (Vec::new(), Vec::new())
        }
    }

    #[cfg(windows)]
    pub use win::*;
    #[cfg(target_os = "macos")]
    pub use mac::*;
    #[cfg(not(any(windows, target_os = "macos")))]
    pub use stub::*;
}

// ============ Main state machine ============

fn local_ymd() -> (i32, u32, u32) {
    let n = Local::now();
    (n.year(), n.month(), n.day())
}

fn ymd_str((y, m, d): (i32, u32, u32)) -> String {
    format!("{y:04}-{m:02}-{d:02}")
}

fn net_file() -> Option<std::path::PathBuf> {
    crate::metrics::home_dir().map(|h| h.join(".zcode").join("speed-panel-net.json"))
}

pub struct NetIo {
    /// (time ms, dewrapped system-wide cumulative upload, cumulative
    /// download) — monotonically increasing, so the speed window can
    /// subtract samples directly
    ring: VecDeque<(i64, u64, u64)>,
    /// Previous-tick snapshot of each interface's counters (per-interface
    /// deltas: each interface wraps at a different time, and summing first
    /// then differencing goes wrong as soon as any interface wraps)
    ifaces: HashMap<String, (u64, u64)>,
    acc_up: u64,
    acc_down: u64,
    today_ymd: (i32, u32, u32),
    up_today: u64,
    down_today: u64,
    /// pid -> process type label ("CLI session process"/"main process"/"renderer process"/...)
    cli_pids: HashMap<u32, String>,
    app_pids: HashMap<u32, String>,
    proc_refresh: Option<Instant>,
    last_save: Option<Instant>,
    dirty: bool,
}

impl NetIo {
    pub fn new() -> Self {
        let mut io = NetIo {
            ring: VecDeque::new(),
            ifaces: HashMap::new(),
            acc_up: 0,
            acc_down: 0,
            today_ymd: local_ymd(),
            up_today: 0,
            down_today: 0,
            cli_pids: HashMap::new(),
            app_pids: HashMap::new(),
            proc_refresh: None,
            last_save: None,
            dirty: false,
        };
        io.load_persisted();
        io
    }

    /// Restore the daily totals
    fn load_persisted(&mut self) {
        let Some(path) = net_file() else { return };
        let Ok(raw) = std::fs::read_to_string(path) else { return };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) else {
            eprintln!("[zcode-speed-panel] net totals file corrupted; starting from zero");
            return;
        };
        let day = v.get("day").and_then(|x| x.as_str()).unwrap_or("");
        if day != ymd_str(self.today_ymd) {
            return; // yesterday's totals: cleared naturally on day rollover
        }
        self.up_today = v.get("up").and_then(|x| x.as_u64()).unwrap_or(0);
        self.down_today = v.get("down").and_then(|x| x.as_u64()).unwrap_or(0);
    }

    fn save(&mut self, force: bool) {
        let due = force || self.last_save.map_or(true, |t| t.elapsed() > NET_SAVE_EVERY);
        if !due || !self.dirty {
            return;
        }
        self.last_save = Some(Instant::now());
        self.dirty = false;
        if let Some(path) = net_file() {
            let json = serde_json::json!({
                "day": ymd_str(self.today_ymd),
                "up": self.up_today,
                "down": self.down_today,
            });
            if let Err(e) = std::fs::write(&path, json.to_string()) {
                eprintln!("[zcode-speed-panel] failed to persist net totals: {e}");
            }
        }
    }

    /// Force a disk save before exit (called by save_all)
    pub fn save_forced(&mut self) {
        self.dirty = true;
        self.save(true);
    }

    /// Called every tick (poller ~700ms)
    pub fn tick(&mut self, now_ms: i64) -> NetNow {
        // Reset on day rollover (system-wide totals only count today)
        let ymd = local_ymd();
        if ymd != self.today_ymd {
            self.today_ymd = ymd;
            self.up_today = 0;
            self.down_today = 0;
            self.dirty = true;
        }

        // System-wide interface counters -> per-interface deltas (wraparound
        // correction: see wrap_delta) -> ring + daily totals.
        // The ring stores dewrapped monotonic totals; the speed window
        // subtracts them directly
        let available;
        if let Some(rows) = platform::net_ifaces() {
            available = true;
            let (mut du, mut dd) = (0u64, 0u64);
            for (k, up, down) in &rows {
                if let Some(&(ou, od)) = self.ifaces.get(k) {
                    du += wrap_delta(*up, ou, platform::NET_COUNTER_WRAP);
                    dd += wrap_delta(*down, od, platform::NET_COUNTER_WRAP);
                }
            }
            self.ifaces = rows.iter().map(|(k, u, d)| (k.clone(), (*u, *d))).collect();
            self.acc_up += du;
            self.acc_down += dd;
            if du > 0 || dd > 0 {
                self.up_today += du;
                self.down_today += dd;
                self.dirty = true;
            }
            self.ring.push_back((now_ms, self.acc_up, self.acc_down));
            while self.ring.len() > NET_RING_CAP {
                self.ring.pop_front();
            }
        } else {
            available = false;
        }

        // ~1s sliding-window delta speed (earliest sample in the window vs
        // the latest; totals are monotonic, so subtract directly; aligned
        // with Task Manager's ~1s refresh cadence — jumpier readings are expected)
        let (up_bps, down_bps) = {
            let r = &self.ring;
            match (r.front(), r.back()) {
                (Some(&(t0, _, _)), Some(&(t1, u1, d1))) if t1 > t0 => {
                    let from = now_ms - NET_WINDOW_MS;
                    let (bt, bu, bd) = r
                        .iter()
                        .find(|&&(t, _, _)| t >= from)
                        .copied()
                        .unwrap_or((t0, u1, d1));
                    let secs = (t1 - bt).max(1) as f64 / 1000.0;
                    (u1.saturating_sub(bu) as f64 / secs, d1.saturating_sub(bd) as f64 / secs)
                }
                _ => (0.0, 0.0),
            }
        };

        // Process-group refresh (enumerating the connection table every tick is cheap; the process + command line scan runs every 5s)
        let due = self.proc_refresh.map_or(true, |t| t.elapsed() > PROC_REFRESH_EVERY);
        if due {
            self.proc_refresh = Some(Instant::now());
            let (cli, app) = platform::zcode_pid_groups();
            self.cli_pids = cli.into_iter().collect();
            self.app_pids = app.into_iter().collect();
        }

        // Connection attribution (only the Windows implementation returns
        // Some): tag each connection with its owning pid, and attach the
        // process type label when assembling ConnStat (one process can own
        // several connections)
        let conns = platform::zcode_conns(&self.cli_pids, &self.app_pids);
        let conns_available = conns.is_some();
        let mut cli_conn_list: Vec<crate::metrics::ConnStat> = Vec::new();
        let mut app_conn_list: Vec<crate::metrics::ConnStat> = Vec::new();
        if let Some((cli_raw, app_raw)) = conns {
            for (remote, pid) in cli_raw {
                let proc = self.cli_pids.get(&pid).cloned().unwrap_or_default();
                cli_conn_list.push(crate::metrics::ConnStat { remote, pid, proc });
            }
            for (remote, pid) in app_raw {
                let proc = self.app_pids.get(&pid).cloned().unwrap_or_default();
                app_conn_list.push(crate::metrics::ConnStat { remote, pid, proc });
            }
        }
        let cli_conns = cli_conn_list.len() as u32;
        let app_conns = app_conn_list.len() as u32;

        self.save(false);

        NetNow {
            available,
            up_bps: up_bps.max(0.0),
            down_bps: down_bps.max(0.0),
            up_today: self.up_today,
            down_today: self.down_today,
            conns_available,
            cli_conns,
            app_conns,
            cli_conn_list,
            app_conn_list,
        }
    }
}

// ============ Tests ============
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sess_est_scales_with_tokens() {
        // Upload counts uncached prompt tokens (cache hits are not resent); download = output tokens x 400
        let (up, down) = sess_bytes_est(1000, 2000);
        assert!((up as f64 - 5000.0).abs() < 1e-6);
        assert!((down as f64 - 800_000.0).abs() < 1e-6);
    }

    /// Interface counter deltas: modulo on 32-bit wraparound yields the
    /// correct delta; regressions (resets) clamp to 0;
    /// huge anomalous deltas (>=2^31, index reuse/reset misread as wrap) also clamp to 0
    #[test]
    fn wrap_delta_handles_32bit_wrap_and_resets() {
        const W: u64 = 1 << 32;
        // normal delta
        assert_eq!(wrap_delta(500, 100, W), 400);
        // wraparound: from 2^32-300 across zero to 196, real delta = 300 + 196
        assert_eq!(wrap_delta(196, 4_294_967_296 - 300, W), 496);
        // mac (wrap=0): counter regression (reset) clamps to 0, no phantom traffic
        assert_eq!(wrap_delta(100, 500, 0), 0);
        assert_eq!(wrap_delta(900, 500, 0), 400);
        // Counter reset (million-scale regression to a small value): reading
        // it as wraparound would give a phantom delta >=2^31, so clamp to 0.
        // Note: on 32-bit counters, resets regressing by <2^31 are inherently
        // indistinguishable from wraparound
        assert_eq!(wrap_delta(1000, 1_000_000, W), 0);
    }

    /// Process type label (Windows methodology): the CLI check precedes the
    /// --type checks — zcode.cjs never appears in a renderer's command line,
    /// so the two checks do not conflict
    #[test]
    #[cfg(windows)]
    fn proc_label_by_command_line() {
        use crate::netio::platform::proc_label;
        assert_eq!(proc_label(r#""C:\...\zcode.exe" "C:\...\zcode.cjs" app-server"#), "CLI session process");
        assert_eq!(proc_label(r#""C:\...\ZCode.exe" --type=renderer --field-trial-handle=x"#), "renderer process");
        assert_eq!(proc_label(r#""C:\...\ZCode.exe" --type=gpu-process"#), "GPU process");
        assert_eq!(proc_label(r#""C:\...\ZCode.exe" --type=utility --utility-sub-type=net"#), "utility process");
        assert_eq!(proc_label(r#""C:\...\ZCode.exe" --type=crashpad-handler"#), "crash reporter process");
        assert_eq!(proc_label(r#""C:\...\ZCode.exe" --js-flags=..."#), "main process");
    }
}
