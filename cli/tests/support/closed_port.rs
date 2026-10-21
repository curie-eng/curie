//! A loopback port that cannot accept connections or be taken by another process.
//!
//! Binding a listener to port 0, recording the port and dropping it leaves a race:
//! a parallel test process can bind the same port before the request goes out and
//! answer it. `ClosedPort` keeps a bound but never-listening socket alive, so a
//! connection cannot be established, and no other process can bind the port
//! while the guard lives. Linux refuses immediately; Darwin drops the SYN, so
//! callers must use a connection deadline.
#![allow(dead_code)]

use std::net::Ipv4Addr;
use std::os::fd::{FromRawFd, OwnedFd};

/// Holds a loopback port bound but not listening, for as long as the guard lives.
pub struct ClosedPort {
    port: u16,
    _socket: OwnedFd,
}

impl ClosedPort {
    pub fn new() -> Self {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let socket_type = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let socket_type = libc::SOCK_STREAM;
        // SAFETY: plain socket(2) call with constant arguments; the result is checked.
        let fd = unsafe { libc::socket(libc::AF_INET, socket_type, 0) };
        if fd < 0 {
            panic!(
                "create closed-port socket: {}",
                std::io::Error::last_os_error()
            );
        }
        // SAFETY: `fd` is a freshly created, valid descriptor that nothing else owns.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };

        // macOS has no SOCK_CLOEXEC socket type flag. Set the descriptor flag
        // before bind; OwnedFd closes it if either checked operation fails.
        // Unlike Linux's socket flag, this fallback is not atomic with exec.
        // https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            // SAFETY: fd is open, and F_GETFD takes no third argument.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(
                flags >= 0,
                "read socket flags: {}",
                std::io::Error::last_os_error()
            );
            // SAFETY: F_SETFD accepts the descriptor flag bits as an integer.
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
            assert!(
                rc >= 0,
                "set socket flags: {}",
                std::io::Error::last_os_error()
            );
        }

        // SO_REUSEADDR stays unset on purpose: with it, Linux lets two
        // non-listening sockets share a port, which would defeat the guard.
        // SAFETY: sockaddr_in is plain data and all-zero is a valid value.
        let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        addr.sin_family = libc::AF_INET as libc::sa_family_t;
        addr.sin_port = 0;
        addr.sin_addr = libc::in_addr {
            s_addr: u32::from(Ipv4Addr::LOCALHOST).to_be(),
        };
        let len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;

        // SAFETY: `addr` is a valid sockaddr_in of `len` bytes and `fd` is open.
        let rc = unsafe {
            libc::bind(
                fd,
                &addr as *const libc::sockaddr_in as *const libc::sockaddr,
                len,
            )
        };
        if rc != 0 {
            panic!(
                "bind closed-port socket: {}",
                std::io::Error::last_os_error()
            );
        }

        // SAFETY: as above, all-zero is a valid sockaddr_in.
        let mut bound: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        let mut bound_len = len;
        // SAFETY: `bound` is writable for `bound_len` bytes and `fd` is open.
        let rc = unsafe {
            libc::getsockname(
                fd,
                &mut bound as *mut libc::sockaddr_in as *mut libc::sockaddr,
                &mut bound_len,
            )
        };
        if rc != 0 {
            panic!(
                "read closed-port socket address: {}",
                std::io::Error::last_os_error()
            );
        }

        Self {
            port: u16::from_be(bound.sin_port),
            _socket: socket,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Default for ClosedPort {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ClosedPort;
    use std::io::ErrorKind;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    #[test]
    fn port_is_held_and_refuses_connections_while_alive() {
        let guard = ClosedPort::new();
        let port = guard.port();
        assert_ne!(port, 0);
        assert_eq!(guard.url(), format!("http://127.0.0.1:{}", guard.port()));

        let err = TcpListener::bind(("127.0.0.1", port)).expect_err("port must be held");
        assert_eq!(err.kind(), ErrorKind::AddrInUse);
        assert_eq!(err.raw_os_error(), Some(libc::EADDRINUSE));

        // Positive control: this host can establish a loopback TCP connection.
        let listening = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        TcpStream::connect_timeout(&listening.local_addr().unwrap(), Duration::from_secs(1))
            .expect("a listening port accepts connections");

        let target = SocketAddr::from(([127, 0, 0, 1], port));
        let err = TcpStream::connect_timeout(&target, Duration::from_millis(250))
            .expect_err("nothing listens");
        // Darwin drops input for TCPS_CLOSED without sending a reset:
        // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet/tcp_input.c
        #[cfg(target_os = "macos")]
        assert_eq!(err.kind(), ErrorKind::TimedOut);
        #[cfg(not(target_os = "macos"))]
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    }

    #[test]
    fn descriptor_is_close_on_exec() {
        use std::os::fd::AsRawFd;

        let guard = ClosedPort::new();
        // SAFETY: the guard owns this open descriptor; F_GETFD takes no third argument.
        let flags = unsafe { libc::fcntl(guard._socket.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn port_is_released_on_drop() {
        let guard = ClosedPort::new();
        let port = guard.port();
        drop(guard);
        TcpListener::bind(("127.0.0.1", port)).expect("port is free after drop");
    }
}
