//! Low-level socket and serialization utilities.
//! Sockaddr utils are partly taken from the Rust std source code

#[cfg(target_os = "linux")]
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[cfg(target_os = "linux")]
fn ip_v4_addr_to_c(addr: &Ipv4Addr) -> libc::in_addr {
    // `s_addr` is stored as BE on all machines and the array is in BE order.
    // So the native endian conversion method is used so that it's never swapped.
    libc::in_addr {
        s_addr: u32::from_ne_bytes(addr.octets()),
    }
}

#[cfg(target_os = "linux")]
fn ip_v6_addr_to_c(addr: &Ipv6Addr) -> libc::in6_addr {
    libc::in6_addr { s6_addr: addr.octets() }
}

#[cfg(target_os = "linux")]
fn socket_addr_v4_to_c(addr: &SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: addr.port().to_be(),
        sin_addr: ip_v4_addr_to_c(addr.ip()),
        ..unsafe { std::mem::zeroed() }
    }
}

#[cfg(target_os = "linux")]
fn socket_addr_v6_to_c(addr: &SocketAddrV6) -> libc::sockaddr_in6 {
    libc::sockaddr_in6 {
        sin6_family: libc::AF_INET6 as libc::sa_family_t,
        sin6_port: addr.port().to_be(),
        sin6_addr: ip_v6_addr_to_c(addr.ip()),
        sin6_flowinfo: addr.flowinfo(),
        sin6_scope_id: addr.scope_id(),
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
pub union SocketAddrCRepr {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
}

#[cfg(target_os = "linux")]
pub fn socket_addr_to_c(addr: &SocketAddr) -> (SocketAddrCRepr, libc::socklen_t) {
    match addr {
        SocketAddr::V4(a) => {
            let sockaddr = SocketAddrCRepr { v4: socket_addr_v4_to_c(a) };
            (sockaddr, size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        SocketAddr::V6(a) => {
            let sockaddr = SocketAddrCRepr { v6: socket_addr_v6_to_c(a) };
            (sockaddr, size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

/// Appends data to a vector without zeroing it out first.
#[inline]
pub fn append_to_vec(vec: &mut Vec<u8>, data: &[u8]) {
    // TODO: benchmark this against Vec::extend_from_slice
    let remaining_capacity = vec.capacity() - vec.len();

    if remaining_capacity < data.len() {
        vec.reserve(data.len() - remaining_capacity);
    }

    let len = vec.len();

    unsafe {
        vec.set_len(len + data.len());
        vec[len..].copy_from_slice(data);
    }
}

#[inline]
pub fn uninit_box_bytes(size: usize) -> Box<[u8]> {
    assert!(size > 0);

    let mut vec = Vec::with_capacity(size);
    let ptr = vec.as_mut_ptr();

    let real_size = vec.capacity();
    std::mem::forget(vec);

    // safety: the pointer is valid and the size is correct
    unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, real_size)) }
}
