/// Requires Copy to ensure the layout is POD
pub fn apply_layout<T: Copy>(data: &[u8]) -> anyhow::Result<&T> {
    let layout_len = std::mem::size_of::<T>();
    if data.len() < layout_len {
        anyhow::bail!("Data is too shor to apply layout");
    }

    //SAFETY: Transmuting a POD is always safe
    unsafe {
        let ptr: *const T = std::mem::transmute(data[..layout_len].as_ptr());
        Ok(ptr.as_ref().unwrap())
    }
}

/// Encoded u32 ensuring on-disk endianess is consistent across architectures
#[derive(Copy,Clone)]
#[repr(C)]
pub struct EncodedU32 {
    inner: [u8; 4],
}

impl EncodedU32 {
    pub fn new(val: u32) -> Self {
        Self {
            inner: val.to_le_bytes(),
        }
    }

    pub fn get(&self) -> u32 {
        u32::from_le_bytes(self.inner)
    }

    pub fn get_as_usize(&self) -> usize {
        u32::from_le_bytes(self.inner) as usize
    }
}

/// Encoded u64 ensuring on-disk endianess is consistent across architectures
#[derive(Copy, Clone)]
#[repr(C)]
pub struct EncodedU64 {
    inner: [u8; 8],
}

impl EncodedU64 {
    pub fn new(val: u64) -> Self {
        Self {
            inner: val.to_le_bytes(),
        }
    }

    pub fn get(&self) -> u64 {
        u64::from_le_bytes(self.inner)
    }

    pub fn get_as_usize(&self) -> usize {
        u64::from_le_bytes(self.inner) as usize
    }

}
