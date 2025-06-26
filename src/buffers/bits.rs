use std::ops::{BitAnd, BitOr, Not, Shl, Shr};

pub trait Integer:
    BitAnd<Self, Output = Self>
    + BitOr<Self, Output = Self>
    + TryFrom<usize>
    + Not<Output = Self>
    + Shr<Self, Output = Self>
    + Shl<Self, Output = Self>
    + Copy
    + PartialEq
    + Default
{
    const SIZE: usize;

    fn decode(arr: [u8; Self::SIZE]) -> Self;
}

impl Integer for u8 {
    const SIZE: usize = 1;

    fn decode(arr: [u8; Self::SIZE]) -> Self {
        arr[0]
    }
}

impl Integer for u16 {
    const SIZE: usize = 2;

    fn decode(arr: [u8; Self::SIZE]) -> Self {
        u16::from_le_bytes(arr)
    }
}

impl Integer for u32 {
    const SIZE: usize = 4;

    fn decode(arr: [u8; Self::SIZE]) -> Self {
        u32::from_le_bytes(arr)
    }
}

impl Integer for u64 {
    const SIZE: usize = 8;

    fn decode(arr: [u8; Self::SIZE]) -> Self {
        u64::from_le_bytes(arr)
    }
}

pub struct Bits<U: Integer> {
    bits: U,
}

impl<U: Integer> Default for Bits<U> {
    fn default() -> Self {
        Bits { bits: U::default() }
    }
}

impl<U: Integer> Bits<U> {
    pub fn new(bits: U) -> Self {
        Bits { bits }
    }

    pub fn set_bit(&mut self, idx: usize) {
        let rhs = match U::try_from(1 << idx) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        self.bits = self.bits | rhs;
    }

    pub fn clear_bit(&mut self, idx: usize) {
        let rhs = match U::try_from(1 << idx) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        self.bits = self.bits & !rhs;
    }

    // Returns a bit at the given index, where 0 is the least significant bit
    pub fn get_bit(&self, idx: usize) -> bool {
        let rhs = match U::try_from(1 << idx) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        (self.bits & rhs) != U::default()
    }

    // TODO: scary impl, test if it works
    #[inline]
    pub fn get_multiple_bits(&self, idx_start: usize, idx_end: usize) -> U {
        // first shift the bits to the right by idx_start
        let shift_amount = match U::try_from(idx_start) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        let shifted = self.bits >> shift_amount;

        // compute the mask
        let mask_size = idx_end - idx_start + 1;
        let mask = match U::try_from((1 << mask_size) - 1) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        // apply the mask to the shifted bits
        shifted & mask
    }

    #[inline]
    pub fn set_multiple_bits(&mut self, idx_start: usize, idx_end: usize, value: U) {
        // first shift the value to the left by idx_start
        let shift_amount = match U::try_from(idx_start) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        let shifted_value = value << shift_amount;

        // compute the mask
        let mask_size = idx_end - idx_start + 1;
        let mask = match U::try_from((1 << mask_size) - 1) {
            Ok(value) => value,
            Err(_) => unreachable!(),
        };

        let mask = mask << shift_amount;

        // clear the bits in the range and set the new value
        self.bits = (self.bits & !mask) | (shifted_value & mask);
    }

    pub fn to_bits(&self) -> U {
        self.bits
    }
}
