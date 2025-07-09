pub mod binary_reader;
pub mod binary_writer;
pub mod bits;
pub mod buffer_pool;
pub mod byte_reader;
pub mod byte_writer;
pub mod circular_byte_buffer;
pub mod heap_byte_writer;
pub mod multi_buffer_pool;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_buffer_pool() {
        let pool = buffer_pool::BufferPool::new(1024, 8, 16);

        assert_eq!(pool.buf_size(), 1024);

        let one = pool.get().await;
        assert_eq!(one.len(), 1024);
        drop(one);

        let mut vec = Vec::new();

        for _ in 0..16 {
            let buf = pool.get().await;
            vec.push(buf);
        }

        // assert that taking the next buffer will time out
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), pool.get()).await;
        assert!(
            result.is_err(),
            "Expected timeout when trying to get more buffers than available"
        );

        // drop one
        drop(vec.pop());

        // assert that we can get a new buffer now
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), pool.get()).await;
        assert!(
            result.is_ok(),
            "Expected to get a new buffer after dropping one"
        );
    }

    #[tokio::test]
    async fn test_binreader() {
        use binary_reader::BinaryReader;

        let mut data: &[u8] = &[
            255, 0, 0, 0, // 255
            0, 0, 0, 255, // 0xff000000
            42, 42, // 42 42
            0xde, 0xad, // 0xadde,
            0x00, 0x00, 0x80, 0x3f, // 1.0f32,
            7, 7, 7, 7, // sevens
            3, b'a', b'b', b'c', // abc u8
            3, 0, b'a', b'b', b'c', // abc u16
            3, b'a', b'b', b'c', // abc varint
        ];

        let mut reader = BinaryReader::new(&mut data);

        assert_eq!(reader.read_u32().unwrap(), 255);
        assert_eq!(reader.read_u32().unwrap(), 0xff000000);
        assert_eq!(reader.read_u8().unwrap(), 42);
        assert_eq!(reader.read_u8().unwrap(), 42);
        assert_eq!(reader.read_u16().unwrap(), 0xadde);
        assert_eq!(reader.read_f32().unwrap(), 1.0f32);

        let mut buf = [0u8; 4];
        reader.read_bytes(&mut buf).unwrap();
        assert_eq!(buf, [7, 7, 7, 7]);

        assert_eq!(reader.read_string_u8().unwrap(), "abc");
        assert_eq!(reader.read_string_u16().unwrap(), "abc");
        assert_eq!(reader.read_string_var().unwrap(), "abc");
    }
}
