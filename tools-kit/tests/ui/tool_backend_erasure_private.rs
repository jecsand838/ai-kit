use tools_kit::{ErasedToolHandlerBackend, ErasedToolsetBackend};

fn main() {
    let _ = std::mem::size_of::<&dyn ErasedToolHandlerBackend>();
    let _ = std::mem::size_of::<&dyn ErasedToolsetBackend>();
}
