use tool_sdk::{ErasedToolHandlerBackend, ErasedToolsetBackend};

fn main() {
    let _ = std::mem::size_of::<&dyn ErasedToolHandlerBackend>();
    let _ = std::mem::size_of::<&dyn ErasedToolsetBackend>();
}
