use tools_kit::{EmptyToolsetBackend, Toolset};

fn touch_empty(toolset: Toolset) {
    let _ = std::mem::size_of::<EmptyToolsetBackend>();
    let _ = matches!(toolset, Toolset::Empty(_));
}

fn main() {}
