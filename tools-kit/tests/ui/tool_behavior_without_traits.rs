use tools_kit::{ToolHandler, Toolset};

fn main() {
    let _ = Toolset::list_tools;
    let _ = Toolset::tool_handler;
    let _ = Toolset::shutdown;
    let _ = ToolHandler::step;
}
