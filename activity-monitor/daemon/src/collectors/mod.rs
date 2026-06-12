pub mod window;
pub mod terminal;
pub mod browser;
pub mod clipboard;
pub mod filesystem;
pub mod process;
pub mod calendar;

pub use window::start_window_collector;
pub use terminal::start_terminal_collector;
pub use browser::publish_browser_message;
pub use clipboard::start_clipboard_collector;
pub use filesystem::start_filesystem_collector;
pub use process::start_process_collector;
pub use calendar::start_calendar_collector;
