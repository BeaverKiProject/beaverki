use anyhow::{Context, Result};
use std::process::{Command, Stdio};

pub fn open_system_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let command_and_args = ("open".to_owned(), vec![url.to_owned()]);

    #[cfg(target_os = "linux")]
    let command_and_args = ("xdg-open".to_owned(), vec![url.to_owned()]);

    #[cfg(target_os = "windows")]
    let command_and_args = (
        "cmd".to_owned(),
        vec![
            "/C".to_owned(),
            "start".to_owned(),
            "".to_owned(),
            url.to_owned(),
        ],
    );

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let command_and_args = {
        anyhow::bail!("system browser launch is not configured for this platform");
    };

    Command::new(&command_and_args.0)
        .args(&command_and_args.1)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to launch browser via {}", command_and_args.0))?;
    Ok(())
}
