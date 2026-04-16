use beaverki_core::ShellRisk;

const SAFE_READ_ONLY_PREFIXES: &[&str] = &[
    "pwd",
    "ls",
    "cat",
    "rg",
    "grep",
    "find",
    "head",
    "tail",
    "wc",
    "sed -n",
    "git status",
    "git diff",
    "git log",
    "git show",
    "git rev-parse",
    "git branch --show-current",
    "date",
    "uname",
    "whoami",
];

const HIGH_RISK_PREFIXES: &[&str] = &[
    "curl ",
    "wget ",
    "brew ",
    "apt ",
    "apt-get ",
    "dnf ",
    "yum ",
    "npm ",
    "pnpm ",
    "yarn ",
    "cargo install ",
    "pip ",
    "pip3 ",
    "git push",
    "git pull",
    "scp ",
    "ssh ",
    "mv ",
    "cp ",
    "touch ",
    "mkdir ",
    "truncate ",
    "tee ",
];

const CRITICAL_PREFIXES: &[&str] = &[
    "rm ", "sudo ", "dd ", "mkfs", "shutdown", "reboot", "kill ", "killall ", "chmod ", "chown ",
];

const SHELL_META_TOKENS: &[&str] = &["&&", "||", ";", "|", ">", "<", "$(", "`"];

pub fn classify_shell_command(command: &str) -> ShellRisk {
    let trimmed = command.trim();

    if trimmed.is_empty() {
        return ShellRisk::Medium;
    }

    if CRITICAL_PREFIXES
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
    {
        return ShellRisk::Critical;
    }

    if SHELL_META_TOKENS
        .iter()
        .any(|token| trimmed.contains(token))
    {
        return ShellRisk::High;
    }

    if HIGH_RISK_PREFIXES
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
    {
        return ShellRisk::High;
    }

    if SAFE_READ_ONLY_PREFIXES
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
    {
        return ShellRisk::Low;
    }

    ShellRisk::Medium
}

pub fn generated_shell_execution_allowed(risk: ShellRisk) -> bool {
    matches!(risk, ShellRisk::Low)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_safe_read_only_commands_as_low() {
        assert_eq!(classify_shell_command("rg TODO ."), ShellRisk::Low);
        assert_eq!(classify_shell_command("git status"), ShellRisk::Low);
    }

    #[test]
    fn classifies_destructive_commands_as_critical() {
        assert_eq!(
            classify_shell_command("rm -rf /tmp/foo"),
            ShellRisk::Critical
        );
    }

    #[test]
    fn blocks_shell_metacharacters() {
        assert_eq!(
            classify_shell_command("cat Cargo.toml | head"),
            ShellRisk::High
        );
    }
}
