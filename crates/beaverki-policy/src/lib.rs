use beaverki_core::{MemoryScope, ShellRisk};

pub const BUILTIN_ROLES: &[(&str, &str)] = &[
    (
        "owner",
        "Full administrative control over the runtime and approvals.",
    ),
    (
        "adult",
        "Trusted household user with approval and household access.",
    ),
    (
        "child",
        "Restricted household user with private-only access by default.",
    ),
    (
        "guest",
        "Temporary restricted user with private-only access.",
    ),
    (
        "service",
        "Non-human service identity for scoped automation tasks.",
    ),
];

const HOUSEHOLD_VISIBLE_ROLES: &[&str] = &["owner", "adult", "service"];
const APPROVER_ROLES: &[&str] = &["owner", "adult"];
const SUBAGENT_ROLES: &[&str] = &["owner", "adult", "service"];
const APPROVAL_ELIGIBLE_ROLES: &[&str] = &["owner", "adult"];

pub fn is_builtin_role(role_id: &str) -> bool {
    BUILTIN_ROLES.iter().any(|(builtin, _)| *builtin == role_id)
}

pub fn built_in_roles() -> Vec<(&'static str, &'static str)> {
    BUILTIN_ROLES.to_vec()
}

pub fn can_view_household_memory(role_ids: &[String]) -> bool {
    role_ids
        .iter()
        .any(|role| HOUSEHOLD_VISIBLE_ROLES.contains(&role.as_str()))
}

pub fn can_grant_approvals(role_ids: &[String]) -> bool {
    role_ids
        .iter()
        .any(|role| APPROVER_ROLES.contains(&role.as_str()))
}

pub fn can_spawn_subagents(role_ids: &[String]) -> bool {
    role_ids
        .iter()
        .any(|role| SUBAGENT_ROLES.contains(&role.as_str()))
}

pub fn can_request_shell_approval(role_ids: &[String], risk: ShellRisk) -> bool {
    matches!(
        risk,
        ShellRisk::Medium | ShellRisk::High | ShellRisk::Critical
    ) && role_ids
        .iter()
        .any(|role| APPROVAL_ELIGIBLE_ROLES.contains(&role.as_str()))
}

pub fn can_request_automation_approval(role_ids: &[String]) -> bool {
    role_ids
        .iter()
        .any(|role| APPROVAL_ELIGIBLE_ROLES.contains(&role.as_str()))
}

pub fn visible_memory_scopes(role_ids: &[String]) -> Vec<MemoryScope> {
    let mut scopes = vec![MemoryScope::Private];
    if can_view_household_memory(role_ids) {
        scopes.push(MemoryScope::Household);
    }
    scopes
}

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

    fn roles(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|value| (*value).to_owned()).collect()
    }

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

    #[test]
    fn owner_sees_household_and_can_approve() {
        let roles = roles(&["owner"]);
        assert!(can_view_household_memory(&roles));
        assert!(can_grant_approvals(&roles));
        assert_eq!(
            visible_memory_scopes(&roles),
            vec![MemoryScope::Private, MemoryScope::Household]
        );
    }

    #[test]
    fn child_is_private_only_and_cannot_approve() {
        let roles = roles(&["child"]);
        assert!(!can_view_household_memory(&roles));
        assert!(!can_grant_approvals(&roles));
        assert_eq!(visible_memory_scopes(&roles), vec![MemoryScope::Private]);
        assert!(!can_request_shell_approval(&roles, ShellRisk::High));
    }
}
