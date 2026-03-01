use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{OnceLock, RwLock};

use crate::catalog::oid::Oid;
use crate::catalog::{SearchPath, with_catalog_read};

pub mod acl;
pub mod rls;
pub mod roles;

pub use acl::TablePrivilege;
pub use rls::{RlsCommand, RlsPolicy};
pub use roles::{AlterRoleOptions, CreateRoleOptions};

#[derive(Debug, Clone, Default)]
pub struct SecurityState {
    roles: roles::RoleRegistry,
    acl: acl::AclRegistry,
    rls: rls::RlsRegistry,
    relation_owners: HashMap<Oid, String>,
}

static GLOBAL_SECURITY: OnceLock<RwLock<SecurityState>> = OnceLock::new();

thread_local! {
    static CURRENT_ROLE: RefCell<String> = RefCell::new("postgres".to_string());
}

fn global_security() -> &'static RwLock<SecurityState> {
    GLOBAL_SECURITY.get_or_init(|| RwLock::new(SecurityState::default()))
}

pub fn with_security_read<T>(f: impl FnOnce(&SecurityState) -> T) -> T {
    let guard = match global_security().read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            debug_assert!(false, "global security lock poisoned for read");
            poisoned.into_inner()
        }
    };
    f(&guard)
}

pub fn with_security_write<T>(f: impl FnOnce(&mut SecurityState) -> T) -> T {
    let mut guard = match global_security().write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            debug_assert!(false, "global security lock poisoned for write");
            poisoned.into_inner()
        }
    };
    f(&mut guard)
}

pub fn snapshot_state() -> SecurityState {
    with_security_read(|state| state.clone())
}

pub fn restore_state(next: SecurityState) {
    with_security_write(|state| {
        *state = next;
    });
}

#[cfg(test)]
pub fn reset_global_security_for_tests() {
    with_security_write(|state| {
        *state = SecurityState::default();
    });
    CURRENT_ROLE.with(|role| {
        *role.borrow_mut() = "postgres".to_string();
    });
}

pub fn current_role() -> String {
    CURRENT_ROLE.with(|role| role.borrow().clone())
}

pub fn with_current_role<T>(role: &str, f: impl FnOnce() -> T) -> T {
    CURRENT_ROLE.with(|slot| {
        let previous = slot.borrow().clone();
        *slot.borrow_mut() = normalize_identifier(role);
        let out = f();
        *slot.borrow_mut() = previous;
        out
    })
}

pub async fn with_current_role_async<T, F, Fut>(role: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let previous = CURRENT_ROLE.with(|slot| {
        let previous = slot.borrow().clone();
        *slot.borrow_mut() = normalize_identifier(role);
        previous
    });
    let out = f().await;
    CURRENT_ROLE.with(|slot| {
        *slot.borrow_mut() = previous;
    });
    out
}

pub fn normalize_identifier(input: &str) -> String {
    input.trim().trim_matches('"').to_ascii_lowercase()
}

pub fn parse_qualified_name(input: &str) -> Vec<String> {
    input
        .split('.')
        .map(normalize_identifier)
        .filter(|part| !part.is_empty())
        .collect()
}

fn owner_or_default(relation_owners: &HashMap<Oid, String>, relation_oid: Oid) -> String {
    relation_owners
        .get(&relation_oid)
        .cloned()
        .unwrap_or_else(|| "postgres".to_string())
}

pub fn current_role_is_superuser() -> bool {
    let role = current_role();
    with_security_read(|state| state.roles.is_superuser(&role))
}

pub fn role_exists(role: &str) -> bool {
    let normalized = normalize_identifier(role);
    with_security_read(|state| state.roles.role_exists(&normalized))
}

pub fn role_requires_password(role: &str) -> bool {
    let normalized = normalize_identifier(role);
    with_security_read(|state| state.roles.password_required(&normalized))
}

pub fn role_password(role: &str) -> Option<String> {
    let normalized = normalize_identifier(role);
    with_security_read(|state| state.roles.password(&normalized))
}

pub fn verify_role_password(role: &str, password: &str) -> bool {
    let normalized = normalize_identifier(role);
    with_security_read(|state| state.roles.verify_password(&normalized, password))
}

pub fn can_role_login(role: &str) -> bool {
    let normalized = normalize_identifier(role);
    with_security_read(|state| state.roles.can_login(&normalized))
}

pub fn can_set_role(session_user: &str, target_role: &str) -> bool {
    let session_user = normalize_identifier(session_user);
    let target_role = normalize_identifier(target_role);
    with_security_read(|state| state.roles.can_set_role(&session_user, &target_role))
}

pub fn create_role(
    actor_role: &str,
    role_name: &str,
    options: CreateRoleOptions,
) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    let role_name = normalize_identifier(role_name);
    with_security_write(|state| {
        if !state.roles.is_superuser(&actor) {
            return Err("permission denied to create role".to_string());
        }
        state.roles.create_role(&role_name, options)
    })
}

pub fn alter_role(
    actor_role: &str,
    role_name: &str,
    options: AlterRoleOptions,
) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    let role_name = normalize_identifier(role_name);
    with_security_write(|state| {
        if !state.roles.is_superuser(&actor) {
            return Err("permission denied to alter role".to_string());
        }
        state.roles.alter_role(&role_name, options)
    })
}

pub fn drop_role(actor_role: &str, role_name: &str) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    let role_name = normalize_identifier(role_name);
    with_security_write(|state| {
        if !state.roles.is_superuser(&actor) {
            return Err("permission denied to drop role".to_string());
        }
        state.roles.drop_role(&role_name)
    })
}

pub fn grant_role(actor_role: &str, role_name: &str, member: &str) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    let role_name = normalize_identifier(role_name);
    let member = normalize_identifier(member);
    with_security_write(|state| {
        if !state.roles.is_superuser(&actor) {
            return Err("permission denied to grant role".to_string());
        }
        state.roles.grant_role(&role_name, &member)
    })
}

pub fn revoke_role(actor_role: &str, role_name: &str, member: &str) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    let role_name = normalize_identifier(role_name);
    let member = normalize_identifier(member);
    with_security_write(|state| {
        if !state.roles.is_superuser(&actor) {
            return Err("permission denied to revoke role".to_string());
        }
        state.roles.revoke_role(&role_name, &member)
    })
}

pub fn set_relation_owner(relation_oid: Oid, owner: &str) {
    let owner = normalize_identifier(owner);
    with_security_write(|state| {
        state.relation_owners.insert(relation_oid, owner);
    });
}

pub fn clear_relation_security(relation_oid: Oid) {
    with_security_write(|state| {
        state.relation_owners.remove(&relation_oid);
        state.acl.clear_relation(relation_oid);
        state.rls.clear_relation(relation_oid);
    });
}

pub fn relation_owner(relation_oid: Oid) -> String {
    with_security_read(|state| owner_or_default(&state.relation_owners, relation_oid))
}

pub fn can_manage_relation(actor_role: &str, relation_oid: Oid) -> bool {
    let actor = normalize_identifier(actor_role);
    with_security_read(|state| {
        if state.roles.is_superuser(&actor) {
            return true;
        }
        let closure = state.roles.role_closure(&actor);
        let owner = owner_or_default(&state.relation_owners, relation_oid);
        closure.contains(&owner)
    })
}

pub fn require_manage_relation(
    actor_role: &str,
    relation_oid: Oid,
    relation_name: &str,
) -> Result<(), String> {
    if can_manage_relation(actor_role, relation_oid) {
        return Ok(());
    }
    Err(format!(
        "permission denied for relation \"{relation_name}\" (owner required)"
    ))
}

pub fn grant_table_privileges(
    actor_role: &str,
    relation_oid: Oid,
    relation_name: &str,
    grantees: &[String],
    privileges: &[TablePrivilege],
) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    with_security_write(|state| {
        let owner = owner_or_default(&state.relation_owners, relation_oid);
        let actor_closure = state.roles.role_closure(&actor);
        if !state.roles.is_superuser(&actor) && !actor_closure.contains(&owner) {
            return Err(format!(
                "permission denied for relation \"{relation_name}\""
            ));
        }
        for grantee in grantees {
            let grantee = normalize_identifier(grantee);
            if !state.roles.role_exists(&grantee) {
                return Err(format!("role \"{grantee}\" does not exist"));
            }
            state.acl.grant(relation_oid, &grantee, privileges);
        }
        Ok(())
    })
}

pub fn revoke_table_privileges(
    actor_role: &str,
    relation_oid: Oid,
    relation_name: &str,
    grantees: &[String],
    privileges: &[TablePrivilege],
) -> Result<(), String> {
    let actor = normalize_identifier(actor_role);
    with_security_write(|state| {
        let owner = owner_or_default(&state.relation_owners, relation_oid);
        let actor_closure = state.roles.role_closure(&actor);
        if !state.roles.is_superuser(&actor) && !actor_closure.contains(&owner) {
            return Err(format!(
                "permission denied for relation \"{relation_name}\""
            ));
        }
        for grantee in grantees {
            let grantee = normalize_identifier(grantee);
            if !state.roles.role_exists(&grantee) {
                return Err(format!("role \"{grantee}\" does not exist"));
            }
            state.acl.revoke(relation_oid, &grantee, privileges);
        }
        Ok(())
    })
}

pub fn has_table_privilege(role: &str, relation_oid: Oid, privilege: TablePrivilege) -> bool {
    let role = normalize_identifier(role);
    with_security_read(|state| {
        if state.roles.is_superuser(&role) {
            return true;
        }

        let closure = state.roles.role_closure(&role);
        let owner = owner_or_default(&state.relation_owners, relation_oid);
        if closure.contains(&owner) {
            return true;
        }
        state.acl.has_privilege(relation_oid, &closure, privilege)
    })
}

pub fn require_table_privilege(
    role: &str,
    relation_oid: Oid,
    relation_name: &str,
    privilege: TablePrivilege,
) -> Result<(), String> {
    if has_table_privilege(role, relation_oid, privilege) {
        return Ok(());
    }
    Err(format!(
        "permission denied for relation \"{}\" (missing {} privilege)",
        relation_name,
        privilege.display_name()
    ))
}

pub fn enable_rls(actor_role: &str, relation_oid: Oid, relation_name: &str) -> Result<(), String> {
    require_manage_relation(actor_role, relation_oid, relation_name)?;
    with_security_write(|state| {
        state.rls.enable_for_relation(relation_oid);
    });
    Ok(())
}

pub fn disable_rls(actor_role: &str, relation_oid: Oid, relation_name: &str) -> Result<(), String> {
    require_manage_relation(actor_role, relation_oid, relation_name)?;
    with_security_write(|state| {
        state.rls.disable_for_relation(relation_oid);
    });
    Ok(())
}

pub fn create_policy(
    actor_role: &str,
    mut policy: RlsPolicy,
    relation_name: &str,
) -> Result<(), String> {
    require_manage_relation(actor_role, policy.relation_oid, relation_name)?;
    for role in &mut policy.roles {
        *role = normalize_identifier(role);
        if !role_exists(role) {
            return Err(format!("role \"{role}\" does not exist"));
        }
    }
    policy.name = normalize_identifier(&policy.name);
    with_security_write(|state| state.rls.create_policy(policy))
}

pub fn drop_policy(
    actor_role: &str,
    relation_oid: Oid,
    relation_name: &str,
    policy_name: &str,
    if_exists: bool,
) -> Result<(), String> {
    require_manage_relation(actor_role, relation_oid, relation_name)?;
    with_security_write(|state| {
        state
            .rls
            .drop_policy(relation_oid, &normalize_identifier(policy_name), if_exists)
    })
}

#[derive(Debug, Clone)]
pub struct RlsEvaluation {
    pub enabled: bool,
    pub bypass: bool,
    pub policies: Vec<RlsPolicy>,
}

pub fn rls_evaluation_for_role(
    role: &str,
    relation_oid: Oid,
    command: RlsCommand,
) -> RlsEvaluation {
    let normalized_role = normalize_identifier(role);
    with_security_read(|state| {
        let enabled = state.rls.is_enabled_for_relation(relation_oid);
        let bypass = state.roles.is_superuser(&normalized_role)
            || state
                .roles
                .role_closure(&normalized_role)
                .contains(&owner_or_default(&state.relation_owners, relation_oid));
        let policies = if enabled && !bypass {
            let closure = state.roles.role_closure(&normalized_role);
            state
                .rls
                .applicable_policies(relation_oid, command, &closure)
        } else {
            Vec::new()
        };
        RlsEvaluation {
            enabled,
            bypass,
            policies,
        }
    })
}

pub fn resolve_relation_oid(name_parts: &[String]) -> Result<(Oid, String), String> {
    let relation = with_catalog_read(|catalog| {
        catalog
            .resolve_table(name_parts, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| err.message)?;
    Ok((relation.oid(), relation.qualified_name()))
}
