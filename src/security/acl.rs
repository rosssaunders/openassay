use std::collections::{HashMap, HashSet};

use crate::catalog::oid::Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TablePrivilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    Usage,
}

impl TablePrivilege {
    pub fn from_keyword(keyword: &str) -> Option<Self> {
        match keyword {
            "SELECT" => Some(Self::Select),
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            "TRUNCATE" => Some(Self::Truncate),
            "USAGE" => Some(Self::Usage),
            _ => None,
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::Select,
            Self::Insert,
            Self::Update,
            Self::Delete,
            Self::Truncate,
            Self::Usage,
        ]
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Usage => "USAGE",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AclRegistry {
    grants: HashMap<Oid, HashMap<String, HashSet<TablePrivilege>>>,
}

impl AclRegistry {
    pub fn clear_relation(&mut self, relation_oid: Oid) {
        self.grants.remove(&relation_oid);
    }

    pub fn grant(&mut self, relation_oid: Oid, role: &str, privileges: &[TablePrivilege]) {
        let role_grants = self
            .grants
            .entry(relation_oid)
            .or_default()
            .entry(role.to_string())
            .or_default();
        for privilege in privileges {
            role_grants.insert(*privilege);
        }
    }

    pub fn revoke(&mut self, relation_oid: Oid, role: &str, privileges: &[TablePrivilege]) {
        let Some(by_role) = self.grants.get_mut(&relation_oid) else {
            return;
        };
        let Some(role_grants) = by_role.get_mut(role) else {
            return;
        };
        for privilege in privileges {
            role_grants.remove(privilege);
        }
        if role_grants.is_empty() {
            by_role.remove(role);
        }
        if by_role.is_empty() {
            self.grants.remove(&relation_oid);
        }
    }

    pub fn has_privilege(
        &self,
        relation_oid: Oid,
        role_closure: &HashSet<String>,
        privilege: TablePrivilege,
    ) -> bool {
        let Some(by_role) = self.grants.get(&relation_oid) else {
            return false;
        };
        role_closure.iter().any(|role| {
            by_role
                .get(role)
                .is_some_and(|privs| privs.contains(&privilege))
        })
    }
}
