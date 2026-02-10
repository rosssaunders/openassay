use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleEntry {
    pub name: String,
    pub superuser: bool,
    pub login: bool,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleOptions {
    pub superuser: bool,
    pub login: bool,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterRoleOptions {
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub password: Option<String>,
}

impl Default for CreateRoleOptions {
    fn default() -> Self {
        Self {
            superuser: false,
            login: true,
            password: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoleRegistry {
    roles: HashMap<String, RoleEntry>,
    granted_roles: HashMap<String, HashSet<String>>,
}

impl Default for RoleRegistry {
    fn default() -> Self {
        Self::bootstrap()
    }
}

impl RoleRegistry {
    pub fn bootstrap() -> Self {
        let mut roles = HashMap::new();
        roles.insert(
            "postgres".to_string(),
            RoleEntry {
                name: "postgres".to_string(),
                superuser: true,
                login: true,
                password: None,
            },
        );
        roles.insert(
            "public".to_string(),
            RoleEntry {
                name: "public".to_string(),
                superuser: false,
                login: false,
                password: None,
            },
        );
        Self {
            roles,
            granted_roles: HashMap::new(),
        }
    }

    pub fn role_exists(&self, role: &str) -> bool {
        self.roles.contains_key(role)
    }

    pub fn create_role(&mut self, role: &str, options: CreateRoleOptions) -> Result<(), String> {
        if self.roles.contains_key(role) {
            return Err(format!("role \"{}\" already exists", role));
        }
        self.roles.insert(
            role.to_string(),
            RoleEntry {
                name: role.to_string(),
                superuser: options.superuser,
                login: options.login,
                password: options.password,
            },
        );
        Ok(())
    }

    pub fn drop_role(&mut self, role: &str) -> Result<(), String> {
        if matches!(role, "postgres" | "public") {
            return Err(format!("role \"{}\" cannot be dropped", role));
        }
        if self.roles.remove(role).is_none() {
            return Err(format!("role \"{}\" does not exist", role));
        }
        self.granted_roles.remove(role);
        for grants in self.granted_roles.values_mut() {
            grants.remove(role);
        }
        Ok(())
    }

    pub fn alter_role(&mut self, role: &str, options: AlterRoleOptions) -> Result<(), String> {
        let Some(entry) = self.roles.get_mut(role) else {
            return Err(format!("role \"{}\" does not exist", role));
        };
        if let Some(value) = options.superuser {
            entry.superuser = value;
        }
        if let Some(value) = options.login {
            entry.login = value;
        }
        if let Some(value) = options.password {
            entry.password = Some(value);
        }
        Ok(())
    }

    pub fn grant_role(&mut self, role: &str, member: &str) -> Result<(), String> {
        if !self.roles.contains_key(role) {
            return Err(format!("role \"{}\" does not exist", role));
        }
        if !self.roles.contains_key(member) {
            return Err(format!("role \"{}\" does not exist", member));
        }
        if role == member {
            return Ok(());
        }

        let entry = self.granted_roles.entry(member.to_string()).or_default();
        entry.insert(role.to_string());
        Ok(())
    }

    pub fn revoke_role(&mut self, role: &str, member: &str) -> Result<(), String> {
        if !self.roles.contains_key(role) {
            return Err(format!("role \"{}\" does not exist", role));
        }
        if !self.roles.contains_key(member) {
            return Err(format!("role \"{}\" does not exist", member));
        }
        if let Some(entry) = self.granted_roles.get_mut(member) {
            entry.remove(role);
        }
        Ok(())
    }

    pub fn is_superuser(&self, role: &str) -> bool {
        self.roles.get(role).is_some_and(|r| r.superuser)
    }

    pub fn can_login(&self, role: &str) -> bool {
        self.roles.get(role).is_some_and(|r| r.login)
    }

    pub fn password_required(&self, role: &str) -> bool {
        self.roles
            .get(role)
            .and_then(|r| r.password.as_ref())
            .is_some()
    }

    pub fn password(&self, role: &str) -> Option<String> {
        self.roles.get(role).and_then(|r| r.password.clone())
    }

    pub fn verify_password(&self, role: &str, password: &str) -> bool {
        self.roles
            .get(role)
            .and_then(|r| r.password.as_ref())
            .is_some_and(|expected| expected == password)
    }

    pub fn role_closure(&self, role: &str) -> HashSet<String> {
        let mut visited = HashSet::new();
        if !self.roles.contains_key(role) {
            return visited;
        }

        let mut queue = VecDeque::new();
        queue.push_back(role.to_string());
        while let Some(current) = queue.pop_front() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(grants) = self.granted_roles.get(&current) {
                for next in grants {
                    queue.push_back(next.clone());
                }
            }
        }

        visited.insert("public".to_string());
        visited
    }

    pub fn can_set_role(&self, session_user: &str, target_role: &str) -> bool {
        if session_user == target_role {
            return true;
        }
        if self.is_superuser(session_user) {
            return true;
        }
        let reachable = self.role_closure(session_user);
        reachable.contains(target_role)
    }
}
