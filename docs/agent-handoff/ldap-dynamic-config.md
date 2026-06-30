# Agent Handoff: Dynamic LDAP Configuration via ExtraConfig

This document summarizes work to propagate LDAP settings from the Kubernetes shim to
YuniKorn core through the existing `ExtraConfig` mechanism, replacing the hard-coded
filesystem path `/run/secrets/ldap`.

**Repos involved:**
- `yunikorn-k8shim` — reads ConfigMap + Secret, sends `ExtraConfig` to core
- `yunikorn-core` — reads `ldap.*` keys from `configs.GetConfigMap()`, hot-reloads LDAP on config updates

**Status:** Implemented locally (not necessarily merged/published). Core changes require
a `go.mod` `replace` or version bump in k8shim to pick up the new core module.

---

## Problem (before)

1. Core LDAP config was read once from `common.LdapMountPath` (`/run/secrets/ldap`) as
   individual secret files (`Host`, `Port`, `BindUser`, etc.).
2. k8shim had no LDAP integration; deployment manifests did not mount an LDAP secret.
3. `UserGroupCache` is a process-wide singleton (`sync.Once`); LDAP config was baked into
   `LdapLookup` at creation with no reload path.
4. `updatePartitionDetails` on config reload did not refresh LDAP settings.
5. Partitions (and `UserGroupCache`) are created in `registerShimLayer()` **before**
   `InitializeState()` registers pods — LDAP must be available at registration time or
   core must support post-registration LDAP updates.

---

## Design decisions (confirmed)

| Decision | Choice |
|----------|--------|
| Secret name | `yunikorn-secret` (YuniKorn namespace) |
| Secret credential keys | `ldap.username`, `ldap.password` |
| ConfigMap / non-secret LDAP keys | `ldap.*` prefix (e.g. `ldap.Host`, `ldap.Port`) |
| Bind credentials source | Secret only → mapped to `ldap.BindUser`, `ldap.BindPassword` in ExtraConfig |
| Filesystem fallback (`/run/secrets/ldap`) | **Removed** |
| Missing secret at startup | **Warn and continue**; LDAP lookups fail until config is complete |
| Secret rotation | **Secret informer** in shim → push updated ExtraConfig to core |
| Partition YAML | Unchanged: `userGroupResolver.type: ldap` still enables LDAP resolver |

---

## Configuration contract

### Kubernetes Secret: `yunikorn-secret`

```yaml
stringData:
  ldap.username: "cn=admin,dc=example,dc=com"
  ldap.password: "change-me"
```

Example manifest: `yunikorn-k8shim/deployments/scheduler/yunikorn-secret.yaml`

### ConfigMap keys (non-credential)

These are flattened into shim config and pass through `GetExtraConfigFromConfigMap` (any
key not ending in `.yaml`):

| ExtraConfig key | Purpose |
|-----------------|--------|
| `ldap.Host` | LDAP server host |
| `ldap.Port` | Port (string, validated as int) |
| `ldap.BaseDN` | Search base DN |
| `ldap.Filter` | Search filter with `%s` for username |
| `ldap.GroupAttr` | Group attribute name |
| `ldap.ReturnAttr` | Comma-separated attributes to return |
| `ldap.Insecure` | TLS insecure skip verify (`true`/`false`) |
| `ldap.SSL` | Use LDAPS (`true`/`false`) |

### Shim → core mapping for credentials

| Secret data key | ExtraConfig key sent to core |
|-----------------|-------------------------------|
| `ldap.username` | `ldap.BindUser` |
| `ldap.password` | `ldap.BindPassword` |

### Partition YAML (unchanged)

```yaml
userGroupResolver:
  type: ldap
```

---

## Architecture / data flow

```
ConfigMap (ldap.*) ──┐
                     ├──> shim buildExtraConfig() ──> ExtraConfig map
Secret (yunikorn-secret) ──┘         │
                                     v
              RegisterResourceManager (initial)
              UpdateConfiguration (hot reload / secret rotation)
                                     │
                                     v
              core configs.SetConfigMap(ExtraConfig)
                     │
                     ├──> registered callbacks (logging, health, ldap, ...)
                     └──> ldap callback: UserGroupCache.UpdateLdapConfig() + reset cache
```

**Startup order (shim `Run()`):**
1. `registerShimLayer()` — loads ConfigMaps + Secret, sends `ExtraConfig` on RM registration
2. `InitializeState()` — registers pods; `convertUGI` may perform LDAP lookups if groups empty

LDAP config must be in `ExtraConfig` at registration time (or core accepts incomplete
config and updates via callback before lookups matter).

**Hot reload:** `processRMConfigUpdateEvent` calls `SetConfigMap` **before** YAML
checksum early-return, so LDAP-only changes still trigger callbacks even when queue YAML
is unchanged.

---

## yunikorn-core changes

### Key files

| File | Changes |
|------|---------|
| `pkg/common/configs/configs.go` | `PrefixLdap`, `LdapHostKey`, … `LdapSSLKey` constants |
| `pkg/common/constants.go` | Removed `LdapMountPath` variable |
| `pkg/common/security/usergroup_ldap_resolver.go` | Major rewrite (see below) |
| `pkg/common/security/usergroup.go` | `resolverType`, `ldapConfig` holder ref, `UpdateLdapConfig()` |
| `pkg/common/security/usergroup_test.go` | Test isolation helpers + resolver-scope comments |
| `pkg/common/security/usergroup_ldap_resolver_test.go` | Map-based config tests, LDAP mocks |

### `usergroup_ldap_resolver.go` behaviour

- **`ReadLdapConfigFromMap(configMap)`** — reads all `ldap.*` keys from ExtraConfig;
  strips prefix; validates via existing `ValidateSecretValue()`; requires all mandatory fields.
- **`configReaderImpl.ReadLdapConfig()`** — delegates to `ReadLdapConfigFromMap(configs.GetConfigMap())`.
- **`ldapConfigHolder`** — mutex-protected live config; `LdapLookup` reads through holder.
- **`GetUserGroupCacheLdap()`** — no longer fatals on missing config; logs warning; lookups
  fail until valid config arrives.
- **`init()` callback** — `configs.AddConfigMapCallback("ldap", updateLdapConfigFromExtraConfig)`:
  if singleton exists and `resolverType == Ldap`, re-read map and call `UpdateLdapConfig()`.
- **`UserGroupCache.UpdateLdapConfig()`** — updates holder, calls `resetCache()`.

### Removed

- Filesystem read from `/run/secrets/ldap` in `configReaderImpl`
- Fatal/panic when LDAP config missing at cache creation

---

## yunikorn-k8shim changes

### Key files

| File | Changes |
|------|---------|
| `pkg/common/utils/ldap_config.go` | `MergeLdapExtraConfig()`, constants |
| `pkg/common/utils/ldap_config_test.go` | Unit tests for merge |
| `pkg/client/interfaces.go` | `KubeClient.GetSecret()` |
| `pkg/client/kubeclient.go` | `GetSecret()` implementation |
| `pkg/client/kubeclient_mock.go` | Mock `GetSecret()` |
| `pkg/client/apifactory.go` | Namespace-scoped `SecretInformer`, `SecretInformerHandlers` |
| `pkg/client/clients.go` | Run/sync Secret informer (17 informers total) |
| `pkg/client/apifactory_mock.go`, `clients_test.go`, `apifactory_test.go` | Mock + count updates |
| `pkg/common/test/secret_informer_mock.go` | Test mock |
| `pkg/cache/context.go` | `buildExtraConfig`, `BuildExtraConfig`, `pushConfigurationToCore`, secret handlers |
| `pkg/shim/scheduler.go` | `BuildExtraConfig()` on registration |
| `deployments/scheduler/yunikorn-secret.yaml` | Example secret manifest |

### Shim behaviour

- **`buildExtraConfig(confMap)`** — merges ConfigMap `ldap.*` keys + Secret credentials;
  warns if secret missing/unreadable (does not fail startup).
- **`registerShimLayer()`** — includes merged LDAP keys in `RegisterResourceManagerRequest.ExtraConfig`.
- **`triggerReloadConfig()`** — refactored to call `pushConfigurationToCore()` with merged ExtraConfig.
- **Secret informer** — watches `yunikorn-secret` in YuniKorn namespace; on add/update/delete
  calls `triggerLdapSecretReload()` → `UpdateConfiguration` with current ConfigMaps + fresh secret
  (always reloads LDAP, independent of ConfigMap hot-refresh setting).

### RBAC

Scheduler Role already allows `get/list/watch` on secrets in the YuniKorn namespace
(`deployments/scheduler/yunikorn-rbac.yaml`). No RBAC change required.

---

## Testing

### Commands

```bash
# Core (includes race + deadlock tags)
cd yunikorn-core && make test

# Shim (selected packages)
cd yunikorn-k8shim && go test ./pkg/common/utils/... ./pkg/client/... ./pkg/cache/... ./pkg/shim/...
```

### Core test fixes (important for agents)

Prior table-driven tests did **not** reset the `UserGroupCache` singleton between subtests
(`sync.Once`), so only the first resolver type in each table was ever exercised. Fixing this
exposed false positives.

**Helpers added in `usergroup_test.go`:**
- `validLdapExtraConfig()` — full LDAP ExtraConfig for tests
- `stopUserGroupCacheIfRunning()` — `Stop()` + clear config map
- `prepareUserGroupCache(t, resolver)` — per-subtest setup with `t.Cleanup(Stop)`

**`ldapAccessForUserGroupTests()`** in `usergroup_ldap_resolver_test.go` — wires
`mockLdapSearchResult` into LDAP search mock.

**Resolver coverage by test** (comments added at each test function):

| Test | Resolvers used | Why |
|------|----------------|-----|
| `TestGetUserGroupCache` | All 4 | Construction + `Stop` only; no lookups |
| `TestGetUserGroup` | Test + LDAP | Mock-backed lookup expectations |
| `TestBrokenUserGroup` | Test only | Test-mock-specific edge cases |
| `TestGetUserGroupFail` | Test only | Failed lookup / negative-cache semantics |
| `TestCacheCleanUp` | Test + LDAP | Manual cleanup with success + failure entries |
| `TestIntervalCacheCleanUp` | Test + LDAP | Background cleaner goroutine |
| `TestConvertUGI` | Test + LDAP | Delegates to `GetUserGroup` when groups absent |
| `TestCleanUpCacheUsesRealTime` | Test only | Seeds cache with testuser1 |
| `TestPositiveCacheHitExpiryTriggersRefresh` | Test only | Re-resolution via test mock |
| `TestUpdateLdapConfigFromExtraConfig` | LDAP only | ExtraConfig callback |
| `TestUserGroupCacheLdap` | LDAP only | LDAP cache wiring |

**OsResolver / UnknownResolver** removed from lookup tests because:
- **OS:** depends on local `/etc/passwd` users (`testuser1`, etc.)
- **Unknown:** does not fail lookups or resolve groups like test mock expects

---

## Dependency note

k8shim `go.mod` pins a remote `yunikorn-core` module. Local core changes require:

```go
replace github.com/apache/yunikorn-core => ../yunikorn-core
```

until core is published and the dependency is bumped.

---

## Operational notes

1. **Admission controller path:** Pods with pre-populated user-info annotation skip LDAP;
   LDAP matters for username-only flows (label) and queue ACL / user limits keyed on groups.
2. **Logging:** Ensure bind passwords are never logged (audit existing LDAP debug logs).
3. **Secret-only rotation:** Secret informer triggers core update without ConfigMap change.
4. **Resolver type changes** (`""` → `ldap`) in partition YAML still require restart
   (out of scope; singleton resolver type fixed at first `GetUserGroupCache` call).

---

## Possible follow-ups (not implemented)

- Document `ldap.*` ConfigMap keys in user-facing deployment docs / README
- E2E test with LDAP resolver + `yunikorn-secret`
- Dedicated OS/unknown resolver unit tests with resolver-specific fixtures
- Mount `yunikorn-secret` in `deployments/scheduler/scheduler.yaml` (optional if using API read only)

---

## Related code references

**Core config update entry points:**
- `pkg/scheduler/context.go` — `processRMRegistrationEvent`, `processRMConfigUpdateEvent`
  both call `configs.SetConfigMap(event.*.ExtraConfig)`

**Core LDAP usage:**
- `pkg/scheduler/partition.go` — `GetUserGroupCache(conf.UserGroupResolver, ...)`
- `pkg/scheduler/context.go` — `partition.convertUGI()` on application accept

**Shim config send:**
- `pkg/shim/scheduler.go` — `registerShimLayer()`
- `pkg/cache/context.go` — `triggerReloadConfig()`, `triggerLdapSecretReload()`
