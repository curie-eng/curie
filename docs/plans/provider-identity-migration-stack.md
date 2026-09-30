# Provider identity migration stack

The integration order is provider installations (#3040), identity links
(#3054), then tenant scope of the core tables (#3052). This preserves the
existing step 5 before step 6 ordering. It changes no identity resolution or
caller admission policy.

- PI-STACK-1: The migration chain is 0072 provider installations, 0073 identity
  links, then 0074 tenant scope. Each branch has exactly one Alembic head.
- PI-STACK-2: Revision 0073 owns
  `provider_installations_tenant_id_id_key`. Revision 0074 reuses that key for
  channel bindings, without creating it again or dropping it on downgrade.
- PI-STACK-3: Upgrading through 0074 and downgrading to 0073 preserves the
  identity links table, its installation foreign key, and its linked rows.
  An installation referenced by a link remains protected by that foreign key.
- PI-STACK-4: The existing plain bot foreign key remains unchanged by this
  integration. Tenant enforcement and bot-link hardening are not added here.
  The existing resolver tenant predicates and route triple selection remain.

The rollback floor remains 0070. Released version windows stay unchanged;
only the candidate window advances. PR #3052 depends on #3054, which depends
on #3040. When a prerequisite merges, retarget the next surviving PR and run
its gates against the resulting base before merging it.
