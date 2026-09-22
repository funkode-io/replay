-- no-transaction
--
-- Keep the status read off the heap now that it reads `last_parked_at`.
--
-- `PolicyStatus` aggregates `COUNT(*)` and the policy's most recent parking in
-- one pass over `policy_dead_letters`. While that was `MAX(created_at)`,
-- `idx_dead_letters_policy` covered both and the pass was index-only; reading
-- `last_parked_at` instead (funkode-io/replay#220) took a column the index does
-- not carry, so every status poll fetched the policy's parked rows from the heap
-- — a backlog's worth of random reads on a call a consumer's health endpoint
-- makes on a timer.
--
-- INCLUDE rather than a second key column: nothing orders or filters on
-- `last_parked_at`, it is only read, and a payload column keeps the index the
-- same shape for the triage query that does order on `created_at`. 0032 drops
-- the index this one replaces.
--
-- CONCURRENTLY, and no IF NOT EXISTS, for the reasons 0026 and 0030 state.
CREATE INDEX CONCURRENTLY idx_dead_letters_policy_created_parked
    ON policy_dead_letters (policy_name, created_at DESC) INCLUDE (last_parked_at);
