-- The access path a retry uses: the reaction, not the row.
--
-- A bulk retry pages the policy's parked reactions on
-- `(policy_name, (global_position, event_id))` and then reads each group by
-- `(policy_name, global_position, event_id)` (funkode-io/replay#211). The only
-- index on the active table was `(policy_name, created_at DESC)`, so every group
-- read rescanned the policy's backlog: O(N^2) over a backlog of N reactions.
--
-- One index serves both because `event_id` and `global_position` come from the
-- same event: the page's keyset is a prefix, the group's filter is the full key,
-- and `id` last keeps the group's `ORDER BY id` inside the index.
CREATE INDEX IF NOT EXISTS idx_dead_letters_policy_reaction
    ON policy_dead_letters (policy_name, global_position, event_id, id);
