-- Which stream types a Policy tracks (funkode-io/replay#267).
--
-- A Policy whose filter is `StreamFilter::ForStreamTypes(types)` compares, reads and keeps
-- places for the streams of those types only:
-- docs/adr/0030-a-policy-filtered-by-stream-type-tracks-only-those-types.md. The runner
-- records the types here each time a worker takes the Policy on, so the status read model
-- — which has no Policy to ask — counts lag over the same streams the Policy tracks.
--
-- NULL is every stream: what a Policy under any other filter tracks, and what a row
-- written before this column existed means until its Policy next starts.
ALTER TABLE policy_cursors
    ADD COLUMN IF NOT EXISTS tracked_stream_types TEXT[];
