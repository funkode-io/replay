-- A `global_position` must already identify exactly one event before 0015 can enforce it.
--
-- Split from the build for two reasons. `CREATE INDEX CONCURRENTLY` cannot run in a
-- transaction block, and Postgres wraps a multi-statement migration in one, so the build
-- has to be a file of its own. And a concurrent build that fails leaves an *invalid*
-- index behind: a database that does contain duplicates should be told which positions
-- they are, not handed debris. Checking first means the build is never reached.
--
-- Normal appends cannot have produced a duplicate — they leave the column to its
-- `BIGSERIAL` default and a sequence never hands out a value twice — so this is expected
-- to find nothing. It fires for a database someone wrote positions into by hand, or
-- restored by copying rows between installations.
DO $$
DECLARE
    shared_positions bigint;
    sample           text;
BEGIN
    SELECT count(*)
      INTO shared_positions
      FROM (SELECT 1 FROM events GROUP BY global_position HAVING count(*) > 1) AS duplicated;

    IF shared_positions > 0 THEN
        -- Capped at 20 so a database with a million duplicates still raises a readable
        -- message; `shared_positions` carries the true size.
        SELECT string_agg(global_position::text, ', ' ORDER BY global_position)
          INTO sample
          FROM (SELECT global_position
                  FROM events
                 GROUP BY global_position
                HAVING count(*) > 1
                 ORDER BY global_position
                 LIMIT 20) AS duplicated;

        RAISE EXCEPTION
            'events.global_position is not unique: % position(s) are held by more than one event (%)',
            shared_positions, sample
            USING HINT = 'Move each duplicated event to a position no event holds, or '
                         'delete the copy, then run the migration again.';
    END IF;
END $$;
