/* pre_game.sql — demo subset from entities (stable for runner) */

WITH demo_entities AS (
  SELECT unnest(ARRAY[
    3191, 1238, 19704, 19705, 19703, 762, 3476, 825, 841
  ])::int AS id
)
SELECT
  e.id          AS entity_id,
  e.name        AS entity_name,
  e.entity_type AS entity_type,
  e.entity_sport AS entity_sport,
  e.geo_country AS geo_country,
  e.geo_state   AS geo_state,
  e.geo_city    AS geo_city,
  e.tag         AS tag,
  now()         AS demo_generated_at
FROM entities e
JOIN demo_entities d ON d.id = e.id
ORDER BY e.entity_type, e.name;