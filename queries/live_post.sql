/* live_post.sql — demo subset (9 entities) */

WITH demo_entities AS (
  SELECT unnest(ARRAY[
    3191,   -- Ajax (team)
    1238,   -- Finn Dahmen
    19704,  -- Noahkai Banks
    19705,  -- Keven Schlotterbeck
    19703,  -- Sandro Wagner
    762,    -- Bakéry Jatta
    3476,   -- Álvaro Arbeloa
    825,    -- Cedric Zesiger
    841     -- Cedric Zesiger (duplicate entity id)
  ])::int AS id
)
SELECT
  e.id          AS entity_id,
  e.name        AS entity_name,
  e.entity_type,
  e.entity_sport,
  e.geo_country,
  e.tag,
  0::int        AS mentions_count,
  0::int        AS posts_count,
  0::bigint     AS engagement_total,
  0::float      AS avg_sentiment,
  now()         AS demo_generated_at
FROM entities e
JOIN demo_entities d ON d.id = e.id
ORDER BY e.entity_type, e.name;