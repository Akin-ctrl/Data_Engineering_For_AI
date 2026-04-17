-- Day 3 agent query views
-- Purpose: provide stable, reusable SQL surfaces that an agent can query quickly.

BEGIN;

SET search_path TO training_data, public;

-- 1) Core paper surface for retrieval and filtering.
CREATE OR REPLACE VIEW training_data.v_agent_papers AS
SELECT
    cp.paper_key,
    cp.versioned_id,
    cp.title,
    cp.summary,
    cp.primary_category,
    cp.categories_json,
    cp.author_count,
    cp.first_author,
    cp.arxiv_url,
    cp.pdf_url,
    cp.doi,
    cp.journal_ref,
    cp.comment,
    cp.published_at,
    cp.updated_at,
    cp.loaded_at
FROM training_data.clean_papers cp;

-- 2) Category distribution snapshot.
CREATE OR REPLACE VIEW training_data.v_agent_category_counts AS
SELECT
    cp.primary_category,
    count(*) AS paper_count
FROM training_data.clean_papers cp
GROUP BY cp.primary_category;

-- 3) Monthly category trend for time-series questions.
CREATE OR REPLACE VIEW training_data.v_agent_monthly_category_counts AS
SELECT
    date_trunc('month', cp.published_at) AS month,
    cp.primary_category,
    count(*) AS paper_count
FROM training_data.clean_papers cp
GROUP BY 1, 2;

-- 4) Global author frequency (deduplicated by paper).
CREATE OR REPLACE VIEW training_data.v_agent_author_frequency AS
SELECT
    pa.author_name,
    count(DISTINCT pa.paper_key) AS paper_count,
    min(cp.published_at) AS first_published_at,
    max(cp.published_at) AS last_published_at
FROM training_data.paper_authors pa
JOIN training_data.clean_papers cp
    ON cp.paper_key = pa.paper_key
GROUP BY pa.author_name;

-- 5) Author frequency by primary category.
CREATE OR REPLACE VIEW training_data.v_agent_author_category_frequency AS
SELECT
    pa.author_name,
    cp.primary_category,
    count(DISTINCT pa.paper_key) AS paper_count
FROM training_data.paper_authors pa
JOIN training_data.clean_papers cp
    ON cp.paper_key = pa.paper_key
GROUP BY pa.author_name, cp.primary_category;

-- 6) Metadata quality profile per paper for ranking/filtering.
CREATE OR REPLACE VIEW training_data.v_agent_metadata_quality AS
SELECT
    cp.paper_key,
    cp.primary_category,
    cp.published_at,
    (CASE WHEN cp.summary IS NOT NULL AND btrim(cp.summary) <> '' THEN 1 ELSE 0 END
     + CASE WHEN cp.doi IS NOT NULL AND btrim(cp.doi) <> '' THEN 1 ELSE 0 END
     + CASE WHEN cp.journal_ref IS NOT NULL AND btrim(cp.journal_ref) <> '' THEN 1 ELSE 0 END
     + CASE WHEN cp.comment IS NOT NULL AND btrim(cp.comment) <> '' THEN 1 ELSE 0 END) AS metadata_score
FROM training_data.clean_papers cp;

-- 7) Pipeline health summary for observability questions.
CREATE OR REPLACE VIEW training_data.v_agent_pipeline_health AS
SELECT
    ps.source_name,
    ps.watermark_published_at,
    ps.watermark_paper_key,
    ps.last_batch_id,
    ps.updated_at AS pipeline_updated_at,
    (SELECT count(*) FROM training_data.clean_papers) AS clean_paper_rows,
    (SELECT count(*) FROM training_data.paper_authors) AS author_rows,
    (SELECT count(*) FROM training_data.raw_arxiv_entries) AS raw_rows,
    (SELECT count(*) FROM training_data.rejected_arxiv_entries) AS rejected_rows,
    (SELECT max(loaded_at) FROM training_data.clean_papers) AS latest_clean_loaded_at
FROM training_data.pipeline_state ps;

-- 8) Recent paper view for recency-biased agent prompts.
CREATE OR REPLACE VIEW training_data.v_agent_recent_papers AS
SELECT
    cp.paper_key,
    cp.title,
    cp.summary,
    cp.primary_category,
    cp.first_author,
    cp.author_count,
    cp.published_at,
    cp.updated_at,
    cp.arxiv_url,
    cp.pdf_url
FROM training_data.clean_papers cp
WHERE cp.published_at >= now() - interval '180 days';

-- 9) Expensive keyword aggregation as materialized view.
DROP MATERIALIZED VIEW IF EXISTS training_data.mv_agent_keyword_frequency;

CREATE MATERIALIZED VIEW training_data.mv_agent_keyword_frequency AS
WITH tokens AS (
    SELECT
        lower(token) AS word
    FROM training_data.clean_papers cp
    CROSS JOIN LATERAL regexp_split_to_table(
        regexp_replace(coalesce(cp.title, '') || ' ' || coalesce(cp.summary, ''), '[^a-zA-Z0-9 ]', ' ', 'g'),
        '\\s+'
    ) AS token
)
SELECT
    t.word,
    count(*) AS frequency
FROM tokens t
WHERE t.word <> ''
  AND length(t.word) >= 3
  AND t.word NOT IN (
      'the','and','for','with','that','this','from','are','was','were','have','has','had',
      'into','over','under','than','then','our','your','their','there','here','also','using',
      'use','used','new','via','toward','towards','can','may','not'
  )
GROUP BY t.word;

CREATE INDEX IF NOT EXISTS idx_mv_agent_keyword_frequency_word
    ON training_data.mv_agent_keyword_frequency (word);

-- 10) Category co-occurrence graph as materialized view.
DROP MATERIALIZED VIEW IF EXISTS training_data.mv_agent_category_cooccurrence;

CREATE MATERIALIZED VIEW training_data.mv_agent_category_cooccurrence AS
WITH exploded AS (
    SELECT
        cp.paper_key,
        jsonb_array_elements_text(cp.categories_json) AS category
    FROM training_data.clean_papers cp
), pairs AS (
    SELECT
        e1.category AS category_a,
        e2.category AS category_b,
        e1.paper_key
    FROM exploded e1
    JOIN exploded e2
        ON e1.paper_key = e2.paper_key
       AND e1.category < e2.category
)
SELECT
    p.category_a,
    p.category_b,
    count(*) AS co_paper_count
FROM pairs p
GROUP BY p.category_a, p.category_b;

CREATE INDEX IF NOT EXISTS idx_mv_agent_category_cooccurrence_pair
    ON training_data.mv_agent_category_cooccurrence (category_a, category_b);

COMMIT;

-- Refresh materialized views after new ingestions:
-- REFRESH MATERIALIZED VIEW training_data.mv_agent_keyword_frequency;
-- REFRESH MATERIALIZED VIEW training_data.mv_agent_category_cooccurrence;