-- Retrieve all CDR sequences for selected entity(ies) in a single row per entity
-- This query joins entities with their variable chains (VH and VL) to extract CDR sequences
WITH
  entity_chains AS (
    -- Get the VH and VL chain IDs for each entity
    SELECT
      e.id AS entity_id,
      e.file_registry_id$ AS entity_registry_id,
      e.vh AS vh_id,
      e.vl AS vl_id
    FROM
      invenra.entity$raw e
    WHERE
      e.archived$ = FALSE
      -- Filter by selected entity(ies), or show all if none selected
      AND 
      
     ( e.file_registry_id$ = ANY (
    string_to_array({{entity_ids}}, ' ')) OR {{entity_ids}} is null )
  
  )
SELECT
  ec.entity_registry_id AS "Entity ID",
  -- VH CDR sequences
  vh.cdr1 AS "VH CDR1",
  vh.cdr2 AS "VH CDR2",
  vh.cdr3 AS "VH CDR3",
  -- VL CDR sequences
  vl.cdr1 AS "VL CDR1",
  vl.cdr2 AS "VL CDR2",
  vl.cdr3 AS "VL CDR3",
  -- Concatenated VH CDRs (handling NULLs to avoid entire concatenation becoming NULL)
  CONCAT(
    COALESCE(vh.cdr1, ''),
    COALESCE(vh.cdr2, ''),
    COALESCE(vh.cdr3, '')
  ) AS "VH CDRs Concatenated",
  -- Concatenated VL CDRs (handling NULLs to avoid entire concatenation becoming NULL)
  CONCAT(
    COALESCE(vl.cdr1, ''),
    COALESCE(vl.cdr2, ''),
    COALESCE(vl.cdr3, '')
  ) AS "VL CDRs Concatenated",
  -- All CDRs concatenated in one line (VH CDR1, VH CDR2, VH CDR3, VL CDR1, VL CDR2, VL CDR3)
  CONCAT(
    COALESCE(vh.cdr1, ''),
    COALESCE(vh.cdr2, ''),
    COALESCE(vh.cdr3, ''),
    COALESCE(vl.cdr1, ''),
    COALESCE(vl.cdr2, ''),
    COALESCE(vl.cdr3, '')
  ) AS "VHVL CDRs Concatenated"
FROM
  entity_chains ec
  -- Join with VH variable chain
  LEFT JOIN invenra.chain$raw vh ON vh.id = ec.vh_id
  AND vh.archived$ = FALSE
  -- Join with VL variable chain
  LEFT JOIN invenra.chain$raw vl ON vl.id = ec.vl_id
  AND vl.archived$ = FALSE
ORDER BY
  ec.entity_registry_id
