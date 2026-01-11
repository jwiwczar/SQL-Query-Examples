/*
Author: Jessica Wiwczar
*/
-- QUERY PARAMETER (KEEP AT TOP) = TOP QUERY!
WITH
  processScale AS (
    SELECT
      sp.id AS "id",
      sp.name$ AS "process",
      sp.entity AS "spentity",
      sp.format,
      sp.created_at$ AS "process_registered_at" -- Capture process registration timestamp
    FROM
      invenra.source_process$raw AS sp
    WHERE
    
      sp.final_protein_complex_mw_computed
      BETWEEN {{ MW target }} - 25000
          AND {{ MW target }} + 25000
     -- sp.format = '1x1'
      --AND sp.name$ = ANY (string_to_array({{Processes}}, ' '))
     -- AND sp.created_at$::DATE >= (DATE (NOW()) - 1095)::DATE
      AND sp.volume_ml != 96
      AND sp.volume_ml != 192
  ),
  FAB AS (
    WITH
    
    Fc1 AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          dnax.id as "Fc1DNAx",
          dnax.id as "Fc1DNAp",
          part.file_registry_id$ AS "Fc1part",
          part.id AS "Fc Chain1",
          part.descriptive_name AS "Fc1name",
    entity.file_registry_id$ AS "Fc1entityID"
        FROM
        
                          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vl = var.id
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (   part.file_registry_id$ = 'PART_001'
            OR part.file_registry_id$ = 'PART_002'
            OR part.file_registry_id$ = 'PART_003'
            OR part.file_registry_id$ = 'PART_004'
            OR part.file_registry_id$ = 'PART_005'
            OR part.file_registry_id$ = 'PART_006'
            OR part.file_registry_id$ = 'PART_007')
        WHERE
          dnax.expressed_protein_mw_computed > 30000

      ),
      Fc3 AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          dnax.id as "Fc3DNAx",
          dnax.id as "Fc3DNAp",
          part.file_registry_id$ AS "Fc3part",
          part.id AS "Fc Chain3",
          part.descriptive_name AS "Fc3name",
          entity.file_registry_id$ AS "Fc3entityID"

        FROM
                  processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vl = var.id
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (   part.file_registry_id$ = 'PART_008'
            OR part.file_registry_id$ = 'PART_003'
            OR part.file_registry_id$ = 'PART_009'
            OR part.file_registry_id$ = 'PART_010'
            OR part.file_registry_id$ = 'PART_004'
            OR part.file_registry_id$ = 'PART_011'
            OR part.file_registry_id$ = 'PART_012'
            OR part.file_registry_id$ = 'PART_013')
        WHERE
          dnax.expressed_protein_mw_computed > 30000
        
 
      ),
    
      FabMab AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          part.id AS "mAb Fab",
          part.file_registry_id$ AS "Mpart",
          part.descriptive_name AS "Mname",
          entity.id AS "MEntity",
          entity.file_registry_id$ AS "MentityID",
          entity.bispecific_target_descriptive_naming AS "Mtarget",
          dnax.variable_chain_dna_subsequences AS "MDNAxVar",
          dnax.entity AS "MDNAxEntity",
          dnax.id AS "MDNAx",
          dnax.id AS "MDNAp",
          SUBSTRING(
            entity.file_registry_id$
            FROM
              'E([0-9]+)'
          )::INTEGER AS "M Entity Number"
        FROM
          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vl = var.id
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (part.file_registry_id$ = 'PART_014')
        WHERE
          dnax.expressed_protein_mw_computed < 30000
      ),
      FabB AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          part.id AS "B-Body Fab",
          part.file_registry_id$ AS "Bpart",
          part.descriptive_name AS "Bname",
          dnax.variable_chain_dna_subsequences AS "BDNAxVar",
          var.name$ AS "Bvar",
          entity.vh,
          entity.entity_number,
          entity.id AS "BEntity",
          entity.file_registry_id$ AS "BentityID",
          entity.bispecific_target_descriptive_naming AS "Btarget",
          dnax.entity AS "BDNAxEntity",
          dnax.id AS "BDNAx",
          dnax.id AS "BDNAp",
          CASE
            WHEN part.file_registry_id$ = 'PART_015' THEN 'B1/B2'
            WHEN part.file_registry_id$ = 'PART_016' THEN 'B1/B2'
            ELSE 'other'
          END AS "Part B",
          SUBSTRING(
            entity.file_registry_id$
            FROM
              'E([0-9]+)'
          )::INTEGER AS "B Entity Number"
        FROM
          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vh = var.id
            AND (
              jsonb_array_length(sp.entity) = 0
              OR sp.entity ? entity.id
            )
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (
            part.file_registry_id$ = 'PART_015'
            OR part.file_registry_id$ = 'PART_016'
            OR part.file_registry_id$ = 'PART_017'
          )
        WHERE
          dnax.expressed_protein_mw_computed < 30000
          AND entity.archived$ = FALSE
      ),
      FabA AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          part.id AS "Ab-like Fab",
          part.file_registry_id$ AS "Apart",
          part.descriptive_name AS "Aname",
          dnax.variable_chain_dna_subsequences AS "ADNAxVar",
          var.name$ AS "Avar",
          dnax.entity AS "ADNAxEntity",
          dnax.id AS "ADNAx",
          dnax.id AS "ADNAp",
          entity.vh,
          entity.entity_number,
          entity.id AS "AEntity",
          entity.file_registry_id$ AS "AentityID",
          entity.bispecific_target_descriptive_naming AS "Atarget",
          CASE
            WHEN part.file_registry_id$ = 'PART_018' THEN 'ck/CH1'
            WHEN part.file_registry_id$ = 'PART_014' THEN 'ck/CH1'
            ELSE 'other'
          END AS "Part A",
          SUBSTRING(
            entity.file_registry_id$
            FROM
              'E([0-9]+)'
          )::INTEGER AS "A Entity Number"
        FROM
          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vh = var.id
            AND (
              jsonb_array_length(sp.entity) = 0
              OR sp.entity ? entity.id
            )
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (
            part.file_registry_id$ = 'PART_018'
            OR part.file_registry_id$ = 'PART_014'
          )
        WHERE
          dnax.expressed_protein_mw_computed < 30000
          AND entity.file_registry_id$ != 'ENTITY_003'
          AND entity.archived$ = FALSE
      ),
      FabT AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          part.id AS "Tri Fab",
          part.file_registry_id$ AS "Tpart",
          part.descriptive_name AS "Tname",
          dnax.variable_chain_dna_subsequences AS "TDNAxVar",
          var.name$ AS "Tvar",
          dnax.entity AS "TDNAxEntity",
          entity.vh,
          entity.entity_number,
          entity.id AS "TEntity",
          entity.file_registry_id$ AS "TentityID",
          entity.bispecific_target_descriptive_naming AS "Ttarget",
          dnax.id AS "TDNAx",
          dnax.id AS "TDNAp",
          CASE
            WHEN part.file_registry_id$ = 'PART_019' THEN 'T1/T2'
            WHEN part.file_registry_id$ = 'PART_020' THEN 'T1/T2 Tmopt'
            ELSE 'other'
          END AS "Part T",
          SUBSTRING(
            entity.file_registry_id$
            FROM
              'E([0-9]+)'
          )::INTEGER AS "T Entity Number"
        FROM
          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          LEFT JOIN invenra.chain$raw AS var ON dnax.variable_chain_dna_subsequences ? var.id
          LEFT JOIN invenra.entity$raw AS entity ON (
            entity.vh = var.id
            AND (
              jsonb_array_length(sp.entity) = 0
              OR sp.entity ? entity.id
            )
            AND entity.file_registry_id$ != 'ENTITY_001'
            AND entity.file_registry_id$ != 'ENTITY_002'
          )
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (
            part.file_registry_id$ = 'PART_019'
            OR part.file_registry_id$ = 'PART_020'
            OR part.file_registry_id$ = 'PART_021'
          )
        WHERE
          dnax.expressed_protein_mw_computed < 30000
          OR dnax.expressed_protein_mw_computed IS NULL
          AND entity.archived$ = FALSE
      ),
      link AS (
        SELECT DISTINCT
          sp.name$ AS "process",
          dnap.id AS "LDNAp",
          dnax.id AS "LDNAx",
          part.file_registry_id$ AS "Lpart",
          part.id AS "Linker",
          part.descriptive_name AS "Lname",
          part.part_type,
          aa_seq.amino_acids AS "Amino Acid Sequence"
        FROM
          processScale
          INNER JOIN invenra.source_process$raw AS sp ON sp.id = processScale.id
          LEFT JOIN invenra.expression_dna_prep$raw AS dnap ON sp.expression_dna_preps ? dnap.id
          LEFT JOIN invenra.expression_dna$raw AS dnax ON dnap.expression_dna = dnax.id
          INNER JOIN invenra.part$raw AS part ON dnax.part_subsequences ? part.id
          AND (part.part_type = 'Linker')
          LEFT JOIN invenra.final_product_aa_sequence$raw fpaa ON part.aa_sequences = fpaa.id
          LEFT JOIN invenra.bnch$aa_sequence$raw aa_seq ON fpaa.id = aa_seq.id
        WHERE
          dnax.expressed_protein_mw_computed > 30000
      )
    SELECT
      processscale.*,
      "MentityID",
      "BentityID",
      "AentityID",
      "TentityID",
   "M Entity Number",
      "B Entity Number",
      "A Entity Number",
      "T Entity Number"
    FROM
      processScale
      LEFT JOIN FabMab ON FabMab."process" = processScale."process"
      LEFT JOIN FabB ON FabB."process" = processScale."process"
      LEFT JOIN FabA ON FabA."process" = processScale."process"
      LEFT JOIN FabT ON FabT."process" = processScale."process"
      LEFT JOIN link ON link."process" = processScale."process"
      LEFT JOIN Fc1 ON Fc1."process" = processScale."process"
      LEFT JOIN Fc3 ON Fc3."process" = processScale."process"
      WHERE ("BentityID" = "Fc3entityID" OR "BentityID" = "Fc1entityID" OR "BentityID" IS NULL)
      AND ("AentityID" = "Fc3entityID" OR "AentityID" = "Fc1entityID" OR "AentityID" IS NULL)
      AND ("TentityID" = "Fc3entityID" OR "TentityID" = "Fc1entityID" OR "TentityID" IS NULL)
  )
  -- Wrap the main query in a subquery to filter on the calculated "Both Orientations?" column
SELECT
  *
FROM
  (
    SELECT DISTINCT
      fab.*
    
    FROM
      fab
  ) AS results
--WHERE
  -- Filter to only show rows where both orientations exist
 -- "Both Orientations?" = 'Yes'
ORDER BY
  "process" ASC
