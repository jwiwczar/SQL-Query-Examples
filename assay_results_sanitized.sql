-- =============================================================================
-- Query: Protein Purification QC Dashboard - Multi-Sample Reporting
-- Author: Jessica Wiwczar
-- Written: ~2023–2025 (real production query)
-- Purpose: Pull comprehensive QC data (expression, yield, purity, stability, polyreactivity, etc.)
--          for multiple final products from selected source batches in one view
--          This became the backbone for team dashboards — the only practical way 
--          to review batches of samples without opening each one individually
-- Key achievement: Moving MW filter + volume exclusion to top CTE (sourceBatchFilter) 
--                  dramatically improved performance
-- Database: PostgreSQL
-- Note: This is intentionally not "perfectly refactored" — it's the real workhorse version
-- =============================================================================

--QUERY PARAMETER (KEEP AT TOP) = TOP QUERY!
--!add a column that includes the descriptive identity to the right of the Source Batch ID
WITH
  -- Early filtering here was the big performance win — push criteria as high as possible
  sourceBatchFilter AS (
    SELECT
      sb.id,
      sb.name$ AS "source_batch",
      sb.created_at$
    FROM
      source_batch AS sb
    WHERE
      sb.final_protein_complex_mw_computed
        BETWEEN {{ MW target }} - 25000
        AND {{ MW target }} + 25000
      AND
        --sb.project = {{Project}}
        --sb.name$ = ANY (string_to_array({{SourceBatches}}, ' '))
        --  sb.created_at$ >= CURRENT_TIMESTAMP - INTERVAL '1095 days'
        --  AND
        (
          sb.volume_ml != 96
          AND sb.volume_ml != 192
        )
  ),
  lot AS (
    WITH
      -- Get the most recent final product version per source batch
      ranked_products AS (
        -- Rank products within each source batch by creation date
        -- Only the most recent (rn=1) will be used in final results
        SELECT
          fp.*,
          ROW_NUMBER() OVER (
            PARTITION BY
              fp.source_batch_id
            ORDER BY
              fp.created_at$ DESC
          ) AS rn
        FROM
          sourceBatchFilter sb
          INNER JOIN final_product$raw fp ON fp.source_batch_id = sb.id
      )
    SELECT
      c.id,
      c.parent_product_id
    FROM
      ranked_products c
      -- JOIN source_batch$raw t ON t.id = c.source_batch_id
      LEFT JOIN final_product$raw p ON p.id = c.parent_product_id
      LEFT JOIN new_qc_normalizations$raw AS norm ON c.id = norm.sample
    WHERE
      -- Only keep the latest product per source batch
      c.rn = 1
  ),
  PS AS (
    WITH
      -- Core results: source batch metadata + yields + basic analytics
      psresults AS (
        SELECT
          sb.id AS "Source Batch ID",
          sb.name$ AS "Source Batch Name",
          sb.source_batch_date,
          t.actual_supernatant_harvest_date,
          t.source_batch_scientist,
          sb.project AS "Project",
          sb.source_batch_ratio_chain_1234 AS "DNA ratio",
          sb.expression_system AS "Cell",
          sb.afucosylated AS "Afuco",
          sb.volume_ml AS "Vol",
          sb.format AS "Format",
          sb.final_protein_complex_pi_computed AS "PI",
          sb.final_protein_complex_mw_computed AS "MW",
          sb.final_protein_complex_ext_coeff_computed AS "Ext. Coeff",
          fp.id AS "Final Product ID",
          parentfp.id AS "Parent Product ID",
          CASE
            WHEN fp_results.expression_yield_ugml IS NULL
              THEN fpres_parent.expression_yield_ugml
            ELSE fp_results.expression_yield_ugml
          END AS "Exp. ug/mL",
          CASE
            WHEN fp.parent_product_id IS NULL
              THEN fp_results.total_protein_quantity_mg
            ELSE fpres_parent.total_protein_quantity_mg
          END AS "Yield off Capture",
          fp_results.total_protein_quantity_mg AS "Final Yield mg",
          fp_results.corrected_concentration_mgml AS "Conc. mg/mL",
          uncle.tm1_c AS "Tm",
          uncle.z_ave_dia_nm AS "z-ave diameter",
          uncle.pdi AS "PDI",
          uncle.tagg_266_c AS "Tagg 266",
          uncle.tagg_473_c AS "Tagg 473",
          lcms.comment AS "LC-MS",
        FROM
          lot
          INNER JOIN final_product AS fp ON fp.id = lot.id
          INNER JOIN source_batch AS sb ON sb.id = fp.source_batch_id
          LEFT JOIN cell_biology_source_batch_results$raw AS t
            ON t.source_batch_id = sb.id
          LEFT JOIN a_final_product_results$raw AS fp_results
            ON fp_results.final_product_id = fp.id
          LEFT JOIN final_product AS parentfp
            ON parentfp.id = fp.parent_product_id
          LEFT JOIN a_final_product_results$raw AS fpres_parent
            ON fpres_parent.final_product_id = parentfp.id
          LEFT JOIN uncle$raw AS uncle
            ON uncle.sample_id = fp.id
          LEFT JOIN lc_ms$raw AS lcms
            ON lcms.sample_id = fp.id
          LEFT JOIN polishing_results$raw
            ON polishing_results$raw.final_product_id = fp.id
          LEFT JOIN endotoxin$raw AS endo ON endo.sample = fp.id
      ),
      -- === Polyreactivity, AC-SINS, CE, HPLC assays (current + parent) ===
      -- (kept separate CTEs because each has slightly different logic and full outer joins)
      -- Note: Column names prefixed with 'p' (e.g., pPolyIns, pAC-SINS, pCE, pSEC) indicate values from the parent entity
      polyreactivity AS (
        SELECT
          fp.id AS "Final Product ID",
          poly.insulin_positive_control AS "PolyIns %Pos",
          poly.insulin_average_od AS "PolyIns OD",
          poly.insulin_cv AS "PolyIns CV",
          entrypoly.display_id AS "PolyINS Exp ID",
          poly.dna_positive_control AS "PolyDNA %Pos",
          poly.dna_average_od AS "PolyDNA OD",
          poly.DNA_cv AS "PolyDNA CV"
        FROM
          lot
          INNER JOIN final_product AS fp ON fp.id = lot.id
          FULL OUTER JOIN ps_polyreactivity$raw AS poly
            ON fp.id = poly.final_product_id
          LEFT JOIN entry$raw AS entrypoly
            ON entrypoly.id = poly.entry_id$
      ),
      Ppolyreactivity AS (
        SELECT
          fp.id AS "Final Product ID",
          poly.insulin_positive_control AS "pPolyIns %Pos",
          poly.insulin_average_od AS "pPolyIns OD",
          poly.dna_positive_control AS "pPolyDNA %Pos",
          poly.dna_average_od AS "pPolyDNA OD"
        FROM
          lot
          INNER JOIN final_product AS fp
            ON fp.id = lot.parent_product_id
          FULL OUTER JOIN ps_polyreactivity$raw AS poly
            ON fp.id = poly.final_product_id
          LEFT JOIN entry$raw AS entrypoly
            ON entrypoly.id = poly.entry_id$
      ),
      ac_sins AS (
        SELECT
          fp.id AS "Final Product ID",
          acsins.average_max_nm_od AS "AC-SINS Average nm",
          acsins.wavelength_shift_relative_to_pbs_control
            AS "AC-SINS nm ShiftPBSc",
          acsins.positive_control AS "AC-SINS %PosControl"
        FROM
          lot
          INNER JOIN final_product AS fp ON fp.id = lot.id
          FULL OUTER JOIN ps_ac_sins$raw AS acsins
            ON fp.id = acsins.final_product_id
          LEFT JOIN entry$raw AS entryacsins
            ON entryacsins.id = acsins.entry_id$
      ),
      Pac_sins AS (
        SELECT
          fp.id AS "Final Product ID",
          acsins.average_max_nm_od AS "pAC-SINS Average nm",
          acsins.wavelength_shift_relative_to_pbs_control
            AS "pAC-SINS nm ShiftPBSc",
          acsins.positive_control AS "pAC-SINS %PosControl"
          --,
          -- entryacsins.display_id as "pAC-SINS Exp ID"
        FROM
          lot
          INNER JOIN final_product AS fp
            ON fp.id = lot.parent_product_id
          FULL OUTER JOIN ps_ac_sins$raw AS acsins
            ON fp.id = acsins.final_product_id
          LEFT JOIN entry$raw AS entryacsins
            ON entryacsins.id = acsins.entry_id$
      ),
      ce AS (
        WITH
          themaxes (notreal, maxscore) AS (
            SELECT
              cetab.sample AS notreal,
              MAX(cetab.purity) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp ON fp.id = lot.id
              INNER JOIN a_ce_tabular_data$raw AS cetab
                ON cetab.sample = fp.id
            WHERE
              cetab.purity IS NOT NULL
              AND cetab.gel_types = 'CE NR'
            GROUP BY
              cetab.sample
          )
        SELECT
          t2.sample AS "Final Product ID",
          CASE
            WHEN fp.format = 'format 1'
              THEN ROUND((t2.size_kda / 1.17)::DECIMAL, 2)
            WHEN fp.format = 'format 2'
              THEN ROUND((t2.size_kda / 1.27)::DECIMAL, 2)
            WHEN fp.format = 'format 3'
              THEN ROUND((t2.size_kda / 1.27)::DECIMAL, 2)
            ELSE ROUND(t2.size_kda::DECIMAL, 2)
          END AS "Corrected CE kDa",
          t2.purity AS "CE Purity",
          t2.size_kda AS "CE kDa"
        FROM
          a_ce_tabular_data$raw AS t2
          INNER JOIN themaxes m ON m.notreal = t2.sample
            AND m.maxscore = t2.purity
          INNER JOIN final_product AS fp ON fp.id = t2.sample
          LEFT JOIN entry$raw AS entryce
            ON entryce.id = t2.entry_id$
      ),
      Pce AS (
        WITH
          themaxes (notreal, maxscore) AS (
            SELECT
              cetab.sample AS notreal,
              MAX(cetab.purity) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp
                ON fp.id = lot.parent_product_id
              INNER JOIN a_ce_tabular_data$raw AS cetab
                ON cetab.sample = fp.id
            WHERE
              cetab.purity IS NOT NULL
              AND cetab.gel_types = 'CE NR'
            GROUP BY
              cetab.sample
          )
        SELECT
          t2.sample AS "Final Product ID",
          t2.purity AS "pCE Purity",
          t2.size_kda AS "pCE kDa"
        FROM
          a_ce_tabular_data$raw AS t2
          INNER JOIN themaxes m ON m.notreal = t2.sample
            AND m.maxscore = t2.purity
          INNER JOIN final_product AS fp ON fp.id = t2.sample
          LEFT JOIN entry$raw AS entryce
            ON entryce.id = t2.entry_id$
      ),
      SEC AS (
        SELECT
          sample AS "Final Product ID",
          percent_area AS "SEC %Purity",
          retention_time AS "SEC RT (min)",
          height AS "SEC Height",
          area AS "SEC Area"
        FROM
          hplc_tabular_data$raw h
        WHERE
          assay_type = 'SEC'
          AND sample IN (
            SELECT
              id
            FROM
              lot
          )
          AND percent_area = (
            SELECT
              MAX(percent_area)
            FROM
              hplc_tabular_data$raw h2
            WHERE
              h2.sample = h.sample
              AND h2.assay_type = 'SEC'
          )
      ),
      PSEC AS (
        WITH
          themaxesSEC (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp
                ON fp.id = lot.parent_product_id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'SEC'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "pSEC %Purity",
          t1.retention_time AS "pSEC RT (min)",
          t1.height AS "pSEC Height",
          t1.area AS "pSEC Area",
          entrySEC.display_id AS "SEC Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entrySEC
            ON entrySEC.id = t1.entry_id$
          INNER JOIN themaxesSEC ON themaxesSEC."nr" = t1.sample
            AND themaxesSEC.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'SEC'
      ),
      SCX AS (
        WITH
          themaxesSCX (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp ON fp.id = lot.id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'SCX'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "SCX %Purity",
          t1.retention_time AS "SCX RT (min)",
          t1.height AS "SCX Height",
          t1.area AS "SCX Area"
          --,
          -- entrySCX.display_id AS "SCX Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entrySCX
            ON entrySCX.id = t1.entry_id$
          INNER JOIN themaxesSCX ON themaxesSCX."nr" = t1.sample
            AND themaxesSCX.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'SCX'
      ),
      PSCX AS (
        WITH
          themaxesSCX (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp
                ON fp.id = lot.parent_product_id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'SCX'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "pSCX %Purity",
          t1.retention_time AS "pSCX RT (min)",
          t1.height AS "pSCX Height",
          t1.area AS "pSCX Area"
          --,
          -- entrySCX.display_id AS "pSCX Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entrySCX
            ON entrySCX.id = t1.entry_id$
          INNER JOIN themaxesSCX ON themaxesSCX."nr" = t1.sample
            AND themaxesSCX.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'SCX'
      ),
      HIC AS (
        WITH
          themaxesHIC (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp ON fp.id = lot.id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'HIC'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "HIC %Purity",
          t1.retention_time AS "HIC RT (min)",
          t1.height AS "HIC Height",
          t1.area AS "HIC Area"
          --,
          --entryHIC.display_id AS "HIC Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entryHIC
            ON entryHIC.id = t1.entry_id$
          INNER JOIN themaxesHIC ON themaxesHIC."nr" = t1.sample
            AND themaxesHIC.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'HIC'
      ),
      PHIC AS (
        WITH
          themaxesHIC (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp
                ON fp.id = lot.parent_product_id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'HIC'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "pHIC %Purity",
          t1.retention_time AS "pHIC RT (min)",
          t1.height AS "pHIC Height",
          t1.area AS "pHIC Area"
          --,
          -- entryHIC.display_id AS "pHIC Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entryHIC
            ON entryHIC.id = t1.entry_id$
          INNER JOIN themaxesHIC ON themaxesHIC."nr" = t1.sample
            AND themaxesHIC.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'HIC'
      ),
      SMAC AS (
        WITH
          themaxesSMAC (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp ON fp.id = lot.id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'SMAC'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "SMAC %Purity",
          t1.retention_time AS "SMAC RT (min)",
          t1.height AS "SMAC Height",
          t1.area AS "SMAC Area"
          --,
          --entrySMAC.display_id AS "SMAC Experiment ID"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entrySMAC
            ON entrySMAC.id = t1.entry_id$
          INNER JOIN themaxesSMAC ON themaxesSMAC."nr" = t1.sample
            AND themaxesSMAC.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'SMAC'
      ),
      PSMAC AS (
        WITH
          themaxesSMAC (nr, maxscore) AS (
            SELECT
              hplc1.sample AS "nr",
              MAX(hplc1.percent_area) AS maxscore
            FROM
              lot
              INNER JOIN final_product AS fp
                ON fp.id = lot.parent_product_id
              INNER JOIN hplc_tabular_data$raw AS hplc1
                ON fp.id = hplc1.sample
            WHERE
              hplc1.assay_type = 'SMAC'
            GROUP BY
              hplc1.sample
          )
        SELECT
          t1.sample AS "Final Product ID",
          t1.percent_area AS "pSMAC %Purity",
          t1.retention_time AS "pSMAC RT (min)",
          t1.height AS "pSMAC Height",
          t1.area AS "pSMAC Area"
        FROM
          hplc_tabular_data$raw AS t1
          LEFT JOIN entry$raw AS entrySMAC
            ON entrySMAC.id = t1.entry_id$
          INNER JOIN themaxesSMAC ON themaxesSMAC."nr" = t1.sample
            AND themaxesSMAC.maxscore = t1.percent_area
        WHERE
          t1.assay_type = 'SMAC'
      )
    -- Final wide join — bringing everything together for dashboard use
    SELECT DISTINCT
      ON (psresults."Final Product ID") psresults.*,
      ce.*,
      sec.*,
      scx.*,
      hic.*,
      smac.*,
      polyreactivity.*,
      ac_sins.*,
      Psec.*
    FROM
      psresults
      LEFT JOIN SEC ON sec."Final Product ID" = psresults."Final Product ID"
      LEFT JOIN SCX ON scx."Final Product ID" = psresults."Final Product ID"
      LEFT JOIN HIC ON hic."Final Product ID" = psresults."Final Product ID"
      LEFT JOIN SMAC ON smac."Final Product ID" = psresults."Final Product ID"
      LEFT JOIN ce ON psresults."Final Product ID" = ce."Final Product ID"
      LEFT JOIN polyreactivity
        ON psresults."Final Product ID" = polyreactivity."Final Product ID"
      LEFT JOIN ac_sins
        ON psresults."Final Product ID" = ac_sins."Final Product ID"
      LEFT JOIN PSEC ON Psec."Final Product ID" = psresults."Parent Product ID"
      LEFT JOIN PSCX ON Pscx."Final Product ID" = psresults."Parent Product ID"
      LEFT JOIN PHIC ON Phic."Final Product ID" = psresults."Parent Product ID"
      LEFT JOIN PSMAC ON Psmac."Final Product ID" = psresults."Parent Product ID"
      LEFT JOIN Pce ON psresults."Parent Product ID" = Pce."Final Product ID"
      LEFT JOIN Ppolyreactivity
        ON psresults."Parent Product ID" = Ppolyreactivity."Final Product ID"
      LEFT JOIN Pac_sins
        ON psresults."Parent Product ID" = Pac_sins."Final Product ID"
  )
SELECT DISTINCT
  PS.*
FROM
  PS
ORDER BY
  PS."Source Batch Name" ASC
