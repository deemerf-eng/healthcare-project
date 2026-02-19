CREATE OR REPLACE VIEW healthcare_curated.lowest_staffing_vs_patient_load AS
WITH facility_rollup AS (
  SELECT
    PROVNUM,
    STATE,
    AVG(CAST(MDScensus AS double)) AS avg_daily_census,
    SUM(
      COALESCE(Hrs_RNDON, 0) + COALESCE(Hrs_RNadmin, 0) + COALESCE(Hrs_RN, 0) +
      COALESCE(Hrs_LPNadmin, 0) + COALESCE(Hrs_LPN, 0) +
      COALESCE(Hrs_CNA, 0) + COALESCE(Hrs_NAtrn, 0) + COALESCE(Hrs_MedAide, 0)
    ) AS total_nursing_hours
  FROM AwsDataCatalog.healthcare_refined.pbj_daily_nurse_staffing
  GROUP BY 1, 2
),
scored AS (
  SELECT
    PROVNUM,
    STATE,
    avg_daily_census,
    total_nursing_hours,
    (total_nursing_hours / NULLIF(avg_daily_census, 0)) AS nhppd
  FROM facility_rollup
  WHERE avg_daily_census > 0
)
SELECT
  PROVNUM,
  STATE,
  avg_daily_census,
  total_nursing_hours,
  nhppd
FROM scored
ORDER BY nhppd ASC
LIMIT 10;
