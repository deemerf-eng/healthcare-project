CREATE OR REPLACE VIEW healthcare_curated.staffing_vs_patient_load AS
WITH facility_rollup AS (
  SELECT
    PROVNUM,
    STATE,
    AVG(CAST(MDScensus AS double)) AS avg_daily_census,
    SUM(CAST(MDScensus AS double)) AS patient_days,
    SUM(
      COALESCE(Hrs_RNDON, 0) + COALESCE(Hrs_RNadmin, 0) + COALESCE(Hrs_RN, 0) +
      COALESCE(Hrs_LPNadmin, 0) + COALESCE(Hrs_LPN, 0) +
      COALESCE(Hrs_CNA, 0) + COALESCE(Hrs_NAtrn, 0) + COALESCE(Hrs_MedAide, 0)
    ) AS total_nursing_hours
  FROM AwsDataCatalog.healthcare_refined.pbj_daily_nurse_staffing
  GROUP BY 1, 2
)
SELECT
  PROVNUM,
  STATE,
  avg_daily_census,
  total_nursing_hours,
  (total_nursing_hours / NULLIF(patient_days, 0)) AS nhppd
FROM facility_rollup;
