CREATE OR REPLACE VIEW healthcare_curated.staffing_levels_vs_readmission_rates AS
WITH staffing_rollup AS (
  SELECT
    provnum,
    state,
    AVG(CAST(mdscensus AS double)) AS avg_daily_census,
    SUM(CAST(mdscensus AS double)) AS patient_days,
    SUM(
      COALESCE(hrs_rndon, 0) + COALESCE(hrs_rnadmin, 0) + COALESCE(hrs_rn, 0) +
      COALESCE(hrs_lpnadmin, 0) + COALESCE(hrs_lpn, 0) +
      COALESCE(hrs_cna, 0) + COALESCE(hrs_natrn, 0) + COALESCE(hrs_medaide, 0)
    ) AS total_nursing_hours
  FROM AwsDataCatalog.healthcare_refined.pbj_daily_nurse_staffing
  GROUP BY 1, 2
),
staffing_scored AS (
  SELECT
    provnum,
    state,
    avg_daily_census,
    total_nursing_hours,
    (total_nursing_hours / NULLIF(patient_days, 0)) AS nhppd
  FROM staffing_rollup
  WHERE patient_days > 0
),
readmit_ranked AS (
  SELECT
    cms_certification_number_ccn AS provnum,
    state,
    measure_code,
    measure_description,
    resident_type,
    measure_period,
    CAST(adjusted_score AS double) AS readmission_value,
    processing_date,
    ROW_NUMBER() OVER (
      PARTITION BY cms_certification_number_ccn, measure_code, resident_type, measure_period
      ORDER BY processing_date DESC
    ) AS rn
  FROM AwsDataCatalog.healthcare_refined.nh_quality_measure_claims
  WHERE measure_code IN (521, 551)
    AND adjusted_score IS NOT NULL
),
readmit_latest AS (
  SELECT
    provnum,
    state,
    measure_code,
    measure_description,
    resident_type,
    measure_period,
    readmission_value
  FROM readmit_ranked
  WHERE rn = 1
)
SELECT
  s.provnum,
  s.state,
  s.avg_daily_census,
  s.total_nursing_hours,
  s.nhppd,
  r.measure_code,
  r.measure_description,
  r.resident_type,
  r.measure_period,
  r.readmission_value
FROM staffing_scored s
LEFT JOIN readmit_latest r
  ON s.provnum = r.provnum;

