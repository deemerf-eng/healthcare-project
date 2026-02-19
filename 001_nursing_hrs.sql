CREATE OR REPLACE VIEW healthcare_curated.total_nurse_hours AS
SELECT
  PROVNUM,
  STATE,
  date_trunc('month', date_parse(CAST(WorkDate AS varchar), '%Y%m%d')) AS month_start,
  SUM(
    Hrs_RNDON + Hrs_RNadmin + Hrs_RN +
    Hrs_LPNadmin + Hrs_LPN +
    Hrs_CNA + Hrs_NAtrn + Hrs_MedAide
  ) AS total_nursing_hours
FROM AwsDataCatalog.healthcare_refined.pbj_daily_nurse_staffing
GROUP BY 1, 2, 3;
