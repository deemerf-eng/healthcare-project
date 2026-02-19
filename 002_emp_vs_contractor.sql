CREATE OR REPLACE VIEW healthcare_curated.employee_vs_contractor_ratio AS
SELECT
  PROVNUM,
  STATE,
  date_trunc('month', date_parse(CAST(WorkDate AS varchar), '%Y%m%d')) AS month_start,

  SUM(
    COALESCE(Hrs_RNDON_emp, 0) + COALESCE(Hrs_RNadmin_emp, 0) + COALESCE(Hrs_RN_emp, 0) +
    COALESCE(Hrs_LPNadmin_emp, 0) + COALESCE(Hrs_LPN_emp, 0) +
    COALESCE(Hrs_CNA_emp, 0) + COALESCE(Hrs_NAtrn_emp, 0) + COALESCE(Hrs_MedAide_emp, 0)
  ) AS employee_hours,

  SUM(
    COALESCE(Hrs_RNDON_ctr, 0) + COALESCE(Hrs_RNadmin_ctr, 0) + COALESCE(Hrs_RN_ctr, 0) +
    COALESCE(Hrs_LPNadmin_ctr, 0) + COALESCE(Hrs_LPN_ctr, 0) +
    COALESCE(Hrs_CNA_ctr, 0) + COALESCE(Hrs_NAtrn_ctr, 0) + COALESCE(Hrs_MedAide_ctr, 0)
  ) AS contractor_hours,

  (
    SUM(
      COALESCE(Hrs_RNDON_emp, 0) + COALESCE(Hrs_RNadmin_emp, 0) + COALESCE(Hrs_RN_emp, 0) +
      COALESCE(Hrs_LPNadmin_emp, 0) + COALESCE(Hrs_LPN_emp, 0) +
      COALESCE(Hrs_CNA_emp, 0) + COALESCE(Hrs_NAtrn_emp, 0) + COALESCE(Hrs_MedAide_emp, 0)
    )
    /
    NULLIF(
      SUM(
        COALESCE(Hrs_RNDON_emp, 0) + COALESCE(Hrs_RNadmin_emp, 0) + COALESCE(Hrs_RN_emp, 0) +
        COALESCE(Hrs_LPNadmin_emp, 0) + COALESCE(Hrs_LPN_emp, 0) +
        COALESCE(Hrs_CNA_emp, 0) + COALESCE(Hrs_NAtrn_emp, 0) + COALESCE(Hrs_MedAide_emp, 0) +
        COALESCE(Hrs_RNDON_ctr, 0) + COALESCE(Hrs_RNadmin_ctr, 0) + COALESCE(Hrs_RN_ctr, 0) +
        COALESCE(Hrs_LPNadmin_ctr, 0) + COALESCE(Hrs_LPN_ctr, 0) +
        COALESCE(Hrs_CNA_ctr, 0) + COALESCE(Hrs_NAtrn_ctr, 0) + COALESCE(Hrs_MedAide_ctr, 0)
      ),
      0
    )
  ) AS pct_employee_hours

FROM AwsDataCatalog.healthcare_refined.pbj_daily_nurse_staffing
GROUP BY 1, 2, 3;
