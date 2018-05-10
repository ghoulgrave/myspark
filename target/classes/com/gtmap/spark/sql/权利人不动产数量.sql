select qlrmc,count(*) as zs from (
  select
    a.bdcdyid,
    c.QLRZJH || '^' || regexp_replace(replace(qlrmc, chr(13) || chr(10), ' '), '<[0-9]+.[0-9]+.[0-9]+>', '') as qlrmc
  from BDCDJ_WZ.BDC_FDCQ a
    left join bdc_qlr c on c.PROID = a.PROID
  where c.qlrlx = 'qlr' and a.QSZT = 1
        and QLRMC is not null and qlrzjh is not null and qlrzjh != '0' and QLRMC != '0'

) group by qlrmc