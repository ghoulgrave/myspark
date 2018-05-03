select * from
(select * from (
  select distinct
    a.proid as p0,    a.bh    as b0,    a.qllx  as q0
    ,d.proid as pd,    d.bh    as bd,    d.qllx  as qd
  ,f.proid as dyq,h.qlrzjh as dyqlrzjh,h.qlrmc as dyqlrmc
  from bdc_xm a
    left join bdc_yg b on a.PROID = b.PROID
    left join BDC_XM_REL c on a.PROID = c.YPROID
    left join BDC_XM d on c.PROID = d.PROID and d.QLLX='18'
    left join bdc_dyaq f on d.PROID = f.PROID
    left join bdc_qlr h on h.proid = d.proid and h.qlrlx='ywr'
  where a.qllx in ('4', '6', '8', '4,6,8') or (a.qllx = '19' and b.YGDJZL in ('1', '2'))
) where pd Is not null and dyq Is not null
) aa left join (
select * from (
select distinct
a.proid as p0, a.bh as b0, a.qllx as q0
, e.proid as pc, e.bh as bc, e.qllx as qc
, f.proid as cfq, h.qlrmc, h.qlrzjh
from bdc_xm a
left join bdc_yg b on a.PROID = b.PROID
left join BDC_XM_REL c on a.PROID = c.YPROID
left join BDC_XM e on c.PROID = e.PROID and e.QLLX='21'
left join bdc_cf f on e.PROID = f.PROID
left join bdc_qlr h on h.PROID = e.PROID
where a.qllx in ('4', '6', '8', '4,6,8') or ( a.qllx = '19' and b.YGDJZL in ('1', '2'))
) where pc is not null and cfq is not null
) bb on aa.p0 = bb.p0 where bb.p0 is not null
--where aa.p0='bdc-730437'
;