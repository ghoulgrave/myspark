-- 抵押可用数据75023
-- 查封数据最多可用1116
-- 抵押被查封795
-- 抵押被查封的不动产个数625




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

select distinct m.base,m.qlr,m.qlrzjh,m.qlbl,m.gyfs,m.bdcdyid,m.jyjg,n.wiid,n.bdbzzqse,n.zgzqqdse,n.zwlxksqx,n.zwlxjsqx from (
select base,proid,qlr,qlrzjh,qlbl,bdcdyid,jyjg/10000 as jyjg,wiid,gyfs from (
select a.qlr||a.qlrzjh||b.bdcdyid as base ,a.proid,a.qlr,a.qlrzjh,a.qlbl,b.jyjg,b.bdcdyid,c.wiid,a.gyfs from bdc_qlr a
left join bdc_xm c on a.proid = c.proid
left join bdc_fdcq b on a.proid = b.proid
where a.qlrlx='qlr'  and b.qlid is not null
) where jyjg >0 and qlr is not null and qlrzjh is not null
) m
left join
(
select * from (
select a.qlr||a.qlrzjh||b.bdcdyid as base ,a.proid,a.qlr,a.qlrzjh,a.qlbl,b.bdcdyid,b.bdbzzqse,b.zgzqqdse,b.zwlxksqx,b.zwlxjsqx,c.wiid from bdc_qlr a
left join bdc_xm c on a.proid = c.proid
left join bdc_dyaq b on a.proid = b.proid
where a.qlrlx='ywr' and b.qlid is not null
)where qlr is not null and qlrzjh is not null
) n on m.base = n.base
order by n.wiid;