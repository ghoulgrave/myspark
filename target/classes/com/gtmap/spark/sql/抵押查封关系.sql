select * from(
               select a.*
               from (
                      select *
                      from (
                        select
                          a.proid,
                          a.bh,
                          a.qllx,
                          a.cjsj,
                          a.bjsj,
                          a.bdcdyid,
                          nd.qlid,
                          nd.BDCDYID                                 as bdcid,
                          nd.ZWLXKSQX,
                          nd.ZWLXJSQX,
                          nd.BDBZZQSE,
                          nd.BJSJ                                    as dybjsj,
                          round(abs(TO_NUMBER(a.BJSJ - nd.BJSJ)), 2) as sjx

                        from bdc_xm a
                          left join (
                                      select
                                        a.qlid,
                                        a.bdcdyid,
                                        a.zwlxksqx,
                                        a.zwlxjsqx,
                                        a.bdbzzqse,
                                        b.bjsj
                                      from bdc_dyaq a left join bdc_xm b on a.PROID = b.PROID -- on a.ywh = b.bh
                                      where b.PROID is not null) nd on a.BDCDYID = nd.BDCDYID
                        where a.qllx in ('4', '6', '8', '4,6,8')
                      )
                      where QLID is not null and (cjsj is not null or bjsj is not null)
                      --and BDCDYID = '320506001113GB00104F00051703'
                    ) a left join (select
                                     proid,
                                     min(sjx) as minsjx
                                   from (
                                     select
                                       a.proid,
                                       a.bh,
                                       a.qllx,
                                       a.cjsj,
                                       a.bjsj,
                                       a.bdcdyid,
                                       nd.qlid,
                                       nd.BDCDYID                                 as bdcid,
                                       nd.ZWLXKSQX,
                                       nd.ZWLXJSQX,
                                       nd.BDBZZQSE,
                                       nd.BJSJ                                    as dybjsj,
                                       round(abs(TO_NUMBER(a.BJSJ - nd.BJSJ)), 2) as sjx
                                     from bdc_xm a
                                       left join (
                                                   select
                                                     a.qlid,
                                                     a.bdcdyid,
                                                     a.zwlxksqx,
                                                     a.zwlxjsqx,
                                                     a.bdbzzqse,
                                                     b.bjsj
                                                   from bdc_dyaq a left join bdc_xm b on a.PROID = b.PROID -- on a.ywh = b.bh
                                                   where b.PROID is not null) nd on a.BDCDYID = nd.BDCDYID
                                     where a.qllx in ('4', '6', '8', '4,6,8')
                                   )
                                   where QLID is not null and (cjsj is not null or bjsj is not null)
                                   --and BDCDYID='320506001113GB00104F00051703'
                                   group by proid) b on a.PROID = b.PROID
               where a.sjx = b.minsjx and BDBZZQSE is not null
             ) a
  left join (
              select proid,case when qlid is null then 0 else 1 end as cfk from (
                                                                                  select a.proid,b.QLID from bdc_xm a
                                                                                    left join bdc_cf b on a.BDCDYID = b.BDCDYID
                                                                                  where a.BDCDYID is not null
                                                                                ) c
            ) n on a.PROID = n.PROID