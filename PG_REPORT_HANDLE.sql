create or replace package body PG_REPORT_HANDLE is


procedure P_ININTIATIVE_ORDER_REPORT(mktCampaignId varchar2) is

  TYPE T_CUR IS REF CURSOR;
  TYPE varchar_array1 IS VARRAY(20) OF varchar2(30);
  TYPE varchar_array2 IS VARRAY(20) OF varchar2(120);
  TYPE varchar_array3 IS VARRAY(20) OF varchar2(3000);
  campaign_cur T_CUR;
  order_cur T_CUR; 
  order_rule_cur T_CUR;
  rule_cur T_CUR;
  v_rule_count number:=0;
  v_rule_sql varchar2(1000);
  v_suc_rule_id number;
  v_suc_rule_name varchar2(60);
  v_suc_sql_text varchar2(3000);
  v_suc_rule_col  varchar2(60);
  v_conditiontext varchar2(600);
  v_campaign_id varchar2(30);
  v_rel_id number;
  v_rule_campaign_id number;
  v_latn_name VARCHAR2(30);
  v_latn_id VARCHAR2(30);
  v_campaign_name VARCHAR2(300);
  v_campaign_remark VARCHAR2(300);
  v_order_id number(20);
  v_status_cd number;
  v_channel_id number;
  v_org_jt_code number;
  v_org_id number;
  v_hasorder  number;
  v_fir_gridId number;
  v_sec_gridId number;
  v_thi_gridId number;
  v_con_sql_text VARCHAR2(3000);
  v_con_sql_text_1 VARCHAR2(3000);
  v_prodInstId number;
  v_custId number;
  v_is_success number;
  v_suc_result varchar2(200);
  v_sucOrderId number;
  v_old_is_success number;
  v_user_id varchar(64);
  v_is_done  VARCHAR2(4);
  v_num number;
  v_hassucnum number;
  v_flag number:=0;
  v_sucRuleSqls varchar_array3:=varchar_array3();
  v_sucRuleNames varchar_array2:=varchar_array2();
  v_sucRuleIds varchar_array1:=varchar_array1();
  v_sucRuleRelIds varchar_array1:=varchar_array1();
  v_sucRuleCols varchar_array1:=varchar_array1();
  sucStartDate date;
  errorCode number; --异常编码  
  errorMsg varchar2(1000); --异常信息
BEGIN

  /*成功口径定义取配置*/
       /*获取成功口径配置sql
       按目录和子活动定义的收单规则取交集查询对应的成功口径*/
          v_rule_sql := 'select a.suc_rule_id, to_char(a.sql_text_order), a.rule_name, c.rel_id,a.report_column
  from mkt_success_rule a, MKT_CAMPAIGN_SUC_RULE_REL b,mkt_campaign_suc_rule_rel c,mkt_success_rule d 
 where a.suc_rule_id = b.suc_rule_id
   and a.state = 10
   and b.rel_type = 1
   and b.campaign_id = :mainCampaignId
   and a.suc_rule_id =  c.suc_rule_id 
   and c.suc_rule_id = d.suc_rule_id 
   and c.rel_type = 2
   and c.campaign_id =  :campaignId
 order by b.seq  ';
  /*查询子活动数据
    按承接的子活动循环
  */
  OPEN campaign_cur FOR 'with latn_table as 
                      (SELECT *
                        FROM (SELECT DBMS_LOB.SUBSTR(REGEXP_SUBSTR(''290,919,910,911,912,913,914,915,916,917'',
                                                                   ''[^,]+'',
                                                                   1,
                                                                   X.N)) AS PLAYER_GUID 　　FROM DUAL A,
                                     (SELECT ROWNUM N FROM DUAL CONNECT BY ROWNUM < 11) X)
                       WHERE PLAYER_GUID IS NOT NULL)

                      select mkt_campaign_id,region_name,latn_id,campaign_name,remark
                        from (select b.mkt_campaign_id,
                                     c.region_name,
                                     to_number(y.PLAYER_GUID) latn_id,
                                     a.campaign_name,
                                     c.region_name remark,
                                     nvl(c.region_id_old,888) region_id_old
                                from pms_campaign a, mkt_cam_conf_attr b, common_region c, latn_table y
                               where a.campaign_id = b.mkt_campaign_id
                                 and a.region_id = c.region_id
                                 and b.attr_nbr = ''MKT_CPC_CAMPAIGN_CATALOG''
                                 and a.end_date > trunc(sysdate)-7
                                 and a.state = ''10A''
                                 and attr_value_id = :mktCampaignId
                                 and exists (select 1 from pms_campaign_chl_rel h
                                 where a.campaign_id = h.campaign_id
                                 and h.channel_id in (select ioc.channel_id
                                                        from initiative_order_channel ioc
                                                       where ioc.app_name = ''bsscall'')) ) x
                       where 1=1
                         and region_id_old = 999
                      union all 
                      select *
                        from (select b.mkt_campaign_id,
                                     c.region_name,
                                     nvl(c.region_id_old, 888) latn_id,
                                     a.campaign_name,
                                     c.region_name remark
                                from pms_campaign a, mkt_cam_conf_attr b, common_region c
                               where a.campaign_id = b.mkt_campaign_id
                                 and a.region_id = c.region_id
                                 and b.attr_nbr = ''MKT_CPC_CAMPAIGN_CATALOG''
                                 and a.end_date > trunc(sysdate)-7
                                 and a.state = ''10A''
                                 and attr_value_id = :mktCampaignId
                                  and exists (select 1 from pms_campaign_chl_rel h
                                 where a.campaign_id = h.campaign_id
                                 and h.channel_id in (select ioc.channel_id
                                                        from initiative_order_channel ioc
                                                       where ioc.app_name = ''bsscall'')) ) x
                       where 1=1
                         and latn_id <> 999 '
       USING mktCampaignId, mktCampaignId;
       LOOP FETCH campaign_cur  INTO v_campaign_id,v_latn_name,v_latn_id,v_campaign_name,v_campaign_remark;
          exit when campaign_cur%notfound;

 sucStartDate := sysdate;
   /*取出成功口径 放入数组，方便后续使用，不需每次再重新执行获取sql
   此处改为在查询了子活动以后执行.. 成功口径取，目录定义的 和 目录下活动定义的收单口径的交集
   */
         v_sucRuleIds.delete();
         v_sucRuleNames.delete();
         v_sucRuleSqls.delete();
         v_sucRuleRelIds.delete();
         v_sucRuleCols.delete();
         v_num := 0;
         v_rule_campaign_id := mktCampaignId;
            open rule_cur for v_rule_sql using mktCampaignId,v_campaign_id;

            loop
              fetch rule_cur into  v_suc_rule_id,v_suc_sql_text,v_suc_rule_name,v_rel_id,v_suc_rule_col;
              exit when  rule_cur%notfound;
              v_num := v_num+1;
              v_sucRuleIds.extend(1);
              v_sucRuleNames.extend(1);
              v_sucRuleSqls.extend(1);
              v_sucRuleRelIds.extend(1);
              v_sucRuleCols.extend(1);
              v_sucRuleIds(v_num) := v_suc_rule_id;
              v_sucRuleNames(v_num) := v_suc_rule_name;
              v_sucRuleSqls(v_num) := v_suc_sql_text;
              v_sucRuleRelIds(v_num) := v_rel_id;
              v_sucRuleCols(v_num) := v_suc_rule_col;
              end loop;
               close rule_cur;       
          
          
       /*增量入统计明细表*/
       execute immediate 'insert into rpt_receive_order_detail
              (report_date,
               update_date,
               latn_id,
               region_id,
               org_id,
               org_jt_code,
               fir_grid_id,
               sec_grid_id,
               thi_grid_id,
               z_mkt_campaign_id,
               a_mkt_campaign_id,
               list_id,
               status_cd,
               old_org_id,
               old_org_jt_code,
               old_status_cd,
               is_success,
               user_id,
               user_name,
               cust_id,
               cust_name,
               acc_nbr,
               cop_channel_id)
       select to_char(sysdate, ''yyyy-mm-dd hh24:mi:ss'') report_date,
       sysdate,
       t.latn_id,
       (select e.common_region_id
          from crm20_pub.organization@link_pub  d,
               crm20_pub.common_region@link_pub e
         where d.common_region_id = e.common_region_id
           and d.org_id = c.org_id) region_id,
       c.org_id,
       c.org_jt_code,
       c.fir_grid_id,
       c.sec_grid_id,
       c.thi_grid_id,
       :1 z_mkt_campaign_id,
       t.campaign_id a_mkt_campaign_id,
       t.initiative_order_id list_id,
       ex.status_cd,
       c.org_id,
       c.org_jt_code,
       ex.status_cd,
       0,
       ex.curr_user_id,
       (select s.staff_name from system_user u, staff s 
         where u.system_user_id = ex.curr_user_id 
           and s.staff_id = u.staff_id and rownum = 1) user_name,
       t.cust_id,
       t.cust_name,
       t.acc_nbr,
       t.channel_id
  from initiative_order_'||v_latn_id||'          t,
       initiative_order_executor_'||v_latn_id||' ex,
       t_grid_org_code_rel           c
 where ex.initiative_order_id = t.initiative_order_id
   and t.state = ''10A''
   and t.channel_id <> 6
   and ex.dept_id = c.org_jt_code(+)
   and not exists (select 1
          from rpt_receive_order_detail a
         where t.initiative_order_id = a.list_id)
   and t.campaign_id = :2
   and t.latn_id = :3 ' using mktCampaignId, v_campaign_id ,v_latn_id;

   /*有状态变更的或营业厅id变更的数据，开刷*/
       OPEN order_cur FOR 'select a.initiative_order_id,
             b.status_cd,
             a.channel_id,
             b.dept_id org_jt_code,
             0,
             a.prod_inst_id,
             a.cust_id,
             0,
             b.curr_user_id
        from INITIATIVE_ORDER_'||v_latn_id||' a, INITIATIVE_ORDER_EXECUTOR_'||v_latn_id||' b,rpt_receive_order_detail c
       where a.initiative_order_id = b.initiative_order_id
         and a.state = ''10A''
         and a.channel_id in (select ioc.channel_id
                                from initiative_order_channel ioc
                               where ioc.app_name = ''bsscall'')
         and b.last_opt_date > trunc(sysdate) - 1
         and b.last_opt_date < trunc(sysdate) 
         and a.initiative_order_id = c.list_id
         and (b.status_cd <> c.status_cd
         or nvl(b.dept_id,0) <> nvl(c.org_jt_code,0)
         or nvl(b.curr_user_id,0) <> nvl(c.user_id,0))
         and a.campaign_id = :1  '
     USING v_campaign_id;
     LOOP
       FETCH order_cur  INTO  v_order_id,v_status_cd,v_channel_id,v_org_jt_code,v_hasorder,v_prodInstId,v_custid,v_old_is_success,v_user_id;
       exit when order_cur%notfound;
       
       begin
       /*增量数据*/
       v_fir_gridId:= -1;
       v_sec_gridId:=-1;
       v_thi_gridId := -1;
       if v_org_jt_code is not null then
       execute immediate
       '   select fir_grid_id, sec_grid_id, thi_grid_id, org_id
     from (select c.fir_grid_id, c.sec_grid_id, c.thi_grid_id, c.org_id
             from t_grid_org_code_rel c
            where c.org_jt_code = :1
           union
           select -2, -2, -2, -2
             from dual
            order by thi_grid_id desc)
    where rownum < 2  ' into v_fir_gridId,v_sec_gridId,v_thi_gridId,v_org_id using v_org_jt_code;
       end if;

       execute immediate ' update rpt_receive_order_detail a
        set old_status_cd   = status_cd,
            status_cd       = :1,
            old_org_id      = org_id,
            org_id          = :2,
            old_org_jt_code = org_jt_code,
            org_jt_code     = :3,
            update_date     = sysdate,
            fir_grid_id     = :4,
            sec_grid_id     = :5,
            thi_grid_id     = :6,
            user_id         = :7,
            user_name       = (select s.staff_name from system_user u, staff s 
                                where u.system_user_id = :8 
                                  and s.staff_id = u.staff_id 
                                  and rownum = 1)
      where a.list_id = :9  ' using v_status_cd, v_org_id, v_org_jt_code, v_fir_gridId,v_sec_gridId,v_thi_gridId, v_user_id, v_user_id, v_order_id;


     EXCEPTION
          when others then
           errorCode := SQLCODE;      
           errorMsg := '活动['||v_campaign_id||']的工单['||v_order_id||']'||SUBSTR(SQLERRM, 1, 200);
          EXECUTE IMMEDIATE 'insert into exception_outcall(errormsg,pro_name,errorcode) values('''||errorMsg||''',
          ''p_order_receive_detail_call'','''||errorCode||''')';
          commit;
          CONTINUE;
        END;
     
     END LOOP;
     CLOSE order_cur;
     
     commit;

  /*客户下有订单数据的数据，开刷*/
  if v_num > 0 then
     open order_rule_cur for 'select a.initiative_order_id,
                                     b.status_cd,
                                     a.channel_id,
                                     1,
                                     a.prod_inst_id,
                                     a.cust_id 
                                from INITIATIVE_ORDER_'||v_latn_id||' a, INITIATIVE_ORDER_EXECUTOR_'||v_latn_id||' b
                               where a.initiative_order_id = b.initiative_order_id
                                 and a.channel_id <> 6
                                 and exists (select 1
                                        from CUSTOMER_ORDER_SYN_'||v_latn_id||' c
                                       where c.cust_id = a.cust_id
                                         and c.status_cd = 300000
                                         and c.status_date >= trunc(sysdate) - 1
                                         and c.status_date < trunc(sysdate))
                                   and nvl(b.is_success,0) = 0 
                                   and a.campaign_id = :1 ' using v_campaign_id;

     LOOP
       FETCH order_rule_cur  INTO  v_order_id,v_status_cd,v_channel_id,v_hasorder,v_prodInstId,v_custid;
       exit when order_rule_cur%notfound;

       v_is_success := 0;
        /*从数组中获取成功口径*/
         for i in 1..v_num loop
           if  v_is_success = 0   then/*只跑一个成功了..其他口径就不跑了*/
           
             v_conditiontext := 'custId:'||v_custId||',prodInstId:'||v_prodInstId||',campaignId:'||v_rule_campaign_id||',sucRuleId:'||
               v_sucRuleIds(i)||',relId:'||v_sucRuleRelIds(i);
               /*执行口径*/
               v_suc_result := GET_DYNAMIC_SQL_RESULT(v_conditiontext,v_sucRuleSqls(i),v_latn_id);
               
               /*失败*/
               if v_suc_result = '-999999999' or v_suc_result = '-1' then 
                 
                  v_sucOrderId := -1;
                 
               /*成功*/
               else
                 v_is_success := 1;
                v_sucOrderId := to_number(v_suc_result);
               
                     execute immediate ' update rpt_receive_order_detail a set is_success = 1 , update_date = sysdate ,suc_rule_id=:1,
                              suc_rule_name =:2,suc_order_id= :3
                             where list_id = :4
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
                                          /*更新工单执行表的状态信息*/     
                     execute immediate 'update initiative_order_executor_'||v_latn_id||' t
                                           set t.is_success    = 1,
                                               t.suc_rule_id   = :1,
                                               t.suc_rule_name = :2,
                                               t.suc_order_id  = :3,
                                               t.suc_time      = sysdate
                                         where t.initiative_order_id = :4' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;

                    /*修改 口径字段值为 1*/
                  execute immediate 'update rpt_receive_order_detail a set '||v_sucRuleCols(i) ||' = 1 where list_id = :1 '
                          using v_order_id;

                  execute immediate 'insert into RPT_RECEIVE_ORDER_SUCINFO(list_id, suc_rule_id, suc_date, suc_rule_name,suc_order_id)
                                    values
                                    (:1, :2, sysdate, :3, :4)' using v_order_id,v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId;
                  
               end if;
               
               end if;
        end loop;
        END LOOP;
       CLOSE order_rule_cur;
       commit;
   end if;
   
    /*记录报表开始和结束时间*/
     execute immediate'insert into rpt_suc_rule_runtime_log values(to_char(sysdate, ''yyyymmdd''),11,:1,:2,sysdate,:3)'using v_campaign_id, sucStartDate, v_latn_id;

     /*以上明细跑完了，后面跑针对活动的统计，以及按天的统计*/
       END LOOP;
       CLOSE campaign_cur;


end P_ININTIATIVE_ORDER_REPORT;



procedure P_ININTIATIVE_ORDER_outcall_REPORT(mktCampaignId varchar2) is

  TYPE T_CUR IS REF CURSOR;
  TYPE varchar_array1 IS VARRAY(20) OF varchar2(30);
  TYPE varchar_array2 IS VARRAY(20) OF varchar2(120);
  TYPE varchar_array3 IS VARRAY(20) OF varchar2(3000);
  campaign_cur T_CUR;
  order_cur T_CUR;
  order_rule_cur T_CUR;
  rule_cur T_CUR;
  v_rule_count number:=0;
  v_rule_sql varchar2(1000);
  v_suc_rule_id number;
  v_suc_rule_name varchar2(60);
  v_suc_sql_text varchar2(3000);
  v_suc_rule_col  varchar2(60);
  v_conditiontext varchar2(600);
  v_campaign_id varchar2(30);
  v_rel_id number;
  v_rule_campaign_id number;
  v_campaign_name VARCHAR2(300);
  v_campaign_remark VARCHAR2(300);
  v_order_id number(20);
  v_status_cd number;
  v_channel_id number;
  v_org_jt_code VARCHAR2(100);
  v_org_id VARCHAR2(100);
  v_hasorder  number;
  v_fir_gridId VARCHAR2(100);
  v_sec_gridId VARCHAR2(100);
  v_thi_gridId VARCHAR2(100);
  v_con_sql_text VARCHAR2(3000);
  v_con_sql_text_1 VARCHAR2(3000);
  v_prodInstId number;
  v_custId number;
  v_is_success number;
  v_suc_result varchar2(200);
  v_sucOrderId number;
  v_old_is_success number;
  v_is_done  VARCHAR2(4);
  v_num number;
  v_hassucnum number;
  v_flag number:=0;
  v_sucRuleSqls varchar_array3:=varchar_array3();
  v_sucRuleNames varchar_array2:=varchar_array2();
  v_sucRuleIds varchar_array1:=varchar_array1();
  v_sucRuleRelIds varchar_array1:=varchar_array1();
  v_sucRuleCols varchar_array1:=varchar_array1();

  v_up_dept_name varchar2(200);
  v_up_dept_id varchar2(200);
  v_dept_name varchar2(200);
  v_dept_id varchar2(200);
  v_curr_user_name varchar2(200);
  v_curr_user_id varchar2(200);
  v_order_result varchar2(20);
  v_call_num number;
  v_org_type_id number;
  
  v_main_campaign_id number(15);
  v_main_campaign_name varchar2(500);
  v_sub_campaign_id number;
  v_sub_campaign_name varchar2(2000);
  v_latn_id varchar2(100);
  
  errorCode number; --异常编码  
  errorMsg varchar2(1000); --异常信息
  
  Type latn_type is table of number(4);
  v_latn_region latn_type := latn_type(290,910,911,912,913,914,915,916,917,919);
BEGIN
/*成功口径定义取配置*/
       /*获取成功口径配置sql
       按父活动查询对应的成功口径*/
          v_rule_sql := 'select a.suc_rule_id, to_char(a.sql_text_order), a.rule_name, c.rel_id, a.report_column
                from mkt_success_rule a, MKT_CAMPAIGN_SUC_RULE_REL b,mkt_campaign_suc_rule_rel c,mkt_success_rule d 
               where a.suc_rule_id = b.suc_rule_id
                 and a.state = 10
                 and b.rel_type = 1
                 and b.campaign_id = :mainCampaignId
                 and a.suc_rule_id =  c.suc_rule_id 
                 and c.suc_rule_id = d.suc_rule_id 
                 and c.rel_type = 2
                 and c.campaign_id =  :campaignId
               order by b.seq  ';

   
    /*按目录查询所有的活动*/
    OPEN campaign_cur FOR 'with latn_table as 
          (SELECT *
            FROM (SELECT DBMS_LOB.SUBSTR(REGEXP_SUBSTR(''290,919,910,911,912,913,914,915,916,917'',
                                                       ''[^,]+'',
                                                       1,
                                                       X.N)) AS PLAYER_GUID 　　FROM DUAL A,
                         (SELECT ROWNUM N FROM DUAL CONNECT BY ROWNUM < 11) X)
           WHERE PLAYER_GUID IS NOT NULL)
          select b.attr_value_id,
                 b.attr_value,
                 a.campaign_id,
                 a.campaign_name,
                 y.PLAYER_GUID latn_id
            from pms_campaign      a,
                 mkt_cam_conf_attr b,
                 common_region     c,
                 latn_table        y
           where a.campaign_id = b.mkt_campaign_id
             and a.region_id = c.region_id
             and a.end_date > trunc(sysdate)-7
             and a.state = ''10A''
             and b.attr_nbr = ''MKT_CPC_CAMPAIGN_CATALOG''
             and attr_value_id = :1
             and c.region_id_old is null
              and exists (select 1 from pms_campaign_chl_rel h
                                 where a.campaign_id = h.campaign_id
                                 and h.channel_id =  ''6'')
          union all 
          select b.attr_value_id,
                 b.attr_value,
                 a.campaign_id,
                 a.campaign_name,
                 to_char(c.region_id_old) latn_id
            from pms_campaign      a,
                 mkt_cam_conf_attr b,
                 common_region     c
           where a.campaign_id = b.mkt_campaign_id
             and a.region_id = c.region_id
             and a.end_date > trunc(sysdate)-7
             and a.state = ''10A''
             and b.attr_nbr = ''MKT_CPC_CAMPAIGN_CATALOG''
             and attr_value_id = :2
             and c.region_id_old is not null
              and exists (select 1 from pms_campaign_chl_rel h
                                 where a.campaign_id = h.campaign_id
                                 and h.channel_id =  ''6'') '
       USING mktCampaignId, mktCampaignId;
       LOOP FETCH campaign_cur INTO v_main_campaign_id,v_main_campaign_name,v_sub_campaign_id,v_sub_campaign_name,v_latn_id;
       exit when campaign_cur%notfound;
       
       /*取出成功口径 放入数组，方便后续使用，不需每次再重新执行获取sql
   此处改为在查询了子活动以后执行.. 成功口径取，目录定义的 和 目录下活动定义的收单口径的交集
   */
         v_sucRuleIds.delete();
         v_sucRuleNames.delete();
         v_sucRuleSqls.delete();
         v_sucRuleRelIds.delete();
         v_sucRuleCols.delete();
         v_num := 0;
         v_rule_campaign_id := mktCampaignId;
            open rule_cur for v_rule_sql using mktCampaignId,v_sub_campaign_id;

            loop
              fetch rule_cur into  v_suc_rule_id,v_suc_sql_text,v_suc_rule_name,v_rel_id,v_suc_rule_col;
              exit when  rule_cur%notfound;
              v_num := v_num+1;
              v_sucRuleIds.extend(1);
              v_sucRuleNames.extend(1);
              v_sucRuleSqls.extend(1);
              v_sucRuleRelIds.extend(1);
              v_sucRuleCols.extend(1);
              v_sucRuleIds(v_num) := v_suc_rule_id;
              v_sucRuleNames(v_num) := v_suc_rule_name;
              v_sucRuleSqls(v_num) := v_suc_sql_text;
              v_sucRuleRelIds(v_num) := v_rel_id;
              v_sucRuleCols(v_num) := v_suc_rule_col;
            end loop;
            close rule_cur;
            
            if v_latn_id = '999' then
	             /*添加省集约外呼 */
               execute immediate 'insert into CALLOUT_ORDER_CREATE_DETAIL
                    (main_campaign_id   ,
                    main_campaign_name ,
                    sub_campaign_id    ,
                    sub_campaign_name  ,
                    region_id          ,
                    prov_dept_id       ,
                    prov_dept_name     ,
                    city_dept_id       ,
                    city_dept_name     ,
                    team_dept_id       ,
                    team_dept_name     ,
                    class_dept_id      ,
                    class_dept_name    ,
                    caller_id          ,
                    caller_name        ,
                    list_id            ,
                    status_cd          ,
                    create_date        ,
                    is_success         ,
                    order_result       ,
                    update_date        ,
                    cust_name          ,
                    prod_inst_id       ,
                    cust_id,
                    city_region
                    )
                    select distinct :1   main_campaign_id,
                                    :2   main_campaign_name,
                                    :3   sub_campaign_id,
                                    :4   sub_campaign_name,
                                    x.*,
                                    null                     team_dept_id,
                                    null                     team_dept_name,
                                    null                     class_dept_id,
                                    null                     class_dept_name,
                                    null                     caller_id,
                                    null                     caller_name,
                                    b.initiative_order_id    list_id,
                                    b.status_cd,
                                    sysdate-1                  create_date,
                                    0                        is_success,
                                    t.order_result,
                                    sysdate-1                  update_date,
                                    a.cust_name,
                                    a.prod_inst_id,
                                    a.cust_id,
                                    a.latn_id
                      from initiative_order_'||v_latn_id||' a,
                           initiative_order_executor_'||v_latn_id||' b,
                           (select city.area_id region_id,
                              prov.bss_org_id prov_dept_id,
                              prov.bss_org_name prov_dept_name,
                              city.bss_org_id city_dept_id,
                              city.bss_org_name city_dept_name
                              from sys_bss_org prov,
                              sys_bss_org city
                              where city.area_id = :5
                              and city.bss_org_type_id = 2
                              and city.up_bss_org_id = prov.bss_org_id
                              and prov.state=''10A''
                              and city.state=''10A'') x,
                           PMS_EXE_ORDER_RESULT_'||v_latn_id||' t
                     where a.initiative_order_id = b.initiative_order_id
                       and t.list_id(+)=a.initiative_order_id
                       and a.state = ''10A''
                       and a.channel_id = 6
                       and not exists (select 1
                              from CALLOUT_ORDER_CREATE_DETAIL t
                             where a.initiative_order_id = t.list_id)
                       and a.campaign_id = :6'
                   using v_main_campaign_id,v_main_campaign_name,v_sub_campaign_id,v_sub_campaign_name,v_latn_id,v_sub_campaign_id;
                   commit;
            else
       
       /*增量入统计明细表*/
       --1200,2100,2200(主动),>=3100
       --班组1101,2200(预测所)
       --团队1101
       --1100
       execute immediate 'insert into CALLOUT_ORDER_CREATE_DETAIL
          (main_campaign_id   ,
          main_campaign_name ,
          sub_campaign_id    ,
          sub_campaign_name  ,
          region_id          ,
          prov_dept_id       ,
          prov_dept_name     ,
          city_dept_id       ,
          city_dept_name     ,
          team_dept_id       ,
          team_dept_name     ,
          class_dept_id      ,
          class_dept_name    ,
          caller_id          ,
          caller_name        ,
          list_id            ,
          status_cd          ,
          create_date        ,
          is_success         ,
          order_result       ,
          update_date        ,
          cust_name          ,
          prod_inst_id       ,
          cust_id
          )
          select distinct :1   main_campaign_id,
                          :2   main_campaign_name,
                          :3   sub_campaign_id,
                          :4   sub_campaign_name,
                          x.*,
                          null                     team_dept_id,
                          null                     team_dept_name,
                          null                     class_dept_id,
                          null                     class_dept_name,
                          null                     caller_id,
                          null                     caller_name,
                          b.initiative_order_id    list_id,
                          b.status_cd,
                          sysdate-1                  create_date,
                          0                        is_success,
                          t.order_result,
                          sysdate-1                  update_date,
                          a.cust_name,
                          a.prod_inst_id,
                          a.cust_id
            from initiative_order_'||v_latn_id||' a,
                 initiative_order_executor_'||v_latn_id||' b,
                 (select city.area_id region_id,
                    prov.bss_org_id prov_dept_id,
                    prov.bss_org_name prov_dept_name,
                    city.bss_org_id city_dept_id,
                    city.bss_org_name city_dept_name
                    from sys_bss_org prov,
                    sys_bss_org city
                    where city.area_id = :5
                    and city.bss_org_type_id = 2
                    and city.up_bss_org_id = prov.bss_org_id
                    and prov.state=''10A''
                    and city.state=''10A'') x,
                 PMS_EXE_ORDER_RESULT_'||v_latn_id||' t
           where a.initiative_order_id = b.initiative_order_id
             and t.list_id(+)=a.initiative_order_id
             and a.state = ''10A''
             and a.channel_id = 6
             and b.latn_id = x.region_id
             and not exists (select 1
                    from CALLOUT_ORDER_CREATE_DETAIL t
                   where a.initiative_order_id = t.list_id)
             and a.campaign_id = :6
             and a.latn_id = :7'
         using v_main_campaign_id,v_main_campaign_name,v_sub_campaign_id,v_sub_campaign_name,v_latn_id,v_sub_campaign_id,v_latn_id;
         commit;
       
       end if;
         
       /*有状态变更的或营业厅id变更的数据*/
   OPEN order_cur FOR 'select a.initiative_order_id,
                   b.status_cd,
                   a.channel_id,
                   b.dept_id,
                   b.curr_user_id,
                   (select order_result from PMS_EXE_ORDER_RESULT_'||v_latn_id||' where list_id=a.initiative_order_id) order_result,
                   nvl((select c.bss_org_type_id from sys_bss_org c where c.bss_org_id=b.dept_id),0) org_type_id
          from INITIATIVE_ORDER_'||v_latn_id||' a, INITIATIVE_ORDER_EXECUTOR_'||v_latn_id||' b,CALLOUT_ORDER_CREATE_DETAIL c
         where a.initiative_order_id = b.initiative_order_id
           and a.state = ''10A''
           and a.channel_id = 6
           and b.status_cd not in (1100,7000)
           and b.last_opt_date >= trunc(sysdate)-1
           and b.last_opt_date < trunc(sysdate)
           and a.initiative_order_id = c.list_id
           and b.status_cd <> c.status_cd
           and a.campaign_id = :1'
     USING v_sub_campaign_id;
     LOOP
       FETCH order_cur  INTO  v_order_id,v_status_cd,v_channel_id,v_dept_id,v_curr_user_id,v_order_result,v_org_type_id;
       exit when order_cur%notfound;
       begin
       
       if v_curr_user_id is not null then
         execute immediate 'select 
                                 b.bss_org_name class_dept_name,
                                 g.bss_org_id team_dept_id,
                                 g.bss_org_name team_dept_name
                          from sys_bss_org b,
                          sys_bss_org g
                          where b.bss_org_id=:1
                          and b.up_bss_org_id = g.bss_org_id
                          and b.state=''10A''
                          and g.state=''10A'''
          into v_dept_name,v_up_dept_id,v_up_dept_name
          using v_dept_id;
          
          execute immediate 'select t.user_name from sys_user t where t.user_id=:1 and t.state=''10A'''
          into v_curr_user_name using v_curr_user_id;
         if v_order_result is not null then
             execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a
                 set old_status_cd   = status_cd,
                    old_class_dept_id= old_class_dept_id,
                    status_cd        = :1,
                    update_date      = sysdate-1,
                    team_dept_id     = :2,
                    team_dept_name   = :3,
                    class_dept_id    = :4,
                    class_dept_name  = :5,
                    caller_id        = :6,
                    caller_name      = :7,
                    order_result     = :8
              where a.list_id = :9  ' using v_status_cd,v_up_dept_id,v_up_dept_name, v_dept_id, v_dept_name,v_curr_user_id,v_curr_user_name,v_order_result,v_order_id;
         else
            execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a
               set old_status_cd   = status_cd,
                  old_class_dept_id= old_class_dept_id,
                  status_cd        = :1,
                  update_date      = sysdate-1,
                  team_dept_id     = :2,
                  team_dept_name   = :3,
                  class_dept_id    = :4,
                  class_dept_name  = :5,
                  caller_id        = :6,
                  caller_name      = :7
            where a.list_id = :8  ' using v_status_cd,v_up_dept_id,v_up_dept_name, v_dept_id, v_dept_name,v_curr_user_id,v_curr_user_name,v_order_id;
         end if;

       elsif v_dept_id is not null then
         --团队
         if v_org_type_id = 3 then
           execute immediate 'select c.bss_org_name team_dept_name
             from sys_bss_org c
            where c.bss_org_id = :1 and c.state=''10A''' into v_dept_name using v_dept_id;
            execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a
               set old_status_cd   = status_cd,
                  status_cd        = :1,
                  update_date      = sysdate-1,
                  team_dept_id     = :2,
                  team_dept_name   = :3
            where a.list_id = :4  ' using v_status_cd, v_dept_id, v_dept_name,v_order_id;
         end if;
         --预测所
         if v_org_type_id = 4 then
           if v_order_result is not null then
              execute immediate 'select 
                                 b.bss_org_name class_dept_name,
                                 g.bss_org_id team_dept_id,
                                 g.bss_org_name team_dept_name
                                from sys_bss_org b,
                                sys_bss_org g
                                where b.bss_org_id=:1
                                and b.up_bss_org_id = g.bss_org_id
                                and b.state=''10A''
                                and g.state=''10A'''
                into v_dept_name,v_up_dept_id,v_up_dept_name
                using v_dept_id;
             
               execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a
                 set old_status_cd   = status_cd,
                    old_class_dept_id= old_class_dept_id,
                    status_cd        = :1,
                    update_date      = sysdate-1,
                    team_dept_id     = :2,
                    team_dept_name   = :3,
                    class_dept_id    = :4,
                    class_dept_name  = :5,
                    order_result     = :6
              where a.list_id = :7  ' using v_status_cd,v_up_dept_id,v_up_dept_name, v_dept_id, v_dept_name,v_order_result,v_order_id;

             
           else
             execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a
               set old_status_cd   = status_cd,
                  old_class_dept_id= old_class_dept_id,
                  status_cd        = :1,
                  update_date      = sysdate-1,
                  team_dept_id     = :2,
                  team_dept_name   = :3,
                  class_dept_id    = :4,
                  class_dept_name  = :5
            where a.list_id = :6  ' using v_status_cd,v_up_dept_id,v_up_dept_name, v_dept_id, v_dept_name,v_order_id;
           end if;
         end if;
       end if;
       
        commit;
       
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
          EXECUTE IMMEDIATE 'insert into exception_outcall(errormsg,pro_name,errorcode) values(''ORA-01403: no data found'',
          ''p_order_outcall_detail_call'','''||v_order_id||''')';
          commit;
          when others then
           errorCode := SQLCODE;      
           errorMsg := '活动['||v_sub_campaign_id||']的工单['||v_order_id||']'||SUBSTR(SQLERRM, 1, 200);
          EXECUTE IMMEDIATE 'insert into exception_outcall(errormsg,pro_name,errorcode) values('''||errorMsg||''',
          ''p_order_outcall_detail_call'','''||errorCode||''')';
          commit;
          CONTINUE;
        END;
      

     END LOOP;
     CLOSE order_cur;

   /*客户下有订单数据的数据，开刷*/
    if v_num > 0 then
      
    if v_latn_id = '999' then
      for j in 1 .. v_latn_region.count loop
        OPEN order_rule_cur FOR 'select a.initiative_order_id,
                   b.status_cd,
                   a.channel_id,
                   1,
                   a.prod_inst_id,
                   a.cust_id
              from INITIATIVE_ORDER_'||v_latn_id||' a, INITIATIVE_ORDER_EXECUTOR_'||v_latn_id||' b
             where a.initiative_order_id = b.initiative_order_id
               and a.channel_id = 6
               and exists (select 1
                      from CUSTOMER_ORDER_SYN_'||v_latn_region(j)||' c
                     where c.cust_id = a.cust_id
                       and c.status_cd = 300000
                       and c.status_date >= trunc(sysdate) - 1
                       and c.status_date < trunc(sysdate))
                 and a.campaign_id = :1
                 and a.latn_id = :2'
         USING v_sub_campaign_id,v_latn_region(j);
         LOOP
           FETCH order_rule_cur  INTO  v_order_id,v_status_cd,v_channel_id,v_hasorder,v_prodInstId,v_custId;
           exit when order_rule_cur%notfound;

            v_is_success := 0;
           execute immediate 'select nvl((select  nvl(d.is_success,0) from CALLOUT_ORDER_CREATE_DETAIL d where d.list_id = :1 ),0) from dual'
            into v_old_is_success using v_order_id;

            
            /*从数组中获取成功口径*/
            for i in 1..v_num loop
             v_conditiontext := 'custId:'||v_custId||',prodInstId:'||v_prodInstId||',campaignId:'||v_rule_campaign_id||',sucRuleId:'||
               v_sucRuleIds(i)||',relId:'||v_sucRuleRelIds(i);
               v_hassucnum := 0;
               if v_old_is_success = 1 then
                 /*如果原来已经成功了，不需要全量跑口径，
                 判断对应口径是否已经成功了，成功了就不跑了继续下一个*/
                 execute immediate 'select count(1) from callout_order_detail_SUCINFO a where a.list_id = :1 and a.suc_rule_id = :2 '
                 into v_hassucnum using v_order_id, v_sucRuleIds(i);

               end if;
               if v_hassucnum = 0 then
                 
               /*执行口径*/
               v_suc_result := GET_DYNAMIC_SQL_RESULT(v_conditiontext,v_sucRuleSqls(i),v_latn_region(j));
               
               /*失败*/
               if v_suc_result = '-999999999' or v_suc_result = '-1' then 
                 
                  v_sucOrderId := -1;
                 
               /*成功*/
               else
                 
                v_sucOrderId := to_number(v_suc_result);
                
                 /*1，处理明细状态*/
                  if v_old_is_success = 0 then
                     
                             
                     /*更新工单执行表的状态信息*/     
                     if v_status_cd > 2000 then
                       execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2 ,suc_order_id= :3
                             where list_id = :4
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;  
                     
	                      execute immediate 'update initiative_order_executor_'||v_latn_id||' t
                                           set t.is_success    = 1,
                                               t.suc_rule_id   = :1,
                                               t.suc_rule_name = :2,     
                                               t.suc_order_id  = :3,
                                               t.suc_time      = sysdate,
                                               t.status_date   = sysdate,
                                               t.last_opt_date = sysdate,
                                               t.last_opt_desc = ''successed complete''
                                         where t.initiative_order_id = :4' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
	                   else
                       execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set status_cd = 7000 , is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2 ,suc_order_id= :3
                             where list_id = :4
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
                         execute immediate 'update initiative_order_executor_'||v_latn_id||' t
                                           set t.is_success    = 1,
                                               t.suc_rule_id   = :1,
                                               t.suc_rule_name = :2,     
                                               t.suc_order_id  = :3,
                                               t.suc_time      = sysdate,
                                               t.hand_call     = 0,
                                               t.status_date   = sysdate,
                                               t.status_cd     = 7000,
                                               t.last_opt_date = sysdate,
                                               t.last_opt_desc = ''successed complete''
                                         where t.initiative_order_id = :4' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id; 
                     end if;     
                             
                  end if;

           /*修改 口径字段值为 1*/
                  execute immediate 'update CALLOUT_ORDER_CREATE_DETAIL a set '||v_sucRuleCols(i) ||' = 1 where list_id = :1 '
                          using v_order_id;

                  execute immediate 'insert into callout_order_detail_SUCINFO(list_id, suc_rule_id, suc_date, suc_rule_name,suc_order_id)
                                    values
                                    (:1, :2, sysdate-1, :3, :4)' using v_order_id,v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId;

               end if;
               commit;
                
               end if;
             
/*             \*执行口径*\
               v_is_success := to_number(GET_DYNAMIC_SQL_RESULT(v_conditiontext,v_sucRuleSqls(i),v_latn_id));
               if v_is_success = 1 then
               \*1，处理明细状态*\
                  if v_old_is_success = 0 then
                     execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2
                             where list_id = :3
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_order_id;
                  end if;

           \*修改 口径字段值为 1*\
                  execute immediate 'update CALLOUT_ORDER_CREATE_DETAIL a set '||v_sucRuleCols(i) ||' = 1 where list_id = :1 '
                          using v_order_id;

                  execute immediate 'insert into callout_order_detail_SUCINFO(list_id, suc_rule_id, suc_date, suc_rule_name)
                                    values
                                    (:1, :2, sysdate-1, :3)' using v_order_id,v_sucRuleIds(i),v_sucRuleNames(i);

               end if;
               end if;*/
               

        end loop;
        end loop;
       CLOSE order_rule_cur;
      
      end loop;
    else
    
      OPEN order_rule_cur FOR 'select a.initiative_order_id,
                   b.status_cd,
                   a.channel_id,
                   1,
                   a.prod_inst_id,
                   a.cust_id
              from INITIATIVE_ORDER_'||v_latn_id||' a, INITIATIVE_ORDER_EXECUTOR_'||v_latn_id||' b
             where a.initiative_order_id = b.initiative_order_id
               and a.channel_id = 6
               and exists (select 1
                      from CUSTOMER_ORDER_SYN_'||v_latn_id||' c
                     where c.cust_id = a.cust_id
                       and c.status_cd = 300000
                       and c.status_date >= trunc(sysdate) - 1
                       and c.status_date < trunc(sysdate))
                 and a.campaign_id = :1'
         USING v_sub_campaign_id;
         LOOP
           FETCH order_rule_cur  INTO  v_order_id,v_status_cd,v_channel_id,v_hasorder,v_prodInstId,v_custId;
           exit when order_rule_cur%notfound;

            v_is_success := 0;
           execute immediate 'select nvl((select  nvl(d.is_success,0) from CALLOUT_ORDER_CREATE_DETAIL d where d.list_id = :1 ),0) from dual'
            into v_old_is_success using v_order_id;

            
            /*从数组中获取成功口径*/
            for i in 1..v_num loop
             v_conditiontext := 'custId:'||v_custId||',prodInstId:'||v_prodInstId||',campaignId:'||v_rule_campaign_id||',sucRuleId:'||
               v_sucRuleIds(i)||',relId:'||v_sucRuleRelIds(i);
               v_hassucnum := 0;
               if v_old_is_success = 1 then
                 /*如果原来已经成功了，不需要全量跑口径，
                 判断对应口径是否已经成功了，成功了就不跑了继续下一个*/
                 execute immediate 'select count(1) from callout_order_detail_SUCINFO a where a.list_id = :1 and a.suc_rule_id = :2 '
                 into v_hassucnum using v_order_id, v_sucRuleIds(i);

               end if;
               if v_hassucnum = 0 then
                 
               /*执行口径*/
               v_suc_result := GET_DYNAMIC_SQL_RESULT(v_conditiontext,v_sucRuleSqls(i),v_latn_id);
               
               /*失败*/
               if v_suc_result = '-999999999' or v_suc_result = '-1' then 
                 
                  v_sucOrderId := -1;
                 
               /*成功*/
               else
                 
                v_sucOrderId := to_number(v_suc_result);
                
                 /*1，处理明细状态*/
                  if v_old_is_success = 0 then
                    
                             
                     /*更新工单执行表的状态信息*/
                     if v_status_cd > 2000 then
                       
                     execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2 ,suc_order_id= :3
                             where list_id = :4
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
	                      execute immediate 'update initiative_order_executor_'||v_latn_id||' t
                                           set t.is_success    = 1,
                                               t.suc_rule_id   = :1,
                                               t.suc_rule_name = :2,     
                                               t.suc_order_id  = :3,
                                               t.suc_time      = sysdate,
                                               t.status_date   = sysdate,
                                               t.last_opt_date = sysdate,
                                               t.last_opt_desc = ''successed complete''
                                         where t.initiative_order_id = :4' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
	                   else
                       execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set status_cd = 7000 , is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2 ,suc_order_id= :3
                             where list_id = :4
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id;
                         execute immediate 'update initiative_order_executor_'||v_latn_id||' t
                                           set t.is_success    = 1,
                                               t.suc_rule_id   = :1,
                                               t.suc_rule_name = :2,     
                                               t.suc_order_id  = :3,
                                               t.suc_time      = sysdate,
                                               t.hand_call     = 0,
                                               t.status_date   = sysdate,
                                               t.status_cd     = 7000,
                                               t.last_opt_date = sysdate,
                                               t.last_opt_desc = ''successed complete''
                                         where t.initiative_order_id = :4' using v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId,v_order_id; 
                     end if;     
                             
                             
                  end if;

           /*修改 口径字段值为 1*/
                  execute immediate 'update CALLOUT_ORDER_CREATE_DETAIL a set '||v_sucRuleCols(i) ||' = 1 where list_id = :1 '
                          using v_order_id;

                  execute immediate 'insert into callout_order_detail_SUCINFO(list_id, suc_rule_id, suc_date, suc_rule_name,suc_order_id)
                                    values
                                    (:1, :2, sysdate-1, :3, :4)' using v_order_id,v_sucRuleIds(i),v_sucRuleNames(i),v_sucOrderId;

               end if;
               commit;
                
               end if;
             
/*             \*执行口径*\
               v_is_success := to_number(GET_DYNAMIC_SQL_RESULT(v_conditiontext,v_sucRuleSqls(i),v_latn_id));
               if v_is_success = 1 then
               \*1，处理明细状态*\
                  if v_old_is_success = 0 then
                     execute immediate ' update CALLOUT_ORDER_CREATE_DETAIL a set is_success = 1 , update_date = sysdate-1 ,suc_rule_id=:1,
                              suc_rule_name =:2
                             where list_id = :3
                             and nvl(is_success,0) = 0 ' using v_sucRuleIds(i),v_sucRuleNames(i),v_order_id;
                  end if;

           \*修改 口径字段值为 1*\
                  execute immediate 'update CALLOUT_ORDER_CREATE_DETAIL a set '||v_sucRuleCols(i) ||' = 1 where list_id = :1 '
                          using v_order_id;

                  execute immediate 'insert into callout_order_detail_SUCINFO(list_id, suc_rule_id, suc_date, suc_rule_name)
                                    values
                                    (:1, :2, sysdate-1, :3)' using v_order_id,v_sucRuleIds(i),v_sucRuleNames(i);

               end if;
               end if;*/
               

        end loop;
        end loop;
       CLOSE order_rule_cur;
       
       end if;
       
    end if;

     /*以上明细跑完了，后面跑针对活动的统计，以及按天的统计*/
       END LOOP;
       CLOSE campaign_cur;


end P_ININTIATIVE_ORDER_outcall_REPORT;


procedure p_synch_callout_bssorg_table is

  begin

      --清理映射表
       execute immediate 'truncate table callout_bssorg_caller_rel';

       --同步数据
       insert into callout_bssorg_caller_rel
       select distinct
               class_dept.area_id      region_id,
               provi_dept.bss_org_id   prov_dept_id,
               provi_dept.bss_org_name prov_dept_name,
               latn_dept.bss_org_id    city_dept_id,
               latn_dept.bss_org_name  city_dept_name,
               group_dept.bss_org_id   team_dept_id,
               group_dept.bss_org_name team_dept_name,
               class_dept.bss_org_id   class_dept_id,
               class_dept.bss_org_name class_dept_name,
               u.user_id caller_id,u.user_name caller_name,sysdate-1
          from sys_bss_org class_dept,
               sys_bss_org group_dept,
               sys_bss_org latn_dept,
               sys_bss_org provi_dept,
               sys_user_job t,
               sys_user u
         where class_dept.bss_org_type_id = 4
           and class_dept.up_bss_org_id = group_dept.bss_org_id
           and group_dept.up_bss_org_id = latn_dept.bss_org_id
           and latn_dept.up_bss_org_id = provi_dept.bss_org_id
           and t.role_id in (10004,10070,10071)
           and t.user_id = u.user_id
           and t.bss_org_id = class_dept.bss_org_id;
      /* \*员工是班组*\
       select class_dept.area_id region_id,
        provi_dept.bss_org_id prov_dept_id,provi_dept.bss_org_name prov_dept_name,
        latn_dept.bss_org_id city_dept_id,latn_dept.bss_org_name city_dept_name,
        group_dept.bss_org_id team_dept_id,group_dept.bss_org_name team_dept_name,
        class_dept.bss_org_id class_dept_id,class_dept.bss_org_name class_dept_name,
        u.user_id caller_id,u.user_name caller_name,sysdate-1
       from sys_bss_org class_dept,
       sys_bss_org group_dept,
       sys_bss_org latn_dept,
       sys_bss_org provi_dept,
       sys_user u
       where class_dept.bss_org_dep=6 and class_dept.bss_org_type_id=4
       and class_dept.up_bss_org_id=group_dept.bss_org_id
       and group_dept.up_bss_org_id=latn_dept.bss_org_id
       and latn_dept.up_bss_org_id=provi_dept.bss_org_id
       and u.dept_id=class_dept.bss_org_id
       union
       \*员工是外呼团队*\
       select group_dept.area_id region_id,
        provi_dept.bss_org_id prov_dept_id,provi_dept.bss_org_name prov_dept_name,
        latn_dept.bss_org_id city_dept_id,latn_dept.bss_org_name city_dept_name,
        group_dept.bss_org_id team_dept_id,group_dept.bss_org_name team_dept_name,
        null class_dept_id,null class_dept_name,
        u.user_id caller_id,u.user_name caller_name,sysdate-1
       from sys_user u,
       sys_bss_org group_dept,
       sys_bss_org latn_dept,
       sys_bss_org provi_dept
       where group_dept.bss_org_dep=6 and group_dept.bss_org_type_id=3
       and group_dept.up_bss_org_id=latn_dept.bss_org_id
       and latn_dept.up_bss_org_id=provi_dept.bss_org_id
       and u.dept_id=group_dept.bss_org_id
       union
       \*员工是市级代理*\
       select latn_dept.area_id region_id,
        provi_dept.bss_org_id prov_dept_id,provi_dept.bss_org_name prov_dept_name,
        latn_dept.bss_org_id city_dept_id,latn_dept.bss_org_name city_dept_name,
        null team_dept_id,null team_dept_name,
        null class_dept_id,null class_dept_name,
        u.user_id caller_id,u.user_name caller_name,sysdate-1
       from sys_user u,
       sys_bss_org latn_dept,
       sys_bss_org provi_dept
       where latn_dept.bss_org_dep=6 and latn_dept.bss_org_type_id=2
       and latn_dept.up_bss_org_id=provi_dept.bss_org_id
       and u.dept_id=latn_dept.bss_org_id
       union
       \*员工是省级管理*\
       select provi_dept.area_id region_id,
       provi_dept.bss_org_id prov_dept_id,provi_dept.bss_org_name prov_dept_name,
       null city_dept_id,null city_dept_name,
        null team_dept_id,null team_dept_name,
        null class_dept_id,null class_dept_name,
        u.user_id caller_id,u.user_name caller_name,sysdate-1
       from sys_user u,
       sys_bss_org provi_dept
       where provi_dept.bss_org_dep=6 and provi_dept.bss_org_type_id=1
       and u.dept_id=provi_dept.bss_org_id;*/
       commit;

  end p_synch_callout_bssorg_table;


procedure P_SYNCH_CHANNEL_TABLE(i number)is

  begin

       DBMS_OUTPUT.PUT_LINE(i);
      --清理映射表
       execute immediate 'truncate table  tb_pty_grid_tree';
       execute immediate 'truncate table  channel';
       execute immediate 'truncate table  pms_exe.organization';
       execute immediate 'truncate table  chn_sale_outlets_grid_rel';
       execute immediate 'truncate table  t_grid_org_code_rel';
       execute immediate 'truncate table  staff';
       execute immediate 'truncate table  system_user';
       execute immediate 'truncate table  mkt_campaign_all';
       execute immediate 'truncate table  block_contractor_rela';

       --同步数据
       insert into tb_pty_grid_tree
        select
         TREE_ID,
         TREE_NAME,
         P_TREE_ID,
         TREE_LEVEL,
         ORDERS,
         NET_CODE,
         LATN_ID,
         LATN_NAME,
         CHANNEL_TYPE,
         ADMIN_FLAG,
         STATE,
         CREATE_DATE,
         AREA_ID,
         Y_AREA_ID,
         TREE_REMARK,
         CHANNEL_NAME,
         CHANNEL_SECEND_TYPE,
         CHANNEL_SECEND_TYPE_NAME,
         CHANNEL_THIRD_TYPE,
         CHANNEL_THIRD_TYPE_NAME,
         CHANNEL_LEVEL,
         CHANNEL_LEVEL_NAME,
         CHANNEL_SECEND_LEVEL,
         CHANNEL_SECEND_LEVEL_NAME,
         ADMIN_NAME,
         ADMIN_TEL,
         ADMIN_ADDR,
         CHANNEL_TYPE_NAME,
         CHANNEL_ID,
         CHANNEL_FENLEI_ID,
         CHANNEL_FENLEI_NAME,
         CHANNEL_CODE,
         STATE_DATE,
         CHANNEL_MANAGE_PRO,
         CHANNEL_MANAGE_PRO_NAME,
         BUSINESS_EXC,
         ROLE_TYPE,
         OBU_TYPE_1,
         OBU_TYPE_2,
         OBU_TYPE_3,
         TREE_ID_PATH,
         CONTRACT_MODEL,
         PEOPLETYPE,
         CUSTMOER_QUN,
         BUS_USER_RATE,
         CONTRACT_MODEL_FIVE,
         CONTRACT_CODE,
         ACTIVITY_PROPERTYL_FIVE,
         AREA_ATTRIBUTE_FIVE,
         BLOCK_TYPE_1,
         BLOCK_TYPE_2,
         BLOCK_TYPE_3,
         CONTRACT_TYPE
          from mmsuser.tb_pty_grid_tree@MMDB_link;
       commit;

      insert into channel
        select CHANNEL_ID,
               CHANNEL_NAME,
               CHANNEL_LEVEL_CD,
               CHANNEL_TYPE_CD,
               CHANNEL_SUBTYPE_CD,
               STATUS_CD,
               PARENT_CHN_ID,
               CHANNEL_NBR,
               COMMON_REGION_ID,
               PROV_CODE,
               CHANNEL_SPEC_ID,
               CAPACITY,
               START_DT,
               END_DT,
               OPEN_TIME,
               CLOSE_TIME,
               DESCRIPTION,
               VERSION,
               CC_CODE_MKT,
               CC_CODE_PART,
               CC_CODE_OPER,
               OPER_FLAG,
               STATUS_DATE,
               CC_PROVINCE,
               CC_CITY,
               CC_COUNTY,
               CC_TOWN,
               CC_ADDR,
               IS_IPHONE,
               LATN_ID,
               ZONE_ID,
               CC_NUMBER_OPER,
               JT_UNI_NUMBER,
               LORD_NUMBER,
               IF_JT,
               ORG_ID,
               CHANNEL_CLASS,
               ECS_CODE,
               CBS_CODE,
               CHANNEL_THIRD_TYPE,
               ACTION,
               CHANNEL_CREATE_TIME,
               LOC_X,
               LOC_Y,
               GPS_LOC_X,
               GPS_LOC_Y,
               CHN_TYPE_CD,
               FIVE_GRID_ID,
               COMMON_REGION_TOWN_ID,
               APPROVE_NUMBER,
               CHANNEL_PHOTOS,
               EXT_CHANNEL_ID
          from intf_market.channel@MMDB_link;

       commit;
       insert into pms_exe.organization
         select a.org_id,a.common_region_id,org_type,org_name,latn_id,status_cd,org_code,org_jt_code,update_date,jt_uni_number,channel_third_type from
       intf_market.organization@MMDB_link a;
       commit;
       insert into chn_sale_outlets_grid_rel
         select grid_id, chn_sale_outlets_id, modify_date, status from mmsuser.chn_sale_outlets_grid_rel@MMDB_link;
       commit;
       insert into t_grid_org_code_rel
       select a.*,sysdate from grid_org_code_rel a ;
       commit;
      insert into staff
        select STAFF_ID,
               STAFF_CODE,
               ORG_ID,
               STAFF_NAME,
               STAFF_DESC,
               STATUS_CD,
               STATUS_DATE,
               CREATE_DATE,
               PARTY_ID,
               EMPEE_ADDR_DESC,
               EMPEE_EMAIL_ADDR,
               EMPEE_MOB_NO,
               EMPEE_TELE_NO,
               EMPEE_PHS_NO,
               STAFF_TYPE,
               CERT_NUBR,
               LOGIN_NUM,
               LATN_ID,
               PWD_CRT_DATE,
               TITLE,
               IP_ADDRESS,
               MAC,
               EMPEE_LEVEL,
               QQ,
               MSN,
               HR_PERSONID,
               STYLE,
               PTY_LATN_ID,
               GPROVICE,
               URL_MAPPING,
               SITE_ID,
               EFF_DATE,
               EXP_DATE,
               USER_TYPE,
               STAFF_SEX,
               STAFF_BIRTHDAY,
               STAFF_PINYIN,
               PTY_ID
          from staff@link_pub;
      insert into system_user
        select SYSTEM_USER_ID,
               STAFF_ID,
               PASSWORD,
               STATUS_CD,
               STATUS_DATE,
               CREATE_DATE,
               SYSTEM_USER_CODE,
               EFF_DATE,
               EXP_DATE,
               LOGIN_NUM,
               PASSWORD_TIME,
               MOBILE_TELEPHONE,
               MARK,
               LAST_LOGIN_TIME,
               IS_TEST_USER
          from crm30_pub.system_user@link_pub;
        insert into mkt_campaign_all
          select MKT_CAMPAIGN_ID,
                 TIGGER_TYPE,
                 MKT_CAMPAIGN_NAME,
                 PLAN_BEGIN_TIME,
                 PLAN_END_TIME,
                 BEGIN_TIME,
                 END_TIME,
                 MKT_CAMPAIGN_TYPE,
                 MKT_ACTIVITY_NBR,
                 MKT_ACTIVITY_TARGET,
                 MKT_CAMPAIGN_DESC,
                 EXEC_TYPE,
                 EXEC_INVL,
                 EXEC_NUM,
                 STATUS_CD,
                 STATUS_DATE,
                 CREATE_STAFF,
                 CREATE_DATE,
                 UPDATE_STAFF,
                 UPDATE_DATE,
                 REMARK,
                 LAN_ID,
                 MKT_CAMPAIGN_CATEGORY,
                 MKT_CAMPAIGN_FULL_NAME,
                 MKT_MAIN_CAMPAIGN_ID,
                 ASSEMBLE_TEMPLET_ID,
                 LIFE_CYCLE,
                 THEME_ID,
                 ASSESS_TEMPLATE_ID,
                 EVALUATE_DAYS,
                 CYCLE_TYPE,
                 CYCLE_EXPRESSION,
                 CYCLE_ACTIVE_DAYS,
                 CAMPAIGN_TEMPLET_ID,
                 IS_TEMPLATE,
                 PROC_DEF_ID,
                 IS_TEST,
                 AREA_ID,
                 IS_EXIST_WAVE,
                 CYCLE_TIGGER_IMMEDIATE
            from IMS_SN.mkt_campaign;

       INSERT INTO block_contractor_rela(
           latn_id,
           latn_name,
           area_id,
           area_name,
           obu_id,
           obu_name,
           block_id,
           block_name,
           staff_id,
           staff_name,
           staff_code
        )
        select distinct
               latn_id,
               latn_name,
               area_id,
               area_name,
               obu_id,
               obu_name,
               block_id,
               block_name,
               staff_id,
               staff_name,
               staff_code
          from mmsuser.view_tree_block_rele@mmdb_link;
      commit;
  end P_SYNCH_CHANNEL_TABLE;

  procedure p_order_outcall_statistic_data(mktCampaignId varchar2) is
    v_grid1_del_sql varchar2(1000);
    v_grid1_exe_sql varchar2(30000);

    v_grid2_del_sql varchar2(1000);
    v_grid2_exe_sql varchar2(30000);

    v_grid3_del_sql varchar2(1000);
    v_grid3_exe_sql varchar2(30000);

    v_grid4_del_sql varchar2(1000);
    v_grid4_exe_sql varchar2(30000);

  begin
    --省级报表详情
    v_grid1_del_sql := 'delete from callout_order_detail_prov t where t.main_campaign_id = :1';
    EXECUTE IMMEDIATE v_grid1_del_sql using mktCampaignId;

    v_grid1_exe_sql := 'insert into callout_order_detail_prov t
                        (MAIN_CAMPAIGN_ID,
                        MAIN_CAMPAIGN_NAME,
                        SUB_CAMPAIGN_ID,
                        SUB_CAMPAIGN_NAME,
                        REGION_ID,
                        PROV_DEPT_ID,
                        PROV_DEPT_NAME,
                        CITY_DEPT_ID,
                        CITY_DEPT_NAME,
                        ALL_NUM,
                        ASSIGN_NUM,
                        EXE_NUM,
                        CALLSUCC_NUM,
                        NEXT_NUM,
                        MKTSUCC_NUM,
                        MKTFAIL_NUM,
                        CALLFAIL_NUM,
                        VALID_COL_NUM,
                        COL_1_NUM,
                        COL_1_NAME,
                        COL_2_NUM,
                        COL_2_NAME,
                        COL_3_NUM,
                        COL_3_NAME,
                        COL_4_NUM,
                        COL_4_NAME,
                        COL_5_NUM,
                        COL_5_NAME,
                        COL_6_NUM,
                        COL_6_NAME,
                        COL_7_NUM,
                        COL_7_NAME,
                        COL_8_NUM,
                        COL_8_NAME,
                        COL_9_NUM,
                        COL_9_NAME,
                        COL_10_NUM,
                        COL_10_NAME,
                        COL_11_NUM,
                        COL_11_NAME,
                        COL_12_NUM,
                        COL_12_NAME,
                        COL_13_NUM,
                        COL_13_NAME,
                        COL_14_NUM,
                        COL_14_NAME,
                        COL_15_NUM,
                        COL_15_NAME,
                        COL_16_NUM,
                        COL_16_NAME,
                        COL_17_NUM,
                        COL_17_NAME,
                        COL_18_NUM,
                        COL_18_NAME,
                        COL_19_NUM,
                        COL_19_NAME,
                        COL_20_NUM,
                        COL_20_NAME,
                        OR_SEQ)
                        select MAIN_CAMPAIGN_ID,
                              MAIN_CAMPAIGN_NAME,
                              SUB_CAMPAIGN_ID,
                              SUB_CAMPAIGN_NAME,
                              REGION_ID,
                              PROV_DEPT_ID,
                              PROV_DEPT_NAME,
                              CITY_DEPT_ID,
                              CITY_DEPT_NAME,
                              ALL_NUM,
                              ASSIGN_NUM,
                              EXE_NUM,
                              CALLSUCC_NUM,
                              NEXT_NUM,
                              MKTSUCC_NUM,
                              MKTFAIL_NUM,
                              CALLFAIL_NUM,
                              VALID_COL_NUM,
                              COL_1_NUM,
                              COL_1_NAME,
                              COL_2_NUM,
                              COL_2_NAME,
                              COL_3_NUM,
                              COL_3_NAME,
                              COL_4_NUM,
                              COL_4_NAME,
                              COL_5_NUM,
                              COL_5_NAME,
                              COL_6_NUM,
                              COL_6_NAME,
                              COL_7_NUM,
                              COL_7_NAME,
                              COL_8_NUM,
                              COL_8_NAME,
                              COL_9_NUM,
                              COL_9_NAME,
                              COL_10_NUM,
                              COL_10_NAME,
                              COL_11_NUM,
                              COL_11_NAME,
                              COL_12_NUM,
                              COL_12_NAME,
                              COL_13_NUM,
                              COL_13_NAME,
                              COL_14_NUM,
                              COL_14_NAME,
                              COL_15_NUM,
                              COL_15_NAME,
                              COL_16_NUM,
                              COL_16_NAME,
                              COL_17_NUM,
                              COL_17_NAME,
                              COL_18_NUM,
                              COL_18_NAME,
                              COL_19_NUM,
                              COL_19_NAME,
                              COL_20_NUM,
                              COL_20_NAME,
                              OR_SEQ
                          from v_callout_order_detail_prov v
                           where v.main_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid1_exe_sql using mktCampaignId;


    --市级报表详情
    v_grid2_del_sql := 'delete from callout_order_detail_city t where t.main_campaign_id = :1';

    EXECUTE IMMEDIATE v_grid2_del_sql using mktCampaignId;


    v_grid2_exe_sql := 'insert into callout_order_detail_city t
                        (MAIN_CAMPAIGN_ID,
                        MAIN_CAMPAIGN_NAME,
                        SUB_CAMPAIGN_ID,
                        SUB_CAMPAIGN_NAME,
                        REGION_ID,
                        PROV_DEPT_ID,
                        PROV_DEPT_NAME,
                        CITY_DEPT_ID,
                        CITY_DEPT_NAME,
                        team_dept_id,
                        team_dept_name,
                        ALL_NUM,
                        ASSIGN_NUM,
                        EXE_NUM,
                        CALLSUCC_NUM,
                        NEXT_NUM,
                        MKTSUCC_NUM,
                        MKTFAIL_NUM,
                        CALLFAIL_NUM,
                        VALID_COL_NUM,
                        COL_1_NUM,
                        COL_1_NAME,
                        COL_2_NUM,
                        COL_2_NAME,
                        COL_3_NUM,
                        COL_3_NAME,
                        COL_4_NUM,
                        COL_4_NAME,
                        COL_5_NUM,
                        COL_5_NAME,
                        COL_6_NUM,
                        COL_6_NAME,
                        COL_7_NUM,
                        COL_7_NAME,
                        COL_8_NUM,
                        COL_8_NAME,
                        COL_9_NUM,
                        COL_9_NAME,
                        COL_10_NUM,
                        COL_10_NAME,
                        COL_11_NUM,
                        COL_11_NAME,
                        COL_12_NUM,
                        COL_12_NAME,
                        COL_13_NUM,
                        COL_13_NAME,
                        COL_14_NUM,
                        COL_14_NAME,
                        COL_15_NUM,
                        COL_15_NAME,
                        COL_16_NUM,
                        COL_16_NAME,
                        COL_17_NUM,
                        COL_17_NAME,
                        COL_18_NUM,
                        COL_18_NAME,
                        COL_19_NUM,
                        COL_19_NAME,
                        COL_20_NUM,
                        COL_20_NAME,
                        OR_SEQ)
                        select MAIN_CAMPAIGN_ID,
                              MAIN_CAMPAIGN_NAME,
                              SUB_CAMPAIGN_ID,
                              SUB_CAMPAIGN_NAME,
                              REGION_ID,
                              PROV_DEPT_ID,
                              PROV_DEPT_NAME,
                              CITY_DEPT_ID,
                              CITY_DEPT_NAME,
                              team_dept_id,
                              team_dept_name,
                              ALL_NUM,
                              ASSIGN_NUM,
                              EXE_NUM,
                              CALLSUCC_NUM,
                              NEXT_NUM,
                              MKTSUCC_NUM,
                              MKTFAIL_NUM,
                              CALLFAIL_NUM,
                              VALID_COL_NUM,
                              COL_1_NUM,
                              COL_1_NAME,
                              COL_2_NUM,
                              COL_2_NAME,
                              COL_3_NUM,
                              COL_3_NAME,
                              COL_4_NUM,
                              COL_4_NAME,
                              COL_5_NUM,
                              COL_5_NAME,
                              COL_6_NUM,
                              COL_6_NAME,
                              COL_7_NUM,
                              COL_7_NAME,
                              COL_8_NUM,
                              COL_8_NAME,
                              COL_9_NUM,
                              COL_9_NAME,
                              COL_10_NUM,
                              COL_10_NAME,
                              COL_11_NUM,
                              COL_11_NAME,
                              COL_12_NUM,
                              COL_12_NAME,
                              COL_13_NUM,
                              COL_13_NAME,
                              COL_14_NUM,
                              COL_14_NAME,
                              COL_15_NUM,
                              COL_15_NAME,
                              COL_16_NUM,
                              COL_16_NAME,
                              COL_17_NUM,
                              COL_17_NAME,
                              COL_18_NUM,
                              COL_18_NAME,
                              COL_19_NUM,
                              COL_19_NAME,
                              COL_20_NUM,
                              COL_20_NAME,
                              OR_SEQ
                          from v_callout_order_detail_city v
                          where v.main_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid2_exe_sql using mktCampaignId;


    --外呼团队表详情
    v_grid3_del_sql := 'delete from callout_order_detail_team t where t.main_campaign_id = :1';

    EXECUTE IMMEDIATE v_grid3_del_sql using mktCampaignId;


    v_grid3_exe_sql := 'insert into callout_order_detail_team t
                        (MAIN_CAMPAIGN_ID,
                        MAIN_CAMPAIGN_NAME,
                        SUB_CAMPAIGN_ID,
                        SUB_CAMPAIGN_NAME,
                        REGION_ID,
                        PROV_DEPT_ID,
                        PROV_DEPT_NAME,
                        CITY_DEPT_ID,
                        CITY_DEPT_NAME,
                        team_dept_id,
                        team_dept_name,
                        class_dept_id,
                        class_dept_name,
                        ALL_NUM,
                        ASSIGN_NUM,
                        EXE_NUM,
                        CALLSUCC_NUM,
                        NEXT_NUM,
                        MKTSUCC_NUM,
                        MKTFAIL_NUM,
                        CALLFAIL_NUM,
                        VALID_COL_NUM,
                        COL_1_NUM,
                        COL_1_NAME,
                        COL_2_NUM,
                        COL_2_NAME,
                        COL_3_NUM,
                        COL_3_NAME,
                        COL_4_NUM,
                        COL_4_NAME,
                        COL_5_NUM,
                        COL_5_NAME,
                        COL_6_NUM,
                        COL_6_NAME,
                        COL_7_NUM,
                        COL_7_NAME,
                        COL_8_NUM,
                        COL_8_NAME,
                        COL_9_NUM,
                        COL_9_NAME,
                        COL_10_NUM,
                        COL_10_NAME,
                        COL_11_NUM,
                        COL_11_NAME,
                        COL_12_NUM,
                        COL_12_NAME,
                        COL_13_NUM,
                        COL_13_NAME,
                        COL_14_NUM,
                        COL_14_NAME,
                        COL_15_NUM,
                        COL_15_NAME,
                        COL_16_NUM,
                        COL_16_NAME,
                        COL_17_NUM,
                        COL_17_NAME,
                        COL_18_NUM,
                        COL_18_NAME,
                        COL_19_NUM,
                        COL_19_NAME,
                        COL_20_NUM,
                        COL_20_NAME,
                        OR_SEQ)
                        select MAIN_CAMPAIGN_ID,
                              MAIN_CAMPAIGN_NAME,
                              SUB_CAMPAIGN_ID,
                              SUB_CAMPAIGN_NAME,
                              REGION_ID,
                              PROV_DEPT_ID,
                              PROV_DEPT_NAME,
                              CITY_DEPT_ID,
                              CITY_DEPT_NAME,
                              team_dept_id,
                              team_dept_name,
                              class_dept_id,
                              class_dept_name,
                              ALL_NUM,
                              ASSIGN_NUM,
                              EXE_NUM,
                              CALLSUCC_NUM,
                              NEXT_NUM,
                              MKTSUCC_NUM,
                              MKTFAIL_NUM,
                              CALLFAIL_NUM,
                              VALID_COL_NUM,
                              COL_1_NUM,
                              COL_1_NAME,
                              COL_2_NUM,
                              COL_2_NAME,
                              COL_3_NUM,
                              COL_3_NAME,
                              COL_4_NUM,
                              COL_4_NAME,
                              COL_5_NUM,
                              COL_5_NAME,
                              COL_6_NUM,
                              COL_6_NAME,
                              COL_7_NUM,
                              COL_7_NAME,
                              COL_8_NUM,
                              COL_8_NAME,
                              COL_9_NUM,
                              COL_9_NAME,
                              COL_10_NUM,
                              COL_10_NAME,
                              COL_11_NUM,
                              COL_11_NAME,
                              COL_12_NUM,
                              COL_12_NAME,
                              COL_13_NUM,
                              COL_13_NAME,
                              COL_14_NUM,
                              COL_14_NAME,
                              COL_15_NUM,
                              COL_15_NAME,
                              COL_16_NUM,
                              COL_16_NAME,
                              COL_17_NUM,
                              COL_17_NAME,
                              COL_18_NUM,
                              COL_18_NAME,
                              COL_19_NUM,
                              COL_19_NAME,
                              COL_20_NUM,
                              COL_20_NAME,
                              OR_SEQ
                          from v_callout_order_detail_team v
                          where v.main_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid3_exe_sql using mktCampaignId;

    --外呼班组表详情
    v_grid4_del_sql := 'delete from callout_order_detail_class t where t.main_campaign_id = :1';

    EXECUTE IMMEDIATE v_grid4_del_sql using mktCampaignId;


    v_grid4_exe_sql := 'insert into callout_order_detail_class t
                        (MAIN_CAMPAIGN_ID,
                        MAIN_CAMPAIGN_NAME,
                        SUB_CAMPAIGN_ID,
                        SUB_CAMPAIGN_NAME,
                        REGION_ID,
                        PROV_DEPT_ID,
                        PROV_DEPT_NAME,
                        CITY_DEPT_ID,
                        CITY_DEPT_NAME,
                        team_dept_id,
                        team_dept_name,
                        class_dept_id,
                        class_dept_name,
                        caller_id,
                        caller_name,
                        ALL_NUM,
                        ASSIGN_NUM,
                        EXE_NUM,
                        CALLSUCC_NUM,
                        NEXT_NUM,
                        MKTSUCC_NUM,
                        MKTFAIL_NUM,
                        CALLFAIL_NUM,
                        VALID_COL_NUM,
                        COL_1_NUM,
                        COL_1_NAME,
                        COL_2_NUM,
                        COL_2_NAME,
                        COL_3_NUM,
                        COL_3_NAME,
                        COL_4_NUM,
                        COL_4_NAME,
                        COL_5_NUM,
                        COL_5_NAME,
                        COL_6_NUM,
                        COL_6_NAME,
                        COL_7_NUM,
                        COL_7_NAME,
                        COL_8_NUM,
                        COL_8_NAME,
                        COL_9_NUM,
                        COL_9_NAME,
                        COL_10_NUM,
                        COL_10_NAME,
                        COL_11_NUM,
                        COL_11_NAME,
                        COL_12_NUM,
                        COL_12_NAME,
                        COL_13_NUM,
                        COL_13_NAME,
                        COL_14_NUM,
                        COL_14_NAME,
                        COL_15_NUM,
                        COL_15_NAME,
                        COL_16_NUM,
                        COL_16_NAME,
                        COL_17_NUM,
                        COL_17_NAME,
                        COL_18_NUM,
                        COL_18_NAME,
                        COL_19_NUM,
                        COL_19_NAME,
                        COL_20_NUM,
                        COL_20_NAME,
                        OR_SEQ)
                        select MAIN_CAMPAIGN_ID,
                              MAIN_CAMPAIGN_NAME,
                              SUB_CAMPAIGN_ID,
                              SUB_CAMPAIGN_NAME,
                              REGION_ID,
                              PROV_DEPT_ID,
                              PROV_DEPT_NAME,
                              CITY_DEPT_ID,
                              CITY_DEPT_NAME,
                              team_dept_id,
                              team_dept_name,
                              class_dept_id,
                              class_dept_name,
                              caller_id,
                              caller_name,
                              ALL_NUM,
                              ASSIGN_NUM,
                              EXE_NUM,
                              CALLSUCC_NUM,
                              NEXT_NUM,
                              MKTSUCC_NUM,
                              MKTFAIL_NUM,
                              CALLFAIL_NUM,
                              VALID_COL_NUM,
                              COL_1_NUM,
                              COL_1_NAME,
                              COL_2_NUM,
                              COL_2_NAME,
                              COL_3_NUM,
                              COL_3_NAME,
                              COL_4_NUM,
                              COL_4_NAME,
                              COL_5_NUM,
                              COL_5_NAME,
                              COL_6_NUM,
                              COL_6_NAME,
                              COL_7_NUM,
                              COL_7_NAME,
                              COL_8_NUM,
                              COL_8_NAME,
                              COL_9_NUM,
                              COL_9_NAME,
                              COL_10_NUM,
                              COL_10_NAME,
                              COL_11_NUM,
                              COL_11_NAME,
                              COL_12_NUM,
                              COL_12_NAME,
                              COL_13_NUM,
                              COL_13_NAME,
                              COL_14_NUM,
                              COL_14_NAME,
                              COL_15_NUM,
                              COL_15_NAME,
                              COL_16_NUM,
                              COL_16_NAME,
                              COL_17_NUM,
                              COL_17_NAME,
                              COL_18_NUM,
                              COL_18_NAME,
                              COL_19_NUM,
                              COL_19_NAME,
                              COL_20_NUM,
                              COL_20_NAME,
                              OR_SEQ
                          from v_callout_order_detail_class v
                          where v.main_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid4_exe_sql using mktCampaignId;
    commit;
  end p_order_outcall_statistic_data;

    procedure p_order_statistic_data(mktCampaignId varchar2) is


    v_grid1_del_sql varchar2(1000);
    v_grid1_exe_sql varchar2(4000);

    v_grid3_del_sql varchar2(1000);
    v_grid3_exe_sql varchar2(4000);

  begin
      --同步一级网格看数报表内容
    v_grid1_del_sql := 'delete from t_order_receive_detail_sec t where t.z_mkt_campaign_id = :1';
    EXECUTE IMMEDIATE v_grid1_del_sql using mktCampaignId;

    v_grid1_exe_sql := 'insert into t_order_receive_detail_sec t
                        (t.z_mkt_campaign_id,
                         t.campaign_name,
                         t.fir_grid_id,
                         t.sec_grid_id,
                         t.dept_name,
                         t.receive_num,
                         t.receive_suc_num,
                         t.claimed_count,
                         t.exe_count,
                         t.exe_suc_count,
                         t.receive_org_count,
                         t.claimed_org_count,
                         t.suc_rec_per,
                         t.claimed_org_per,
                         t.order_claimed_per,
                         t.report_date,
                         t.report_day,
                         t.col_1,
                         t.col_2,
                         t.col_3,
                         t.col_4,
                         t.col_5,
                         t.col_6,
                         t.col_7,
                         t.col_8,
                         t.col_9,
                         t.col_10,
                         t.col_11,
                         t.col_12,
                         t.col_13,
                         t.col_14,
                         t.col_15,
                         t.col_16,
                         t.col_17,
                         t.col_18,
                         t.col_19,
                         t.col_20,
                         t.cop_channel_id)
                        select v.z_mkt_campaign_id,
                               v.campaign_name,
                               v.fir_grid_id,
                               v.sec_grid_id,
                               v.dept_name,
                               v.receive_num,
                               v.receive_suc_num,
                               v.claimed_count,
                               v.exe_count,
                               v.exe_suc_count,
                               v.receive_org_count,
                               v.claimed_org_count,
                               v.suc_rec_per,
                               v.claimed_org_per,
                               v.order_claimed_per,
                               v.report_date,
                               to_char(v.report_date,''yyyy-mm-dd''),
                               v.col_1,
                               v.col_2,
                               v.col_3,
                               v.col_4,
                               v.col_5,
                               v.col_6,
                               v.col_7,
                               v.col_8,
                               v.col_9,
                               v.col_10,
                               v.col_11,
                               v.col_12,
                               v.col_13,
                               v.col_14,
                               v.col_15,
                               v.col_16,
                               v.col_17,
                               v.col_18,
                               v.col_19,
                               v.col_20,
                               v.cop_channel_id
                          from v_order_receive_detail_sec v
                           where v.Z_MKT_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid1_exe_sql using mktCampaignId;


    --同步三级网格看数报表内容
    v_grid3_del_sql := 'delete from t_order_receive_detail_org t where t.z_mkt_campaign_id = :1';

    EXECUTE IMMEDIATE v_grid3_del_sql using mktCampaignId;


    v_grid3_exe_sql := 'insert into t_order_receive_detail_org t
                        (t.Z_MKT_CAMPAIGN_ID,
                         t.A_MKT_CAMPAIGN_ID,
                         t.CAMPAIGN_NAME,
                         t.C_CAMPAIGN_NAME,
                         t.FIR_GRID_ID,
                         t.SEC_GRID_ID,
                         t.GRID_ID,
                         t.THI_GRID_ID,
                         t.ORG_ID,
                         t.org_jt_code,
                         t.SEC_GRID_NAME,
                         t.THI_GRID_NAME,
                         t.DEPT_NAME,
                         t.RECEIVE_NUM,
                         t.RECEIVE_SUC_NUM,
                         t.CLAIMED_COUNT,
                         t.EXE_COUNT,
                         t.EXE_SUC_COUNT,
                         t.order_claimed_per,
                         t.report_date,
                         t.report_day,
                         t.col_1,
                         t.col_2,
                         t.col_3,
                         t.col_4,
                         t.col_5,
                         t.col_6,
                         t.col_7,
                         t.col_8,
                         t.col_9,
                         t.col_10,
                         t.col_11,
                         t.col_12,
                         t.col_13,
                         t.col_14,
                         t.col_15,
                         t.col_16,
                         t.col_17,
                         t.col_18,
                         t.col_19,
                         t.col_20,
                         t.cop_channel_id)
                        select v.Z_MKT_CAMPAIGN_ID,
                               v.A_MKT_CAMPAIGN_ID,
                               v.CAMPAIGN_NAME,
                               v.C_CAMPAIGN_NAME,
                               v.FIR_GRID_ID,
                               v.SEC_GRID_ID,
                               v.GRID_ID,
                               v.THI_GRID_ID,
                               v.ORG_ID,
                               v.org_jt_code,
                               v.SEC_GRID_NAME,
                               v.THI_GRID_NAME,
                               v.DEPT_NAME,
                               v.RECEIVE_NUM,
                               v.RECEIVE_SUC_NUM,
                               v.CLAIMED_COUNT,
                               v.EXE_COUNT,
                               v.EXE_SUC_COUNT,
                               v.order_claimed_per,
                               v.report_date,
                               to_char(v.report_date,''yyyy-mm-dd''),
                               v.col_1,
                               v.col_2,
                               v.col_3,
                               v.col_4,
                               v.col_5,
                               v.col_6,
                               v.col_7,
                               v.col_8,
                               v.col_9,
                               v.col_10,
                               v.col_11,
                               v.col_12,
                               v.col_13,
                               v.col_14,
                               v.col_15,
                               v.col_16,
                               v.col_17,
                               v.col_18,
                               v.col_19,
                               v.col_20,
                               v.cop_channel_id
                          from v_order_receive_detail_org v
                          where v.Z_MKT_CAMPAIGN_ID = :1';
    EXECUTE IMMEDIATE v_grid3_exe_sql using mktCampaignId;

  end p_order_statistic_data;

/*取字符串拆分字段*/
  function getSplitSize(i_params in varchar2, i_split varchar2) return number is
    v_num number := 1;
  begin
    if (i_params is null) then
      return 0;
    end if;
    while (instr(i_params, i_split, 1, v_num) > 0) loop
      v_num := v_num + 1;
    end loop;
    return v_num;
  end;
 /*
  *  从i_params=1,2,3,4字符串中取第i_num=2值 则返回 2
  *  i_order从开始处计算
  */
  function getSplitValue(i_params in varchar2,
                         i_num    in number,
                         i_split  varchar2,
                         i_order  number) return varchar2 is
    v_params varchar2(500) := i_split || i_params || i_split;
    v_start  number;
    v_end    number;
  begin
    if (i_params is null or i_num is null) then
      return null;
    else
      --从开始计算
      if (i_order > 0) then
        v_start := instr(v_params, i_split, 1, i_num);
        v_end   := instr(v_params, i_split, 1, i_num + 1);
      else
        v_end   := instr(v_params, i_split, -1, i_num);
        v_start := instr(v_params, i_split, -1, i_num + 1);
      end if;
      if (i_num = 0) then
        return null;
      else
        return substr(v_params, v_start + 1, v_end - (v_start + 1));
      end if;
    end if;
  end;

 /*动态执行传入的sql，
 输出返回值，
 本方法只返回一个字段
 有更多要求的可以扩充此方法
 conditiontext 格式如
 custId:1232,prodInstId:23232,latnId:290
 :前参数编码 :后此编码对应的值
 多个编码值对用,连接
 sqlText中参数按入参中的 参数编码[custId] 格式实现
 */
 function GET_DYNAMIC_SQL_RESULT(v_conditiontext varchar2,v_sqltext_in varchar2,v_latnId varchar2)
    return varchar2 is
    vCountNum        number;/*入参数量*/
     valCur          number;
    v_fea_val       varchar2(1000);
    vName           varchar2(200);
    vNames          varchar2(4000);
    vValue           varchar2(200);
    v_sqltext      varchar2(3000);
 begin
      vCountNum := getSplitSize(v_conditiontext, ',');
      --对目标sql进行预分析
      valCur     := dbms_sql.open_cursor;
      v_sqltext := replace(upper(v_sqltext_in), '[LATN_ID]', v_latnId);

      for i in 1 .. vCountNum loop
        vNames := getSplitValue(v_conditiontext, i, ',', 1);
        vName  := upper(getSplitValue(vNames, 1, ':', 1));
        vValue := getSplitValue(vNames, 2, ':', 1);

        v_sqltext := replace(v_sqltext,
                              '[' || vName || ']',
                              ':' || vName || ' ');
      end loop;

      dbms_sql.parse(valCur, v_sqltext, dbms_sql.native);
      for i in 1 .. vCountNum loop
        vNames := getSplitValue(v_conditiontext, i, ',', 1);
        vName  := upper(getSplitValue(vNames, 1, ':', 1));
        vValue := getSplitValue(vNames, 2, ':', 1);

        if instr(v_sqltext, ':' || vName || ' ') > 0 then
          dbms_sql.bind_variable(valCur, ':' || vName, vValue);
        end if;
      end loop;


      dbms_sql.define_column(valCur, 1, v_fea_val, 1000);
      vCountNum := dbms_sql.execute(valCur);
      v_fea_val := '-999999999';
      loop
        exit when dbms_sql.fetch_rows(valCur) <= 0 or v_fea_val != '-999999999' ;
        dbms_sql.column_value(valCur, 1, v_fea_val);

      end loop;
     dbms_sql.close_cursor(valCur);
   return v_fea_val;
   exception
          when others then
            return -1;
 end GET_DYNAMIC_SQL_RESULT;

end PG_REPORT_HANDLE;
