CREATE DEFINER=`root`@`%` PROCEDURE `kpi_hour_statistic`()
BEGIN  
		  declare kid int(4);
		  declare ktype int(2);
		  declare kseq varchar(256);
		  declare ksub varchar(40);
		  declare kdomain varchar(40);
		  declare kname varchar(40);
		  declare ksum int(12);
		  declare kavg int(12);
		  declare kmax int(12);
		  declare kmin int(12);
		  declare kdate datetime;
        declare done int default -1;  
		  declare hour_data_cur cursor for select KPI_ID,KPI_TYPE,Seq_no,Subsys_name,Domain_name,BOL_CLOUD_NAME, create_date,
          sum(kpi_value),avg(kpi_value),max(kpi_value),min(kpi_value) from c_info_kpi
		  where hour_number in (DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 HOUR),'%l')) and kpi_id in (1002) group by KPI_ID,KPI_TYPE,Seq_no,Subsys_name,Domain_name,BOL_CLOUD_NAME;
		  declare continue handler for not found set done=1;  
		  open hour_data_cur;
		  read_loop:loop
		  fetch hour_data_cur into kid,ktype,kseq,ksub,kdomain,kname,kdate,ksum,kavg,kmax,kmin;
      if done then 
      LEAVE read_loop;
      end if;
		  insert into c_info_kpi_stat values (kid,ktype,kseq,ksub,kdomain,kname,ksum,kmax,kavg,kmin,kdate,1,month(stat_time));
		  end loop read_loop;
      close hour_data_cur;
	  truncate table c_info_kpi;
END;


	  
	  
可视建立EVENT从12点开始，一小时做一次，call kpi_hour_statistic()

