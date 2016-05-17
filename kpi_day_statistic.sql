CREATE DEFINER=`root`@`%` PROCEDURE `kpi_day_statistic`()
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
		  declare day_data_cur cursor for select KPI_ID,KPI_TYPE,Seq_no,Subsys_name,Domain_name,BOL_CLOUD_NAME,stat_time,
		  SUM_VALUE,AVERAGE_VALUE,Max_VALUE,Min_VALUE from c_info_kpi_stat where day(stat_time) in (DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY),'%e')) and kpi_id in (1002);
		  declare continue handler for not found set done=1; 
		  open day_data_cur;
		  read_loop: loop 
		  fetch day_data_cur into kid,ktype,kseq,ksub,kdomain,kname,kdate,ksum,kavg,kmax,kmin;
      if done then 
         LEAVE read_loop;
      end if;
		  insert into c_info_kpi_stat_day values (kid,ktype,kseq,ksub,kdomain,kname,ksum,kavg,kmax,kmin,kdate,2,month(stat_time));
		  end loop read_loop;
		  close day_data_cur;
	  END;


	  
	  
可视建立EVENT从12点开始，每天做一次，call kpi_day_statistic()