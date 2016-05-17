CREATE DEFINER=`root`@`%` PROCEDURE `node_day_statistic`()
BEGIN  
		  declare nhostname varchar(80);
		  declare nipaddr varchar(40);

		  declare navgcpu int(4);
		  declare nmaxcpu int(4);
		  declare nmincpu int(4);

		  declare navgram int(12);
		  declare nmaxram int(12);
		  declare nminram int(12);

		  declare navgdiscspace int(12);
		  declare nmaxdiscspace int(12);
		  declare nmindiscspace int(12);

		  declare navgio int(12);
		  declare nmaxio int(12);
		  declare nminio int(12);

		  declare kdate datetime;
      declare done int default -1;  
		  declare day_data_cur cursor for select hostname, Ip_addr, Average_Cpu, Average_RAM, Average_Disc_space,Average_io,max_Cpu,max_RAM,max_Disc_space,
		  max_io,min_Cpu,min_RAM,min_Disc_space,min_io, stat_time from c_info_node_stat
		  where day(stat_time) in (DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY),'%e')); 
      declare continue handler for not found set done=1;  
      open day_data_cur;
		  read_loop:loop
      fetch day_data_cur into nhostname,nipaddr,navgcpu,navgram,navgdiscspace,navgio,nmaxcpu,nmaxram,nmaxdiscspace,nmaxio,nmincpu,nminram,nmindiscspace,nminio,
      kdate;
      insert into c_info_node_stat_day values(nhostname,nipaddr,navgcpu,navgram,navgdiscspace,navgio,nmaxcpu,nmaxram,nmaxdiscspace,nmaxio,nmincpu,nminram,nmindiscspace,nminio,
      kdate,2,month(stat_time));
		  if done then
		  leave read_loop;
		  end if;
      end loop read_loop;
      close day_data_cur;
        
END;