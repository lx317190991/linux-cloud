CREATE DEFINER=`root`@`%` PROCEDURE `node_hour_statistic`()
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
		  declare hour_data_cur cursor for select hostname, Ip_addr, avg(cpu),avg(used_RAM),avg(used_Disc_space),avg(io),
		  max(cpu),max(used_RAM),max(used_Disc_space),max(io),min(cpu),min(used_RAM),min(used_Disc_space),min(io), status_time from c_info_node
		  where hour_number in (DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 HOUR),'%l')) group by hostname, Ip_addr; 
      declare continue handler for not found set done=1;  
      open hour_data_cur;
		  read_loop:loop
      fetch hour_data_cur into nhostname,nipaddr,navgcpu,navgram,navgdiscspace,navgio,nmaxcpu,nmaxram,nmaxdiscspace,nmaxio,nmincpu,nminram,nmindiscspace,nminio,
      kdate;
      insert into c_info_node_stat values(nhostname,nipaddr,navgcpu,navgram,navgdiscspace,navgio,nmaxcpu,nmaxram,nmaxdiscspace,nmaxio,nmincpu,nminram,nmindiscspace,nminio,
      kdate,1,month(stat_time));
		  if done then
		  leave read_loop;
		  end if;
      end loop read_loop;
      close hour_data_cur;
        
END;


	  
	  
可视建立EVENT从12点开始，一小时做一次，call node_hour_statistic()

