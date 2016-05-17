//#define BOOST_SPIRIT_THREADSAFE 
#include <time.h>
#include <map>
#include <iostream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include "CloudScheduleTaskLoop.h"
#include "CheckPoint.h"
#include "json/json.h"

namespace BM35
{
	sessionData g_sessionData;

	void CloudScheduleTaskLoop::refreshBpcbInfo()
	{
		std::vector<MetaBpcbInfo> vBpcbInfo;
		MetaBpcbInfoOp *p = dynamic_cast<MetaBpcbInfoOp *>(
		      ctlOper->getObjectPtr(MetaBpcbInfoOp::getObjName()));
		if (!p->getAllBpcbInfo(vBpcbInfo))
		{
			bmco_error_f2(theLogger, "%s|%s|Fail to getAllBpcbInfo", 
				std::string("0"),std::string(__FUNCTION__));
			return;
		}
		
		std::string cloudStatusStr;
		std::string bpcbInfoPath = ZNODE_ZOOKEEPER_STATUS_BPCB + "/" + g_sessionData.getBolName();

		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(bpcbInfoPath))
			{
				if (!CloudDatabaseHandler::instance()->createNode(bpcbInfoPath, ZOO_EPHEMERAL))
				{
					bmco_error_f3(theLogger, "%s|%s|Faild to createNode %s", 
						std::string("0"), std::string(__FUNCTION__), bpcbInfoPath);
					return;
				}
			}

			Json::Value root(Json::objectValue);
			Json::Value field(Json::objectValue);
			Json::Value record(Json::arrayValue);

			std::string bolName = g_sessionData.getBolName();

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);
			
			field["bpcb_id"] = "bpcbid";
			field["sys_pid"] = "系统进程ID";
			field["program_id"] = "程序ID";
			field["create_date"] = "调度启动时间";
			field["heart_beat"] = "心跳时间";
			field["status"] = "进程状态";
			field["fd_limitation"] = "打开文件限制数";
			field["fd_inusing"] = "已打开文件数";
			field["cpu"] = "cpu使用率";
			field["ram"] = "RAM使用数";
			field["flow_id"] = "所属流程ID";
			field["instance_id"] = "实例ID";
			field["task_source"] = "任务来源";
			field["source_id"] = "来源ID";
			field["snapshot"] = "snapshot标志位";
			field["action_flag"] = "action";
			field["bol_name"] = "bolname";
			field["status_time"] = "状态变更时间";

			for (int i = 0; i < vBpcbInfo.size(); i++)
			{
				LocalDateTime dt(vBpcbInfo[i].m_tHeartBeat);
				std::string stHeartBeat = DateTimeFormatter::format(dt, DateTimeFormat::SORTABLE_FORMAT);
		 		dt = vBpcbInfo[i].m_tCreateTime;
	    		std::string stCreateTime = DateTimeFormatter::format(dt, DateTimeFormat::SORTABLE_FORMAT);

				Json::Value array(Json::objectValue);
				array["bpcb_id"] = Bmco::format("%?d", vBpcbInfo[i].m_iBpcbID);
				array["sys_pid"] = Bmco::format("%?d", vBpcbInfo[i].m_iSysPID);
				array["program_id"] = Bmco::format("%?d", vBpcbInfo[i].m_iProgramID);
				array["create_date"] = stCreateTime;
				array["heart_beat"] = stHeartBeat;
				array["status"] = Bmco::format("%?d", vBpcbInfo[i].m_iStatus);
				array["fd_limitation"] = Bmco::format("%?d", vBpcbInfo[i].m_iFDLimitation);
				array["fd_inusing"] = Bmco::format("%?d", vBpcbInfo[i].m_iFDInUsing);
				array["cpu"] = Bmco::format("%?d", vBpcbInfo[i].m_iCPU);
				array["ram"] = Bmco::format("%?d", vBpcbInfo[i].m_iRAM);
				array["flow_id"] = Bmco::format("%?d", vBpcbInfo[i].m_iFlowID);
				array["instance_id"] = Bmco::format("%?d", vBpcbInfo[i].m_iInstanceID);
				array["task_source"] = Bmco::format("%?d", vBpcbInfo[i].m_iTaskSource);
				array["source_id"] = Bmco::format("%?d", vBpcbInfo[i].m_iSourceID);
				array["snapshot"] = Bmco::format("%?d", vBpcbInfo[i].m_iSnapShot);
				array["action_flag"] = Bmco::format("%?d", vBpcbInfo[i].m_iAction);
				array["bol_name"] = bolName;
				array["status_time"] = stNow;
				record[i] = array;
			}

			root["name"] =  "c_info_bpcb";
			root["desc"] =  "bpcb信息";
			root["field"] = field;
			root["record"] = record;

			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(bpcbInfoPath, oss))
			{
				bmco_error(theLogger, "Faild to write  MetaBpcbInfo");
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}
	}

	void CloudScheduleTaskLoop::refreshChunkInfo()
	{
		std::vector<MetaChunkInfo> vChunkInfo;
		MetaChunkInfoOp *p = dynamic_cast<MetaChunkInfoOp *>(
		      ctlOper->getObjectPtr(MetaChunkInfoOp::getObjName()));
		if (!p->Query(vChunkInfo))
		{
			bmco_error_f2(theLogger, "%s|%s|Fail to getAllChunkInfo", 
				std::string("0"),std::string(__FUNCTION__));
			return;
		}
		
		std::string cloudStatusStr;
		std::string chunkInfoPath = ZNODE_ZOOKEEPER_STATUS_CHUNK + "/" + g_sessionData.getBolName();

		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(chunkInfoPath))
			{
				if (!CloudDatabaseHandler::instance()->createNode(chunkInfoPath, ZOO_EPHEMERAL))
				{
					bmco_error_f3(theLogger, "%s|%s|Faild to createNode %s", 
						std::string("0"), std::string(__FUNCTION__), chunkInfoPath);
					return;
				}
			}

			Json::Value root(Json::objectValue);
			Json::Value field(Json::objectValue);
			Json::Value record(Json::arrayValue);

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);

			field["chunk_id"] = "内存块号";
			field["used_size"] = "使用大小";
			field["status_time"] = "修改时间";
			field["bol_id"] = "BOLID";

			for (int i = 0; i < vChunkInfo.size(); i++)
			{
				Json::Value array(Json::objectValue);
				array["chunk_id"] = Bmco::format("%?d", vChunkInfo[i].id);
				array["used_size"] = Bmco::format("%?d", vChunkInfo[i].usedBytes);
				array["status_time"] = stNow;
				array["bol_id"] = 0;
				record[i] = array;
			}

			root["name"] = "c_info_chunk";
			root["desc"] = "chunk信息";
			root["field"] = field;
			root["record"] = record;

			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(chunkInfoPath, oss))
			{
				bmco_error(theLogger, "Faild to write  MetaChunkInfo");
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}
	}

	void CloudScheduleTaskLoop::refreshKpiInfo()
	{
		bmco_debug(theLogger, "refreshKpiInfo()");
		
		MetaKPIInfoOp *tmpKPIPtr = NULL;
		std::vector<MetaKPIInfo> vecKPIInfo;
		tmpKPIPtr = dynamic_cast<MetaKPIInfoOp*>(ctlOper->getObjectPtr(MetaKPIInfoOp::getObjName()));

		if (!tmpKPIPtr->getAllKPIInfo(vecKPIInfo))
		{
			bmco_error(theLogger, "Failed to execute getAllKPIInfo on MetaShmKPIInfoTable");
		}

		std::string cloudKPIStr;
		std::string KPIInfoPath = ZNODE_ZOOKEEPER_STATUS_KPI + "/bol/" + g_sessionData.getBolName();
		
		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(KPIInfoPath))
			{
				if (!CloudDatabaseHandler::instance()->createNode(KPIInfoPath, ZOO_EPHEMERAL))
				{
					bmco_error_f3(theLogger, "%s|%s|Faild to createNode: %s", 
						std::string("0"), std::string(__FUNCTION__), KPIInfoPath);
					return;
				}
			}

			Json::Value root(Json::objectValue);
			Json::Value field(Json::objectValue);
			Json::Value record(Json::arrayValue);

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);

			MetaBolInfo::Ptr ptr = new MetaBolInfo(BOL_NORMAL, "", "");
			MetaBolInfoOp *p = dynamic_cast<MetaBolInfoOp *>(
				   ctlOper->getObjectPtr(MetaBolInfoOp::getObjName()));
			if (!p->Query(ptr))
			{
				bmco_error(theLogger, "Failed to query MetaBolInfo definition.");
				return;
			}

			// 第一行存放本BOL的状态指标
			BolStatus status = ptr->status;
			std::string bolName = g_sessionData.getBolName();
			std::string subsys = g_sessionData.getsetSubSys();
			std::string domainName = g_sessionData.getDomainName();
			
			field["kpi_id"] = "指标编号";
			field["seq_no"] = "bol名称";
			field["bol_name"] = "bol名称";
			field["kpi_value"] = "指标值";
			field["kpi_value2"] = "指标值2";
			field["status_time"] = "变更时间";

			Json::Value array(Json::objectValue);
			array["kpi_id"] = std::string("1001");
			array["seq_no"] = bolName;
			array["bol_name"] = bolName;
			array["kpi_value"] = Bmco::format("%?d", Bmco::UInt32(status));
			array["kpi_value2"] = "";
			array["status_time"] = stNow;
			record[0] = array;

			for (int i = 0; i < vecKPIInfo.size(); i++)
			{
				if (1003 != vecKPIInfo[i].KPI_ID)
				{
					continue;
				}
				
				LocalDateTime dt(vecKPIInfo[i].Create_date);
				std::string stCreateTime = DateTimeFormatter::format(dt, DateTimeFormat::SORTABLE_FORMAT);
		 		dt = vecKPIInfo[i].Modi_date;
	    		std::string stModityTime = DateTimeFormatter::format(dt, DateTimeFormat::SORTABLE_FORMAT);

				Json::Value array(Json::objectValue);
				array["kpi_id"] = Bmco::format("%?d", vecKPIInfo[i].KPI_ID);
				array["seq_no"] = std::string(vecKPIInfo[i].Seq_No.c_str());
				array["bol_name"] = std::string(vecKPIInfo[i].Bol_Cloud_Name.c_str());
				array["kpi_value"] = std::string(vecKPIInfo[i].KPI_Value.c_str());
				array["kpi_value2"] = std::string("");
				array["status_time"] = stModityTime;
				record[i+1] = array;
			}

			root["name"] = "c_info_kpi";
			root["desc"] = "指标信息";
			root["field"] = field;
			root["record"] = record;

			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(KPIInfoPath, oss))
			{
				bmco_error(theLogger, "Faild to write KPIInfo");
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}
	}

	/*void CloudScheduleTaskLoop::doMasterTaskMission()
	{
		// Master的工作
		if (!isMasterCloudAgent())
		{
			return ;
		}

		bmco_information(theLogger, "***do MasterTaskMission over***");
	}

	// 判断当前的cloudagent是否作为Master进行工作
	bool CloudScheduleTaskLoop::isMasterCloudAgent()
	{
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();

		bool isMasterFlag = false;
		std::string masterName;
		std::vector<std::string> nodes;
		std::string nodePath = ZNODE_ZOOKEEPER_SLAVE_DIR;
		std::string curPath = g_sessionData.getBolName();
		std::string bolMemberStr = nodePath + "/" + curPath;
		// 检查当前临时节点是否存在
		if (!CloudDatabaseHandler::instance()->nodeExist(bolMemberStr))
		{
			bmco_information_f1(theLogger, "%s has disappeared!", bolMemberStr);
			if (!CloudDatabaseHandler::instance()->createNode(bolMemberStr, ZOO_EPHEMERAL))
			{
				bmco_error_f1(theLogger, "Faild to createNode bolMemberStr %s", bolMemberStr);
				return false;
			}
		}

		// 获取当前存在的节点
		if (!CloudDatabaseHandler::instance()->GetChildrenNode(nodePath, nodes))
		{
			bmco_information_f2(theLogger, "%s|%s|GetChildrenNode failed, return.",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		std::sort(nodes.begin(), nodes.end());
		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);
		std::vector<struct bolinfo>::iterator itBol;

		for (int i = 0;i < nodes.size();i++)
		{
			for (itBol = sessionBolInfo.begin();
				itBol != sessionBolInfo.end(); itBol++)
			{
				// 按顺序找到第一个运行态的标志位为1的，即为当前的Master
				if (0 == nodes[i].compare(itBol->bol_cloud_name)
					&& ("1" == itBol->master_flag))
				{
					isMasterFlag = true;
					masterName = itBol->bol_cloud_name;
					break;
				}
			}
			if (isMasterFlag)
			{
				break;
			}
		}

		if (masterName.empty())
		{
			bmco_information_f2(theLogger, "%s|%s|no master work, please check it",std::string("0"),std::string(__FUNCTION__));
		}
		else
		{
			bmco_information_f3(theLogger, "%s|%s|master is %s",std::string("0"),std::string(__FUNCTION__), masterName);
		}

		if (!isMasterFlag)
		{
			// bmco_information_f2(theLogger, "%s|%s|Is not global agent, return.",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		// Master选取规则，以存在的节点名称排序，第一个作为Master
		if (0 != Bmco::icompare(curPath, masterName))
		{
			return false;
		}

		// 上报Master的Bol信息到zookeeper
		if (!refreshMasterInfo(curPath))
		{
			return false;
		}

		bmco_information_f2(theLogger, "%s|%s|I am the master",std::string("0"),std::string(__FUNCTION__));
		return true;
	}

	bool CloudScheduleTaskLoop::refreshMasterInfo(std::string MasterBol)
	{
		std::string MasterInfoPath = "/status/master";
		bmco_debug_f1(theLogger, "MasterBol=%s", MasterBol);
		
		if (!CloudDatabaseHandler::instance()->nodeExist(MasterInfoPath))
		{
			bmco_warning_f1(theLogger, "need to create new node: %s", MasterInfoPath);
			if (!CloudDatabaseHandler::instance()->createNode(MasterInfoPath))
			{
				bmco_error_f1(theLogger, "Faild to createNode: %s", MasterInfoPath);
				return false;
			}
		}

		if (!CloudDatabaseHandler::instance()->setNodeData(MasterInfoPath, MasterBol))
		{
			return false;
		}

		return true;
	}

	// 检查zookeeper上本bol的临时节点的存在
	bool CloudScheduleTaskLoop::checkZookeeperEphemeralZode()
	{
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string nodePath = ZNODE_ZOOKEEPER_SLAVE_DIR;
		std::string curPath = g_sessionData.getBolName();
		std::string bolMemberStr = nodePath + "/" + curPath;
		// 检查当前临时节点是否存在
		if (!CloudDatabaseHandler::instance()->nodeExist(bolMemberStr))
		{
			bmco_warning_f1(theLogger, "%s has disappeared!", bolMemberStr);
			if (!CloudDatabaseHandler::instance()->createNode(bolMemberStr, ZOO_EPHEMERAL))
			{
				bmco_error_f1(theLogger, "Faild to createNode bolMemberStr %s", bolMemberStr);
				bmco_error_f3(theLogger, "%s|%s|zookeeperZode %s is dead", 
					std::string("0"), std::string(__FUNCTION__), bolMemberStr);
				return false;
			}
		}

		return true;
	}*/
}


