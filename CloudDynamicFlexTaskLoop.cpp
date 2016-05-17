#include <time.h>
#include <map>
#include <iostream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include "CloudDynamicFlexTaskLoop.h"
#include "CheckPoint.h"
#include <boost/typeof/typeof.hpp>
#include <boost/lexical_cast.hpp>
#include "json/json.h"

namespace BM35
{
	// 获取bolname，域名信息
	/*void CloudDynamicFlexTaskLoop::recordHostBaseInfo()
	{
		bmco_debug(theLogger, "recordHostBaseInfo()");
		
		std::string bolName;
		//std::string domainName;
		
		MetaBolConfigOp* c = dynamic_cast<MetaBolConfigOp*>(ctlOper->getObjectPtr(MetaBolConfigOp::getObjName()));
		MetaBolConfigInfo::Ptr cfgpayload(new MetaBolConfigInfo("info.bol_name"));
		if (c->Query(cfgpayload)) 
		{
			bolName = cfgpayload->_strval.c_str();
		}
		else
		{
			return;
		}

		m_sessionContent.sessionBolName = bolName;
		bmco_information_f3(theLogger, "%s|%s|bolName=%s",
			std::string("0"),std::string(__FUNCTION__), 
			m_sessionContent.sessionBolName);
		
	}*/

	// 获取/status/kpi 下面所有bol的指标信息
	void CloudDynamicFlexTaskLoop::recordParentKPIPath()
	{
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string bolName = g_sessionData.getBolName();
		// std::string domainName = m_sessionContent.sessionDomainName.c_str();
		std::string KPIInfoPath = ZNODE_ZOOKEEPER_STATUS_KPI + "/bol";
		
		if (!CloudDatabaseHandler::instance()->nodeExist(KPIInfoPath))
		{
			return;
		}

		std::vector<std::string> childrenVec;
		childrenVec.clear();
		// 获取当前bol的指标信息，如果有指标，应该是多个BOL目录
		if (!CloudDatabaseHandler::instance()->GetChildrenNode(KPIInfoPath, childrenVec))
		{
			bmco_error_f3(theLogger, "%s|%s|GetChildrenNode [%s] failed", std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
			return;
		}

		// 没有指标
		if (0 == childrenVec.size())
		{
			return;
		}

		std::vector<std::string>::iterator it;
		std::string cmdString;
		std::vector<struct kpiinfo> tmpVec;
		// m_sessionContent.sessionKpiInfo.clear();
		// 收集的是各个bolname
		for (it = childrenVec.begin();it != childrenVec.end();it++)
		{
			recordKPIInfo(*it, tmpVec);
		}
		g_sessionData.setKpiInfo(tmpVec);

		bmco_debug(theLogger, "recordParentKPIPath successfully");
	}
	
	// 获取记录某个BOL的指标信息
	void CloudDynamicFlexTaskLoop::recordKPIInfo(std::string bolName, 
		std::vector<struct kpiinfo> &tmpVec)
	{
		bmco_debug(theLogger, "recordKPIInfo()");

		std::string KPIInfoPath = "/status/kpi/bol/" + bolName;
		std::string cloudConfigStr;

		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(KPIInfoPath))
			{
				bmco_error_f3(theLogger, "%s|%s|%s is disappeared when download!", 
					std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
				return;
			}
			
			bool isChanged = false;
			if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(KPIInfoPath, isChanged))
			{
				bmco_error_f3(theLogger, "%s|%s|%s is getNodeChangedFlag failed!", 
					std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
				return;
			}

			// 节点内容没有变化不处理
			if (!isChanged)
			{
				return;
			}

			Json::Value root;
			Json::Reader jsonReader;
			
			if (0 != CloudDatabaseHandler::instance()->getNodeData(KPIInfoPath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
				return;
			}

			if (cloudConfigStr.empty())
			{
				bmco_information_f3(theLogger, "%s|%s|no data in %s", std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
				return;
			}

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("kpi_id")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no kpi_id",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				if (!value.isMember("bol_name")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no bol_name",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				if (!value.isMember("kpi_value")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no kpi_value",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				
				struct kpiinfo tmpKpiInfo;
				tmpKpiInfo.kpi_id = value["kpi_id"].asString();
				tmpKpiInfo.bol_cloud_name = value["seq_no"].asString();
				tmpKpiInfo.bol_cloud_name = value["bol_name"].asString();
				tmpKpiInfo.kpi_value = value["kpi_value"].asString();
				tmpVec.push_back(tmpKpiInfo);
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
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordKPIInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}

		bmco_information_f1(theLogger, "recordKPIInfo [%s] successfully", KPIInfoPath);
	}
	
	// 获取记录bol配置信息
	// 包括bolid bolname,ipaddr,user_pwd_id,nideid,auto_flex_flag
	void CloudDynamicFlexTaskLoop::recordBOLInfo()
	{
		bmco_debug(theLogger, "recordBOLInfo()");
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string NodeInfoPath = ZNODE_ZOOKEEPER_CLUSTER + "/node";
		std::string BolInfoPath = ZNODE_ZOOKEEPER_CLUSTER + "/bolcfg";
		std::string cloudNodeConfigStr;
		std::string cloudBolConfigStr;

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(NodeInfoPath, cloudNodeConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), NodeInfoPath);
				return;
			}
			if (cloudNodeConfigStr.empty())
			{
				bmco_error_f3(theLogger, "%s|%s|no data in %s", std::string("0"),std::string(__FUNCTION__), NodeInfoPath);
				return;
			}

			if (0 != CloudDatabaseHandler::instance()->getNodeData(BolInfoPath, cloudBolConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), BolInfoPath);
				return;
			}
			if (cloudBolConfigStr.empty())
			{
				bmco_error_f3(theLogger, "%s|%s|no data in %s", std::string("0"),std::string(__FUNCTION__), BolInfoPath);
				return;
			}

			Json::Value pNodeRootNode;
			Json::Value pBolRootNode;
			Json::Reader jsonNodeReader;
			Json::Reader jsonBolReader;
			
			if (!jsonNodeReader.parse(cloudNodeConfigStr, pNodeRootNode)) 
			{
				return;
			}

			if (!jsonBolReader.parse(cloudBolConfigStr, pBolRootNode)) 
			{
				return;
			}

			std::string bolName = g_sessionData.getBolName();

			if (!pNodeRootNode.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return;
			}
			if (!pBolRootNode.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return;
			}

			Json::Value valueNodeArray = pNodeRootNode["record"];
			Json::Value valueBolArray = pBolRootNode["record"];

			struct bolinfo tmpBolInfo;
			std::vector<struct bolinfo> tmpVec;

			for (int indexNode = 0; indexNode < valueNodeArray.size(); ++indexNode) 
			{
				Json::Value valueNode = valueNodeArray[indexNode];
				if (!valueNode.isMember("node_id")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no node_id",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				if (!valueNode.isMember("node_name")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no node_name",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				if (!valueNode.isMember("hostname")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no hostname",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				if (!valueNode.isMember("ip_addr")) 
				{
					bmco_error_f2(theLogger, "%s|%s|no ip_addr",std::string("0"),std::string(__FUNCTION__));
					return;
				}
				for (int indexBol = 0; indexBol < valueBolArray.size(); ++indexBol) 
				{
					Json::Value valueBol = valueBolArray[indexBol];
					if (!valueBol.isMember("node_id")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no node_id",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("bol_id")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no bol_id",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("bol_name")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no bol_name",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("bol_dir")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no bol_dir",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("auto_flex_flag")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no auto_flex_flag",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("master_flag")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no master_flag",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("subsys_type")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no subsys_type",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("domain_type")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no domain_type",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (!valueBol.isMember("user_name")) 
					{
						bmco_error_f2(theLogger, "%s|%s|no user_name",std::string("0"),std::string(__FUNCTION__));
						return;
					}
					if (valueNode["node_id"].asString() 
						== valueBol["node_id"].asString())
					{
						if (0 == bolName.compare(valueBol["bol_name"].asString()))
						{
							g_sessionData.setSubSys(valueBol["subsys_type"].asString());
							g_sessionData.setDomainName(valueBol["domain_type"].asString());
						}
						tmpBolInfo.bol_id = valueBol["bol_id"].asString();
						tmpBolInfo.bol_cloud_name = valueBol["bol_name"].asString();
						tmpBolInfo.nedir = valueBol["bol_dir"].asString();
						tmpBolInfo.node_id = valueNode["node_id"].asString();
						tmpBolInfo.node_name = valueNode["node_name"].asString();
						tmpBolInfo.hostname = valueNode["hostname"].asString();
						tmpBolInfo.ip_addr = valueNode["ip_addr"].asString();
						tmpBolInfo.auto_flex_flag = valueBol["auto_flex_flag"].asString();
						tmpBolInfo.master_flag = valueBol["master_flag"].asString();
						tmpBolInfo.subsys = valueBol["subsys_type"].asString();
						tmpBolInfo.domainName = valueBol["domain_type"].asString();
						tmpBolInfo.userName = valueBol["user_name"].asString();
						tmpVec.push_back(tmpBolInfo);
					}
				}
			}
			g_sessionData.setBolInfo(tmpVec);
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
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordBOLInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}

		bmco_information(theLogger, "recordBOLInfo successfully");
	}

	void CloudDynamicFlexTaskLoop::recordFlexInfo()
	{
		// 在zookeeper上获取规则表信息
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string nodePath(ZNODE_ZOOKEEPER_FlEX);
		std::string cloudConfigStr;

		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_information_f3(theLogger, "%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
				nodePath);
			return;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), nodePath);
				return;
			}
			if (cloudConfigStr.empty())
			{
				bmco_information_f3(theLogger, "%s|%s|no data in %s", std::string("0"),std::string(__FUNCTION__), nodePath);
				return;
			}

			Json::Value root;
			Json::Reader jsonReader;

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return;
			}

			Json::Value valueArray = root["record"];
			std::vector<struct flexinfo> tmpVec;
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				
				struct flexinfo tmpFlexInfo;
				tmpFlexInfo.rule_id = value["rule_id"].asString();
				tmpFlexInfo.subsys_type = value["subsys_type"].asString();
				tmpFlexInfo.domain_type = value["domain_type"].asString();
				tmpFlexInfo.rule_name = value["rule_name"].asString();
				tmpFlexInfo.rule_desc = value["rule_desc"].asString();
				tmpFlexInfo.rule_type = value["rule_type"].asString();
				tmpFlexInfo.kpi_name_str = value["kpi_name_str"].asString();
				tmpFlexInfo.value_str = value["value_str"].asString();
				tmpFlexInfo.status = value["status"].asString();
				tmpFlexInfo.create_date = value["create_date"].asString();
				tmpFlexInfo.modi_date = value["modi_date"].asString();
				tmpVec.push_back(tmpFlexInfo);
			}
			g_sessionData.setFlexInfo(tmpVec);
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
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordFlexInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}

		bmco_information(theLogger, "recordFlexInfo successfully");
	}

	void CloudDynamicFlexTaskLoop::recordDictInfo()
	{
		// 在zookeeper上获取规则表信息
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string nodePath(ZNODE_ZOOKEEPER_DICT);
		std::string cloudConfigStr;

		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_information_f3(theLogger, "%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			nodePath);
			return;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), nodePath);
				return;
			}

			if (cloudConfigStr.empty())
			{
				bmco_information_f3(theLogger, "%s|%s|no data in %s", std::string("0"),std::string(__FUNCTION__), nodePath);
				return;
			}

			Json::Value root;
			Json::Reader jsonReader;

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return;
			}

			Json::Value valueArray = root["record"];
			std::vector<struct dictinfo> tmpVec;

			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];

				struct dictinfo tmpDictInfo;
				tmpDictInfo.id = value["id"].asString();
				tmpDictInfo.type = value["type"].asString();
				tmpDictInfo.key = value["key"].asString();
				tmpDictInfo.value = value["value"].asString();
				tmpDictInfo.desc = value["desc"].asString();
				tmpDictInfo.begin_date = value["begin_date"].asString();
				tmpDictInfo.end_date = value["end_date"].asString();
				tmpVec.push_back(tmpDictInfo);
			}
			g_sessionData.setDictInfo(tmpVec);
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
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordDictInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}

		bmco_information(theLogger, "recordDictInfo successfully");
	}

	/*void CloudDynamicFlexTaskLoop::recordMQDefInfo()
	{
		// 在zookeeper上获取消息队列定义表信息
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::string nodePath(ZNODE_ZOOKEEPER_MQ_MQ);
		std::string cloudConfigStr;

		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_information_f3(theLogger, "%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			nodePath);
			return;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), nodePath);
				return;
			}
			
			std::istringstream istr(cloudConfigStr);
			Bmco::AutoPtr<IniFileConfigurationNew> pConf = new IniFileConfigurationNew(istr);

			int nodeNum = pConf->getInt(TABLE_PREFIX_LIST[1] + ".recordnum");
			
			for (int i = 1; i <= nodeNum; i++)
			{
				std::string ruleCheckStr = pConf->getString(TABLE_PREFIX_LIST[1] + ".record." + Bmco::NumberFormatter::format(i));
				bmco_information_f3(theLogger, "%s|%s|ruleCheckStr: %s", 
					std::string("0"),std::string(__FUNCTION__),ruleCheckStr);
				Bmco::StringTokenizer tokensRuleStr(ruleCheckStr, "|", Bmco::StringTokenizer::TOK_TRIM);
				Bmco::StringTokenizer tokensDefStr(tokensRuleStr[3], ",", Bmco::StringTokenizer::TOK_TRIM);
				m_brokerlist = tokensDefStr[0];
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordMQDefInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}
		
		m_zookeeperlist = config.getString("zkConf.zookeeper.host");
		bmco_information(theLogger, "recordMQDefInfo successfully");
	}*/

	bool CloudDynamicFlexTaskLoop::recordTopicInfo(std::string currTopicName, 
		std::string &domainName)
	{
		MetaMQTopicOp *tmpTopicPtr = NULL;
		tmpTopicPtr = dynamic_cast<MetaMQTopicOp*>(ctlOper->getObjectPtr(MetaMQTopicOp::getObjName()));
		std::vector<MetaMQTopic> vecTopicInfo;
		Bmco::UInt32 partitionNum = 0;
		std::string topicName;
		if (!tmpTopicPtr->getAllMQTopic(vecTopicInfo))
		{
			bmco_error_f2(theLogger, "%s|%s|Failed to execute getAllMQTopic on MetaShmTopicInfoTable",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		m_topicInfoMap.clear();
		std::vector<MetaMQTopic>::iterator it;
		std::map<std::string, Bmco::UInt32>::iterator itMap;
		for (it = vecTopicInfo.begin();it != vecTopicInfo.end();it++)
		{
			topicName = it->Topic_name.c_str();
			partitionNum = it->Partition_number;
			if (0 == currTopicName.compare(topicName))
			{
				domainName = it->domain_type.c_str();
			}
			itMap = m_topicInfoMap.find(topicName);
			if (itMap == m_topicInfoMap.end())
			{
				m_topicInfoMap.insert(std::pair<std::string, Bmco::UInt32>(topicName, partitionNum));
			}
			else
			{
				m_topicInfoMap[topicName] = partitionNum;
			}
		}
		
		return true;
	}

	bool CloudDynamicFlexTaskLoop::recordRelaInfo()
	{
		MetaMQRelaOp *tmpRelaPtr = NULL;
		std::vector<MetaMQRela> vecRelaInfo;
		tmpRelaPtr = dynamic_cast<MetaMQRelaOp*>(ctlOper->getObjectPtr(MetaMQRelaOp::getObjName()));
		m_relaInfoVec.clear();

		// 获取消息队列关系信息
		if (!tmpRelaPtr->getAllMQRela(m_relaInfoVec))
		{
			bmco_error_f2(theLogger, "%s|%s|Failed to execute getAllMQRela on MetaShmRelaInfoTable",std::string("0"),std::string(__FUNCTION__));
			return false;
		}
		
		return true;
	}

	// 远程启动bol，一定要将新启动的bol的云代理同时启动
	bool CloudDynamicFlexTaskLoop::launchBolNode(const std::string bolName)
	{
		char cBuf[1024];
		memset(cBuf, 0, sizeof(cBuf));	
		Bmco::Int32 ret = -999;	
		std::string des_ipaddr;
		std::string des_dir;
		std::string des_user;

		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);
		std::vector<struct bolinfo>::iterator it;
		for (it = sessionBolInfo.begin();
			it != sessionBolInfo.end();it++)
		{
			if (0 == bolName.compare(it->bol_cloud_name))
			{
				des_ipaddr = it->ip_addr;
				des_dir = it->nedir;
				des_user = it->userName;
			}
		}

		try
        {
            HTTPClientSession cs(des_ipaddr.c_str(), 8040);

			Json::Value head(Json::objectValue);
			Json::Value content(Json::objectValue);
			Json::Value rootReq(Json::objectValue);

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);

			head["processcode"] = "doprocess";
			head["reqseq"] = "012014120101";
			head["reqtime"] = stNow;

			content["username"] = des_user;
			content["path"] = des_dir;
			content["cmd"] = "bolstart.sh";

			rootReq["head"] = head;
			rootReq["content"] = content;

			Json::FastWriter jfw;
			std::string oss = jfw.write(rootReq);
			
            //HTTPRequest request("POST", "/echoBody");
            HTTPRequest request;
            request.setContentLength((int) oss.length());
            request.setContentType("text/plain");
            cs.sendRequest(request) << oss;
            HTTPResponse response;
            std::string rbody;
            cs.receiveResponse(response) >> rbody;

			Json::Value rootRsp;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(rbody, rootRsp)) 
			{
				bmco_error_f2(theLogger, "%s|%s|Failed to execute jsonReader",std::string("0"),std::string(__FUNCTION__));
				return false;
			}
			if (!rootRsp.isMember("content")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no content",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			Json::Value value = rootRsp["content"];
			std::string cmd_result = value["cmd_result"].asString();
			std::string respcode = value["respcode"].asString();
			
			if (respcode != "0")
			{
				bmco_error_f2(theLogger, "%s|%s|respcode not success",std::string("0"),std::string(__FUNCTION__));
				return false;
			}
        }
        catch(Bmco::Exception &e)
        {
            return false;
        }
		
		return true;
	}
	
	// 判断当前的cloudagent是否作为Master进行工作
	bool CloudDynamicFlexTaskLoop::isMasterCloudAgent()
	{
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
		std::vector<struct bolinfo>::iterator itBol;
		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);

		for (int i = 0;i < nodes.size();i++)
		{
			for (itBol = sessionBolInfo.begin();
				itBol != sessionBolInfo.end(); itBol++)
			{
				std::string ccBol = itBol->bol_cloud_name;
				std::string ccflag = itBol->master_flag;
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
			bmco_warning_f2(theLogger, "%s|%s|no master work, please check it",std::string("0"),std::string(__FUNCTION__));
		}
		// else
		// {
		//     bmco_information_f3(theLogger, "%s|%s|master is %s",std::string("0"),std::string(__FUNCTION__), masterName);
		// }

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

	bool CloudDynamicFlexTaskLoop::refreshMasterInfo(std::string MasterBol)
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

		Json::Value root(Json::objectValue);
		Json::Value field(Json::objectValue);
		Json::Value record(Json::objectValue);

		field["bol_name"] = "bolname";
		record["bol_name"] = MasterBol;

		root["name"] = "c_info_master";
		root["desc"] = "Master信息";
		root["field"] = field;
		root["record"] = record;

		Json::FastWriter jfw;
		std::string oss = jfw.write(root);

		if (!CloudDatabaseHandler::instance()->setNodeData(MasterInfoPath, oss))
		{
			return false;
		}

		return true;
	}

	// 除了本bol是否还有其他的bol处理此topic
	bool CloudDynamicFlexTaskLoop::isTheOnlyBolForTheTopic(std::string topicName,
			std::string bolName)
	{
		bmco_debug(theLogger, "isTheOnlyBolForTheTopic()");
		std::string tmpBolName;
		std::string tmpTopicName;
		std::vector<MetaMQRela>::iterator itRela;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			tmpBolName = itRela->BOL_CLOUD_NAME.c_str();
			tmpTopicName = itRela->IN_TOPIC_NAME.c_str();
			if (0 == topicName.compare(tmpTopicName))
			{
				if (0 != bolName.compare(tmpBolName))
				{
					bmco_information_f3(theLogger, 
						"%s|%s|is Not TheOnlyBolForTheTopic include %s", 
						std::string("0"),std::string(__FUNCTION__),tmpBolName);
					return false;
				}
			}
		}

		bmco_information_f3(theLogger, 
				"%s|%s|%s isTheOnlyBolForTheTopic", 
				std::string("0"),std::string(__FUNCTION__),bolName);
		return true;
	}

	bool CloudDynamicFlexTaskLoop::chooseSuitAbleBolForLaunch
		(std::string domainName, std::string &launchBol)
	{	
		bmco_debug(theLogger, "chooseSuitAbleBolForLaunch()");
		bmco_information_f1(theLogger, "domainName = %s", domainName);
		
		std::string bolName;
		std::string curDomainName;
		bool hasMaintainsBol = false;
		std::vector<struct kpiinfo> sessionKpiInfo;
		g_sessionData.getKpiInfo(sessionKpiInfo);
		std::vector<struct kpiinfo>::iterator itKpi;
		for (itKpi = sessionKpiInfo.begin();
				itKpi != sessionKpiInfo.end();itKpi++)
		{
			if ("1001" == itKpi->kpi_id
				&& "1" == itKpi->kpi_value)
				//&& (0 == domainName.compare(itKpi->domian_type)
				//|| (0 == itKpi->domian_type.compare("flex"))))
			{
				// 没有KPI上报的BOL,继续查找
				if (!g_sessionData.getBolDomain(itKpi->bol_cloud_name, curDomainName))
				{
					continue;
				}
				// 对应域或者动态伸缩域的可以支持
				if ((0 == curDomainName.compare(domainName))
					|| curDomainName.compare("flex"))
				{
					bolName = itKpi->bol_cloud_name;
					hasMaintainsBol = true;
					break;
				}
			}
		}

		// 如果存在维保态的bol，先用维保态的bol
		if (hasMaintainsBol)
		{
			launchBol = bolName;
			bmco_information_f3(theLogger, "%s|%s|launchBolNode %s successfully", 
				std::string("0"), std::string(__FUNCTION__), bolName);
			return true;
		}
		
		std::vector<std::string> nodes;
		std::string nodePath = ZNODE_ZOOKEEPER_SLAVE_DIR;

		// 获取当前存在的节点
		if (!CloudDatabaseHandler::instance()->GetChildrenNode(nodePath, nodes))
		{
			bmco_information_f2(theLogger, "%s|%s|GetChildrenNode failed, return.",std::string("0"),std::string(__FUNCTION__));
			return false;
		}
		
		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);

		std::vector<struct bolinfo>::iterator itSession;
		std::vector<std::string>::iterator itNode;
		for (itSession = sessionBolInfo.begin();
				itSession != sessionBolInfo.end();itSession++)
		{
			// 非动态伸缩的bol
			if (0 != itSession->auto_flex_flag.compare("1"))
			{
				continue;
			}

			// 不是可伸缩域的bol
			if (0 != itSession->domainName.compare(domainName)
				&& (0 != itSession->domainName.compare("flex")))
			{
				continue;
			}

			bmco_information_f3(theLogger, "%s|%s|%s accord with the conditions", 
					std::string("0"), std::string(__FUNCTION__), itSession->bol_cloud_name);

			itNode = find(nodes.begin(), nodes.end(), itSession->bol_cloud_name);
			// 目前没有启动的可伸缩bol
			if (itNode == nodes.end())
			{
				bolName = itSession->bol_cloud_name;
				bmco_information_f3(theLogger, "%s|%s|try to launchBolNode %s", 
						std::string("0"), std::string(__FUNCTION__), bolName);
				if (launchBolNode(bolName))
				{
					launchBol = bolName;
					bmco_information_f3(theLogger, "%s|%s|launchBolNode %s successfully", 
						std::string("0"), std::string(__FUNCTION__), bolName);
					return true;
				}
			}
		}
				
		bmco_error_f2(theLogger, "%s|%s|launchBolNode failed",std::string("0"),std::string(__FUNCTION__));
		return false;
	}

	// 判断是否符合n+1或者n-1的规则
	void CloudDynamicFlexTaskLoop::doRuleCheckDynamicFlex()
	{
		// Master的工作
		//if (!isMasterCloudAgent())
		//{
		//	return ;
		//}

		if (!recordRelaInfo())
		{
			return;
		}

		// 获取距离上次获取指标的时间间隔
		Bmco::Timespan intervalTime;
		getIntervalTime(intervalTime);

		// 收集各个主题的消息堆积数
		std::map<std::string, Bmco::Int64> topicMessageNumMap;
		std::map<std::string, mqCalcOffsetData> TopicUpDownOffsetVec;
		if (!accumulateMessageNum(intervalTime, topicMessageNumMap, TopicUpDownOffsetVec))
		{
			return ;
		}
		
		// 刷新本次获取结束时间
		setLastTimeStamp();

		// 打印各个主题堆积的消息数
		std::map<std::string, Bmco::Int64>::iterator itTopicMap;
		for (itTopicMap = topicMessageNumMap.begin();
			itTopicMap != topicMessageNumMap.end();itTopicMap++)
		{
			bmco_information_f4(theLogger, "%s|%s|topic %s messageNum %?d\n",
			std::string("0"), std::string(__FUNCTION__), 
			itTopicMap->first, itTopicMap->second);
		}

		refreshKPIInfoByMaster(topicMessageNumMap, TopicUpDownOffsetVec);

		if (0 == topicMessageNumMap.size())
		{
			bmco_information_f2(theLogger, "%s|%s|all topic are empty\n",
				std::string("0"), std::string(__FUNCTION__));
			return;
		}

		std::string kpi_name;
		std::string kpi_value;
		std::string topicName;
		std::vector<struct flexinfo> sessionFlexInfo;
		g_sessionData.getFlexInfo(sessionFlexInfo);
		std::map<std::string, Bmco::Int64>::iterator it;
		// 根据配置的规则表进行判断是否有需要动态伸缩及宕机的处理
		std::vector<struct flexinfo>::iterator itFlex;
		for (itFlex = sessionFlexInfo.begin();
			itFlex != sessionFlexInfo.end();itFlex++)
		{
			// 配置规则表无效
			if (Bmco::NumberParser::parse(itFlex->status) == 0)
			{
				bmco_information_f1(theLogger, "flex %s status is invalid", 
					itFlex->rule_name);
				continue;
			}
			
			Bmco::StringTokenizer tokensKpiName(itFlex->kpi_name_str, "~", Bmco::StringTokenizer::TOK_TRIM);
			Bmco::StringTokenizer tokensKpiValue(itFlex->value_str, "~", Bmco::StringTokenizer::TOK_TRIM);
			bmco_information_f3(theLogger, "%s|%s|%s", 
				itFlex->rule_type, itFlex->kpi_name_str, itFlex->value_str);
			bmco_information_f4(theLogger, "topic:%s ruleid:%s interval:%s kpivalue:%s", tokensKpiName[0],
					tokensKpiName[1], tokensKpiValue[0], tokensKpiValue[1]);

			// n+1逻辑
			if (Bmco::NumberParser::parse(itFlex->rule_type) == 1)
			{
				doExtendOperationOrNot(tokensKpiName[0],
					tokensKpiName[1], tokensKpiValue[0], tokensKpiValue[1], 
					topicMessageNumMap);
			}

			// n-1逻辑
			else if (Bmco::NumberParser::parse(itFlex->rule_type) == 2)
			{
				doShrinkOperationOrNot(tokensKpiName[0],
					tokensKpiName[1], tokensKpiValue[0], tokensKpiValue[1], 
					topicMessageNumMap);
			}

			// 宕机处理逻辑
			else if (Bmco::NumberParser::parse(itFlex->rule_type) == 3)
			{
				doBrokenDownBolOperation();
			}
		}
	}
	// n+1逻辑处理
	// 监视的主题名称topicName 监视的类型代号code
	// 监视的时间间隔interval 监视的阀值value
	// 主题与堆积消息数的映射topicMessageNumMap
	void CloudDynamicFlexTaskLoop::doExtendOperationOrNot
		(std::string topicName, std::string code, 
		 std::string interval, std::string value,
		 std::map<std::string, Bmco::Int64> topicMessageNumMap)
	{
		bmco_information_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::doExtendOperationOrNot", std::string("0"),std::string(__FUNCTION__));
		Bmco::Timestamp now;
		Bmco::Int64 monitorInteval = 0;
		std::map<std::string, Bmco::Int64>::iterator it;
		// 根据消息堆积数进行判断
		if ("1002" == code)
		{
			bmco_information_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::has 1002", std::string("0"),std::string(__FUNCTION__));
			// 监视间隔
			// monitorInteval = Bmco::NumberParser::parse(interval)*60*60*1000000;
			// test
			monitorInteval = Bmco::NumberParser::parse(interval)*1000000;

			for (it = topicMessageNumMap.begin();it != topicMessageNumMap.end();it++)
			{
				// 需要监视的topic
				if (0 != topicName.compare(it->first))
				{
					continue;
				}

				// 该主题的消息堆积消息数小于阀值，刷新记录时间
				if (it->second < Bmco::NumberParser::parse(value))
				{
					m_exLatestMonitorTime.update();
					m_needExtendFlex = false;
					bmco_information_f3(theLogger, "%s|%s|topic %s m_exLatestMonitorTime.update()", 
						std::string("0"), std::string(__FUNCTION__), topicName);
				}
				// 该主题的消息堆积消息数大于阀值，不过在监控时间内，
				// 如果有小于的情况，还可以改掉
				else
				{
					// 新的监控周期第一次超过阀值，记录时刻
					if (!m_needExtendFlex)
					{
						m_exLatestMonitorTime.update();
						m_needExtendFlex = true;
					}
				}
				break;
			}
			// 在监控的时间范围内，还不能确定是否要伸缩
			if (m_exLatestMonitorTime + monitorInteval > now)
			{
				bmco_information_f3(theLogger, "%s|%s|topic %s is being monitored!", 
						std::string("0"), std::string(__FUNCTION__), topicName);
			}
			// 已经超出范围，可以进行判断是否要n+1
			else
			{
				// 需要n+1的情况
				if (m_needExtendFlex)
				{
					bmco_information_f3(theLogger, "%s|%s|topic %s need extend", 
						std::string("0"), std::string(__FUNCTION__), topicName);
					if (!extendPartitionAndBolFlow(topicName))
					{
						bmco_information_f3(theLogger, 
							"%s|%s|extend topic %s failed", 
							std::string("0"), std::string(__FUNCTION__), 
							topicName);
					}
					else
					{
						bmco_information_f3(theLogger, 
							"%s|%s|extend topic %s succesfully", 
							std::string("0"), std::string(__FUNCTION__),topicName);
					}
					m_needExtendFlex = false;
					// test
					m_testControl = false;
				}
				m_exLatestMonitorTime.update();
			}
		}
	}

	// n-1逻辑处理
	void CloudDynamicFlexTaskLoop::doShrinkOperationOrNot
		(std::string topicName, std::string code, 
		 std::string interval, std::string value,
		 std::map<std::string, Bmco::Int64> topicMessageNumMap)
	{
		Bmco::Timestamp now;
		Bmco::Int64 monitorInteval = 0;
		std::map<std::string, Bmco::Int64>::iterator it;
		// 根据消息堆积数进行判断
		if ("1002" == code)
		{
			// 监视间隔
			monitorInteval = Bmco::NumberParser::parse(interval)*60*60*1000000;

			for (it = topicMessageNumMap.begin();it != topicMessageNumMap.end();it++)
			{
				// 需要监视的topic
				if (0 != topicName.compare(it->first))
				{
					continue;
				}

				// 该主题的消息堆积消息数大于阀值，刷新记录时间
				if (it->second > Bmco::NumberParser::parse(value))
				{
					m_shLatestMonitorTime.update();
					m_needShrinkFlex = false;
					bmco_information_f3(theLogger, "%s|%s|topic %s m_shLatestMonitorTime.update()", 
						std::string("0"), std::string(__FUNCTION__), topicName);
				}
				// 该主题的消息堆积消息数大于阀值，不过在监控时间内，
				// 如果有大于的情况，还可以改掉
				else
				{
					if (!m_needShrinkFlex)
					{
						m_shLatestMonitorTime.update();
						m_needShrinkFlex = true;
					}
				}
				break;
			}
			// 在监控的时间范围内，还不能确定是否要伸缩
			if (m_shLatestMonitorTime + monitorInteval > now)
			{
				bmco_information_f3(theLogger, "%s|%s|topic %s is being monitored if shrink or not", 
					std::string("0"), std::string(__FUNCTION__), topicName);
			}
			// 已经超出范围，可以进行判断是否要n-1
			else
			{
				// 需要n-1的情况
				if (m_needShrinkFlex)
				{
					bmco_information_f3(theLogger, "%s|%s|topic %s need shrink", 
						std::string("0"), std::string(__FUNCTION__), topicName);
					if (!shrinkPartitionAndBolFlow(topicName))
					{
						bmco_information_f3(theLogger, 
							"%s|%s|shrink topic %s failed", std::string("0"), std::string(__FUNCTION__), topicName);
					}
					else
					{
						bmco_information_f3(theLogger, 
							"%s|%s|shrink topic %s succesfully", std::string("0"), std::string(__FUNCTION__), topicName);
					}
					m_needShrinkFlex = false;
				}
				m_shLatestMonitorTime.update();
			}
		}
	}

	bool CloudDynamicFlexTaskLoop::getFirstStartInstanceId(std::string processName, 
				Bmco::UInt32 &instanceId)
	{
		instanceId = 0;
		MetaProgramDefOp *proPtr = dynamic_cast<MetaProgramDefOp *>(ctlOper->getObjectPtr(MetaProgramDefOp::getObjName()));
		std::vector<MetaProgramDef> proVec;
		if (!proPtr->queryAll(proVec))
		{
			bmco_error_f2(theLogger, "%s|%s|get queryAll failed!", std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		bmco_information(theLogger, "firstStep");

		std::string exeName;
		Bmco::UInt32 proId = 0;
		std::vector<MetaProgramDef>::iterator itPro;
		for (itPro = proVec.begin();itPro != proVec.end();itPro++)
		{	
			exeName = itPro->ExeName.c_str();
			if (0 == exeName.compare(processName))
			{
				proId = itPro->ID;
				break;
			}
		}
		
		if (0 == proId)
		{
			bmco_error_f3(theLogger, "%s|%s|no program named %s", 
				std::string("0"),std::string(__FUNCTION__), processName);
		}

		bmco_information(theLogger, "secondStep");
		
		MetaBpcbInfoOp *bpcbPtr = dynamic_cast<MetaBpcbInfoOp *>(ctlOper->getObjectPtr(MetaBpcbInfoOp::getObjName()));
		std::vector<MetaBpcbInfo> bpcbVec;
		if (!bpcbPtr->getBpcbInfoByProgramID(proId, bpcbVec))
		{
			bmco_error_f2(theLogger, "%s|%s|get getBpcbInfoByProgramID failed!", std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		instanceId = bpcbVec[0].m_iInstanceID;
		bmco_information_f3(theLogger, "%s|%s|start with instanceId %?d", 
			std::string("0"),std::string(__FUNCTION__), instanceId);
		return true;
	}

	bool CloudDynamicFlexTaskLoop::extendPartitionAndBolFlow(std::string topicName)
	{
		bmco_information(theLogger, "extendPartitionAndBolFlow()");

		std::string domainName;

		if (!recordTopicInfo(topicName, domainName))
		{
			return false;
		}

		/*std::set<std::string> relaSet;
		std::vector<MetaMQRela>::iterator itRela;
		for (itRela = vecRelaInfo.begin();itRela != vecRelaInfo.end();itRela++)
		{
			if (0 == topicName.compare(itRela->IN_TOPIC_NAME.c_str()))
			{
				relaSet.insert(std::string(itRela->PROCESS_NAME.c_str()));
			}
		}*/

		// 启动Bol,通过启动bol的规则，得到bol的名称launchBol
		std::string launchBol;
		if (!chooseSuitAbleBolForLaunch(domainName, launchBol))
		{
			return false;
		}
		// test
		// std::string launchBol = "bol_89";

		// 建立新启动的bol的进程实例与上下游的topic的对应关系
		std::vector<MetaMQRela>::iterator itRela;
		relaInfo relainfo;
		std::vector<relaInfo> relaVec;
		Bmco::UInt32 inPartitionNum = 0;
		Bmco::UInt32 outPartitionNum = 0;
		Bmco::UInt32 startInstanceId = 0;
		std::string loggerStr;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			// 选择其中一条关系记录
			if (0 == topicName.compare(itRela->IN_TOPIC_NAME.c_str()))
			{
				relainfo.flowId = itRela->Flow_ID.c_str();
				relainfo.processName = itRela->PROCESS_NAME.c_str();
				// 新启动的bol，实例号为0
				relainfo.bolName = launchBol;
				if (!getFirstStartInstanceId(relainfo.processName, startInstanceId))
				{
					return false;
				}
				// relainfo.instanceId = "0";
				relainfo.instanceId = startInstanceId;
				relainfo.inTopicName = itRela->IN_TOPIC_NAME.c_str();
				relainfo.outTopicName = itRela->OUT_TOPIC_NAME.c_str();
				
				// 获取当前需要建立分区的topic的partiton个数，也是需要
				// 增加的分区号的数字

				if (!getPartitionCount(relainfo.inTopicName, inPartitionNum))
				{
					return false;
				}

				if (!getPartitionCount(relainfo.outTopicName, outPartitionNum))
				{
					return false;
				}

				// 消费关系新增的分区 与 生产关系新增分区的关系
				relainfo.inPartitionName = Bmco::format("%?d", inPartitionNum);
				relainfo.outPartitionName = Bmco::format("%?d", outPartitionNum);
				relainfo.inMqName = itRela->IN_MQ_NAME.c_str();
				relainfo.outMqName = itRela->OUT_MQ_NAME.c_str();
				relainfo.subsys = itRela->subsys_type.c_str();
				relainfo.domain = itRela->domain_type.c_str();
				relaVec.push_back(relainfo);
				loggerStr = Bmco::format("%s|%s|%s|%s|%s|%s|", 
					relainfo.flowId, relainfo.processName, 
					relainfo.bolName, relainfo.instanceId, 
					relainfo.inTopicName, relainfo.outTopicName);
				loggerStr += Bmco::format("%s|%s|%s|%s|%s|%s", 
					relainfo.inPartitionName, relainfo.outPartitionName, 
					relainfo.inMqName, relainfo.outMqName,
					relainfo.subsys, relainfo.domain);
				bmco_information_f3(theLogger, "%s|%s|extend relainfo %s", 
					std::string("0"), std::string(__FUNCTION__), loggerStr);
				break;
			}
		}

		// 没有这样的关系，是异常的
		if (0 == relaVec.size())
		{
			bmco_information_f2(theLogger, "%s|%s|no such relation, don't need extend",std::string("0"),std::string(__FUNCTION__));
			return true;
		}

		std::vector<relaInfo>::iterator it;
		for (it = relaVec.begin();it != relaVec.end();it++)
		{		
			// 启动bol对应的实例，只启动一个
			// test
			if (!launchInstanceReq(launchBol, it->processName))
			{
				bmco_error_f2(theLogger, "%s|%s|launchInstanceReq failed!",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			// 修改上游topic定义表，创建一个partition
			if (!setPartitionCount(it->inTopicName, Bmco::NumberParser::parse(it->inPartitionName)+1))
			{
				bmco_error_f3(theLogger, "%s|%s|setPartitionCount in %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->inTopicName);
				return false;
			}

			// 修改下游topic定义表，创建一个partition
			if (!setPartitionCount(it->outTopicName, Bmco::NumberParser::parse(it->outPartitionName)+1))
			{
				bmco_error_f3(theLogger, "%s|%s|setPartitionCount out %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->outTopicName);
				return false;
			}

			// 保存到关系记录中，最后统一写入zookeeper

			// 递归创建本主题前面所有的关系
			if (!buildRelationBeforeNewBol(it->inTopicName))
			{
				bmco_error_f3(theLogger, "%s|%s|buildRelationBeforeNewBol %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->inTopicName);
				return false;
			}

			// 创建本主题的关系
			//m_relaBuildVec.insert(m_relaBuildVec.end(), relaVec.begin(), relaVec.end());
			m_relaBuildVec.push_back(*it);

			// 递归创建本主题后面所有的关系
			if (!buildRelationAfterNewBol(it->outTopicName))
			{
				bmco_error_f3(theLogger, "%s|%s|buildRelationAfterNewBol %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->outTopicName);
				return false;
			}
		}

		// 将所有变动的主题表更行到zookeeper
		if (!modifyMQTopicTable())
		{
			return false;
		}

		// 将所有变动的关系表更行到zookeeper
		if (!modifyMQRelaTable())
		{
			return false;
		}

		return true;		
	}

	// 启动对应bol的进程实例
	bool CloudDynamicFlexTaskLoop::launchInstanceReq(std::string bolName, 
			std::string processName)
	{
		std::string cloudStatusStr;
		std::string bpcbInfoPath = "/conf/cluster/" + bolName + "/regular";
	
		if (!CloudDatabaseHandler::instance()->nodeExist(bpcbInfoPath))
		{
			bmco_error_f3(theLogger, "%s|%s|%s not exist!", 
				std::string("0"),std::string(__FUNCTION__),bpcbInfoPath);
			return false;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(bpcbInfoPath, cloudStatusStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", 
					std::string("0"),std::string(__FUNCTION__), bpcbInfoPath);
				return false;
			}
			if (cloudStatusStr.empty())
			{
				cloudStatusStr = "{}";
			}
			
			Json::Value root;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(cloudStatusStr, root)) 
			{
				return false;
			}

			MetaProgramDefOp *proPtr = dynamic_cast<MetaProgramDefOp *>(ctlOper->getObjectPtr(MetaProgramDefOp::getObjName()));
			std::vector<MetaProgramDef> vecProgramDef;
			if (!proPtr->queryAll(vecProgramDef))
			{
				bmco_error_f2(theLogger, "%s|%s|get vecProgramDef failed!",std::string("0"),std::string(__FUNCTION__));
				return false;
			}
			std::vector<MetaProgramDef>::iterator itPro;
			std::string tmpExeName;
			Bmco::UInt32 tmpProId = 0;
			for (itPro = vecProgramDef.begin(); itPro != vecProgramDef.end();itPro++)
			{
				tmpExeName = itPro->ExeName.c_str();
				if (0 == tmpExeName.compare(processName))
				{
					tmpProId = itPro->ID;
				}
			}
			// 没有找到相应的程序名称
			if (0 == tmpProId)
			{
				bmco_error_f2(theLogger, "%s|%s|no such processName!",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueNewArray;
			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (tmpProId == Bmco::NumberParser::parseUnsigned(value["program_id"].asString()))
				{
					value["is_valid"] = "1";
				}
				valueNewArray[index] = value;

		        /*pColumnNode = it->second; //first为空
		        if (tmpProId != Bmco::NumberParser::parseUnsigned(pColumnNode.get<std::string>("program_id")))
	        	{
	        		pRecordNewNode.push_back(std::make_pair("", pColumnNode));
	        	}
				else
				{
					pColumnNode.put<std::string>("is_valid", "true");
					pRecordNewNode.push_back(std::make_pair("", pColumnNode));
				}*/
			}
			
			root["record"] = valueNewArray;

			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(bpcbInfoPath, oss))
			{
				bmco_error(theLogger, "Faild to write  MetaBpcbInfo");
				return false;
			}
			else
			{
				bmco_information(theLogger, "***Set jour successfully***");
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	// 创建相应topicName的partitionNum
	/*bool CloudDynamicFlexTaskLoop::modifyMQTopicTable(std::string topicName,
			Bmco::Int32 partitionNum)
	{
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		// 增加一个分区
		
		std::string zookeeperList = CloudKafkaMessageHandler::instance(m_brokerlist, m_zookeeperlist)->getZookeeperList();
		std::string kafkaPath = config.getString("kafka.operation.path");
		// 建立一个partiton
		std::string buildPartitonScript = kafkaPath + "/kafka-topics.sh --alter --topic ["
			+ topicName +  "] --zookeeper ["
			+ zookeeperList + "] --partitions ["
			+ inNumStr + "]";
		
		char cBuf[1024];
		memset(cBuf, 0, sizeof(cBuf));	
		Bmco::Int32 ret = -999; 
		ret = RunScript2(buildPartitonScript.c_str(), cBuf, 1023);
		bmco_information_f3(theLogger, "%s|%s|build partition[%s] result ret=%d, cBuf=[%s]", buildPartitonScript, ret, std::string(cBuf));
		
		if (0 != ret)
		{
			return false;
		}
		
		// 修改相应主题定义表，增加一个分区
		std::string inNumStr = Bmco::format("%?d", partitionNum+1);
        std::string cloudConfigStr;
		std::string nodePath = ZNODE_ZOOKEEPER_MQ_TOPIC;
		try
		{
        	CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr);
	        std::istringstream iss(cloudConfigStr);
			Bmco::AutoPtr<IniFileConfigurationNew> pConf = new Bmco::Util::IniFileConfigurationNew(iss);
			int topicNum = pConf->getInt(TABLE_PREFIX_LIST[1] + ".recordnum");
			Bmco::Util::AbstractConfiguration *view = pConf->createView("Table.Table.1.record");
			for (int i = 0; i < topicNum; i++)
			{
				std::string record = view->getString(Bmco::NumberFormatter::format(i + 1));
				std::vector<std::string> fields;
				boost::algorithm::split(fields, record, boost::algorithm::is_any_of("|"));
				if (0 == topicName.compare(fields[1]))
				{
					fields[4] = inNumStr;
					std::string newRecord = boost::algorithm::join(fields, "|");
					bmco_information_f3(theLogger, "%s|%s|topic new record: %s", 
						std::string("0"), std::string(__FUNCTION__), newRecord);
					view->setString(Bmco::NumberFormatter::format(i + 1), newRecord);
				}
			}
			std::ostringstream oss;
			pConf->save(oss);
			
			if (!CloudDatabaseHandler::instance()->setNodeData(nodePath, oss.str()))
			{
				bmco_error_f2(theLogger, "%s|%s|Faild to write topic Info",std::string("0"),std::string(__FUNCTION__));
				return false;
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false; 
		}

		return true;
	}*/
	bool CloudDynamicFlexTaskLoop::modifyMQTopicTable()
	{
		// 修改相应主题定义表，增加一个分区
        std::string cloudConfigStr;
		std::string nodePath = ZNODE_ZOOKEEPER_MQ_TOPIC;
		std::string topicName;
		
		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_error(theLogger, "need to createNode ZNODE_ZOOKEEPER_MQ_TOPIC");
			return false;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", 
					std::string("0"),std::string(__FUNCTION__), nodePath);
				return false;
			}
			if (cloudConfigStr.empty())
			{
				cloudConfigStr = "{}";
			}
			
			Json::Value root;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return false;
			}

			std::map<std::string, Bmco::UInt32>::iterator itTopic;
			Json::Value valueNewArray;
			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
		        topicName = value["topic_name"].asString();
				itTopic = m_topicInfoMap.find(topicName);
				if (itTopic != m_topicInfoMap.end())
				{
					value["partition_number"] = Bmco::format("%?d", itTopic->second);
				}
				valueNewArray[index] = value;
		        /*if (itTopic == m_topicInfoMap.end())
	        	{
	        		pRecordNewNode.push_back(std::make_pair("", pColumnNode));
	        	}
				else
				{
					pColumnNode.put<Bmco::UInt64>("partition_number", itTopic->second);
					pRecordNewNode.push_back(std::make_pair("", pColumnNode));
				}*/
			}
			
			root["record"] = valueNewArray;
			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(nodePath, oss))
			{
				bmco_error(theLogger, "Faild to write  MetaTopicInfo");
				return false;
			}
			else
			{
				bmco_information(theLogger, "***Set topic successfully***");
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		m_topicInfoMap.clear();
		return true;
	}

	bool CloudDynamicFlexTaskLoop::modifyMQRelaTable()
	{
		std::string cloudStatusStr;
		std::string nodePath = ZNODE_ZOOKEEPER_MQ_RELAION;

		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_error_f1(theLogger, "need to create new node: %s", nodePath);
			return false;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudStatusStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", 
					std::string("0"),std::string(__FUNCTION__), nodePath);
				return false;
			}
			
			if (cloudStatusStr.empty())
			{
				cloudStatusStr = "{}";
			}
			
			Json::Value root;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(cloudStatusStr, root)) 
			{
				return false;
			}
			
			Json::Value valueArray = root["record"];
			int msgNum = valueArray.size();

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);
			std::vector<relaInfo>::iterator it;
			for (it = m_relaBuildVec.begin();it != m_relaBuildVec.end();it++)
			{
				Json::Value array(Json::objectValue);
				array["seq_id"] = Bmco::format("%?d", ++msgNum);
				array["flow_id"] = it->flowId;
				array["program_name"] = it->processName;
				array["instance_id"] = it->instanceId;
				array["bol_name"] = it->bolName;
				array["in_partition_name"] = it->inPartitionName;
				array["in_topic_name"] = it->inTopicName;
				array["in_mq_name"] = it->inMqName;
				array["out_partition_name"] = it->outPartitionName;
				array["out_topic_name"] = it->outTopicName;
				array["out_mq_name"] = it->outMqName;
				array["status"] = "1";
				array["create_date"] = stNow;
				array["modi_date"] = stNow;
				array["subsys_type"] = it->subsys;
				array["domain_type"] = it->domain;
				array["operator_id"] = "";
				array["reserve1"] = "";
				array["reserve2"] = "";
				valueArray[msgNum] = array;
			}

			root["record"] = valueArray;
			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(nodePath, oss))
			{
				bmco_error(theLogger, "Faild to write MetaRelaInfo");
				return false;
			}
			else
			{
				bmco_information(theLogger, "***Set rela successfully***");
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		m_relaBuildVec.clear();
		return true;
	}
	
	bool CloudDynamicFlexTaskLoop::setPartitionCount(std::string topicName, 
					const Bmco::UInt32 &partitionNum)
	{
		std::map<std::string, Bmco::UInt32>::iterator it;
		it = m_topicInfoMap.find(topicName);
		if (it == m_topicInfoMap.end())
		{
			bmco_information_f3(theLogger,"%s|%s|no such topicName: %s ", 
				std::string("0"), std::string(__FUNCTION__), topicName);
			return false;
		}
		else
		{
			m_topicInfoMap[topicName] = partitionNum;
		}

		return true;
	}

	bool CloudDynamicFlexTaskLoop::getPartitionCount(std::string topicName, 
					Bmco::UInt32 &partitionNum)
	{
		std::map<std::string, Bmco::UInt32>::iterator it;
		it = m_topicInfoMap.find(topicName);
		if (it == m_topicInfoMap.end())
		{
			bmco_information_f3(theLogger,"%s|%s|no such topicName: %s ", 
				std::string("0"), std::string(__FUNCTION__), topicName);
			return false;
		}
		else
		{
			partitionNum = it->second;
		}

		return true;
	}

	/*bool CloudDynamicFlexTaskLoop::getPartitionCount(std::string topicName, 
				Bmco::UInt32 &partitionNum)
	{	
		partitionNum = 0;
		
		std::string cloudConfigStr;
		std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_MQ_TOPIC);

		if(!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_information_f3(theLogger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return false;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);
			std::istringstream issNew(cloudConfigStr);

			Bmco::AutoPtr<Bmco::Util::AbstractConfiguration> pConf = new Bmco::Util::IniFileConfiguration(issNew);
			int recordNum = pConf->getInt(TABLE_PREFIX_LIST[1] + ".recordnum");
			for (int j = 1; j <= recordNum; j++)
			{
				std::string record = pConf->getString(Bmco::format("%s.record.%?d",TABLE_PREFIX_LIST[1],j));
				Bmco::StringTokenizer tokens(record, "|", Bmco::StringTokenizer::TOK_TRIM);
				if (tokens[1] == topicName)
				{
					partitionNum = Bmco::NumberParser::parseUnsigned(tokens[4]);
					break;
				}
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}
		
		bmco_information_f4(theLogger,"%s|%s|topicName: %s partitionNum: %?d", 
			std::string("0"), std::string(__FUNCTION__), topicName, partitionNum);
		return true;
	}*/

	bool CloudDynamicFlexTaskLoop::buildRelationAfterNewBol(std::string topicName)
	{
		bmco_information_f3(theLogger, "%s|%s|buildRelationAfterNewBol outtopicName:%s", 
			std::string("0"),std::string(__FUNCTION__),topicName);
		// 获取当前需要建立分区的topic的partiton个数
		Bmco::UInt32 CurrPartNum = 0;
		//test
		/*CurrPartNum = CloudKafkaMessageHandler::instance(m_brokerlist)->getPartitionCount(topicName);
		if (0 == CurrPartNum)
		{
			return false;
		}*/
		// CurrPartNum = 4;

		if (!getPartitionCount(topicName, CurrPartNum))
		{
			return false;
		}
		
		Bmco::UInt32 outPartNum = 0;
		std::vector<MetaMQRela>::iterator itRela;
		relaInfo relainfo;
		std::vector<relaInfo> relaVec;
		std::string tmpInTopicName;
		Bmco::UInt16 tmpInstanceId = 0;
		std::string tmpBolName;
		std::string loggerStr;
		bolInstance ansBolInstance;
		if (!chooseInstanceBuildRela(topicName, ansBolInstance, DM_After))
		{
			return false;
		}

		if (ansBolInstance.bolName.empty())
		{
			bmco_information_f2(theLogger, "%s|%s|return from bottom!",std::string("0"),std::string(__FUNCTION__));
			return true;
		}
		
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			tmpInTopicName = itRela->IN_TOPIC_NAME.c_str();
			tmpBolName = itRela->BOL_CLOUD_NAME.c_str();
			tmpInstanceId = itRela->Instance_id;
			if (0 == topicName.compare(tmpInTopicName)
				&& (ansBolInstance.instanceId == tmpInstanceId)
				&& (0 == ansBolInstance.bolName.compare(tmpBolName)))
			{
				relainfo.flowId = itRela->Flow_ID.c_str();
				relainfo.processName = itRela->PROCESS_NAME.c_str();
				relainfo.bolName = itRela->BOL_CLOUD_NAME.c_str();
				relainfo.instanceId = Bmco::format("%?d", ansBolInstance.instanceId);
				relainfo.inTopicName = itRela->IN_TOPIC_NAME.c_str();
				relainfo.outTopicName = itRela->OUT_TOPIC_NAME.c_str();
				
				// 获取当前需要建立分区的topic的partiton个数
				// test
				/*outPartNum = CloudKafkaMessageHandler::instance(m_brokerlist)->getPartitionCount(relainfo.outTopicName);
				if (0 == outPartNum)
				{
					return false;
				}*/
				// outPartNum = 3;

				if (!getPartitionCount(relainfo.outTopicName, outPartNum))
				{
					return false;
				}

				// 消费关系新增的分区 与 生产关系新增分区的关系
				relainfo.inPartitionName = Bmco::format("%?d", CurrPartNum-1);
				relainfo.outPartitionName = Bmco::format("%?d", outPartNum);
				relainfo.inMqName = itRela->IN_MQ_NAME.c_str();
				relainfo.outMqName = itRela->OUT_MQ_NAME.c_str();
				relainfo.subsys = itRela->subsys_type.c_str();
				relainfo.domain = itRela->domain_type.c_str();
				relaVec.push_back(relainfo);
				loggerStr = Bmco::format("%s|%s|%s|%s|%s|%s|", 
					relainfo.flowId, relainfo.processName, 
					relainfo.bolName, relainfo.instanceId, 
					relainfo.inTopicName, relainfo.outTopicName);
				loggerStr += Bmco::format("%s|%s|%s|%s|%s|%s", 
					relainfo.inPartitionName, relainfo.outPartitionName, 
					relainfo.inMqName, relainfo.outMqName,
					relainfo.subsys, relainfo.domain);
				bmco_information_f3(theLogger, "%s|%s|extend relainfo %s", 
					std::string("0"), std::string(__FUNCTION__), loggerStr);
			}
		}

		if (0 == relaVec.size())
		{
			return true;
		}

		std::vector<relaInfo>::iterator it;
		for (it = relaVec.begin();it != relaVec.end();it++)
		{
			if (!setPartitionCount(it->outTopicName, Bmco::NumberParser::parse(it->outPartitionName)+1))
			{
				bmco_error_f3(theLogger, "%s|%s|setPartitionCount in %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->outTopicName);
				return false;
			}

			// 保存到关系记录中，最后统一写入zookeeper
			//m_relaBuildVec.insert(m_relaBuildVec.end(), relaVec.begin(), relaVec.end());
			m_relaBuildVec.push_back(*it);

			if (!buildRelationAfterNewBol(it->outTopicName))
			{
				bmco_error_f3(theLogger, "%s|%s|buildRelationAfterNewBol %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->outTopicName);
				return false;
			}
		}

		return true;
	}

	bool CloudDynamicFlexTaskLoop::buildRelationBeforeNewBol(std::string topicName)
	{
		bmco_information_f3(theLogger, "%s|%s|buildRelationBeforeNewBol intopicName:%s", 
			std::string("0"),std::string(__FUNCTION__),topicName);
		// 获取当前需要建立分区的topic的partiton个数
		Bmco::UInt32 CurrPartNum = 0;
		// test
		//CurrPartNum = CloudKafkaMessageHandler::instance(m_brokerlist)->getPartitionCount(topicName);
		// CurrPartNum = 4;

		if (!getPartitionCount(topicName, CurrPartNum))
		{
			return false;
		}
		
		if (0 == CurrPartNum)
		{
			bmco_error_f2(theLogger, "%s|%s|buildRelationBeforeNewBol CurrPartNum is zero!",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		Bmco::UInt32 inPartNum = 0;
		std::vector<MetaMQRela>::iterator itRela;
		relaInfo relainfo;
		std::vector<relaInfo> relaVec;
		std::string tmpOutTopicName;
		Bmco::UInt16 tmpInstanceId = 0;
		std::string tmpBolName;
		std::string loggerStr;
		bolInstance ansBolInstance;
		if (!chooseInstanceBuildRela(topicName, ansBolInstance, DM_Before))
		{
			return false;
		}
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			tmpOutTopicName = itRela->OUT_TOPIC_NAME.c_str();
			tmpBolName = itRela->BOL_CLOUD_NAME.c_str();
			tmpInstanceId = itRela->Instance_id;
			if (0 == topicName.compare(tmpOutTopicName)
				&& (ansBolInstance.instanceId == tmpInstanceId)
				&& (0 == ansBolInstance.bolName.compare(tmpBolName)))
			{
				relainfo.flowId = itRela->Flow_ID.c_str();
				relainfo.processName = itRela->PROCESS_NAME.c_str();
				relainfo.bolName = itRela->BOL_CLOUD_NAME.c_str();
				relainfo.instanceId = Bmco::format("%?d", ansBolInstance.instanceId);
				relainfo.inTopicName = itRela->IN_TOPIC_NAME.c_str();
				relainfo.outTopicName = itRela->OUT_TOPIC_NAME.c_str();
				
				// 获取当前需要建立分区的topic的partiton个数
				// test
				/*inPartNum = CloudKafkaMessageHandler::instance(m_brokerlist)->getPartitionCount(relainfo.inTopicName);
				if (0 == inPartNum)
				{
					return false;
				}*/
				//inPartNum = 3;

				if (!relainfo.inTopicName.empty())
				{
					if (!getPartitionCount(relainfo.inTopicName, inPartNum))
					{
						return false;
					}
					// 需要新增的partition号码为inPartNum
					relainfo.inPartitionName = Bmco::format("%?d", inPartNum);
				}
				else
				{
					relainfo.inPartitionName = "";
				}

				// 消费关系新增的分区 与 生产关系新增分区的关系
				// 已经新增了partition，号码为CurrPartNum-1
				relainfo.outPartitionName = Bmco::format("%?d", CurrPartNum-1);
				relainfo.inMqName = itRela->IN_MQ_NAME.c_str();
				relainfo.outMqName = itRela->OUT_MQ_NAME.c_str();
				relainfo.subsys = itRela->subsys_type.c_str();
				relainfo.domain = itRela->domain_type.c_str();
				relaVec.push_back(relainfo);
				loggerStr = Bmco::format("%s|%s|%s|%s|%s|%s|", 
					relainfo.flowId, relainfo.processName, 
					relainfo.bolName, relainfo.instanceId, 
					relainfo.inTopicName, relainfo.outTopicName);
				loggerStr += Bmco::format("%s|%s|%s|%s|%s|%s", 
					relainfo.inPartitionName, relainfo.outPartitionName, 
					relainfo.inMqName, relainfo.outMqName,
					relainfo.subsys, relainfo.domain);
				bmco_information_f3(theLogger, "%s|%s|extend relainfo %s", 
					std::string("0"), std::string(__FUNCTION__), loggerStr);
				// break;
			}
		}

		// 递归到尽头了，出口
		if (0 == relaVec.size())
		{
			bmco_information_f2(theLogger, "%s|%s|return from bottom!",std::string("0"),std::string(__FUNCTION__));
			return true;
		}

		std::vector<relaInfo>::iterator it;
		for (it = relaVec.begin();it != relaVec.end();it++)
		{
			// 递归到尽头了，出口
			if (it->inTopicName.empty())
			{
				m_relaBuildVec.insert(m_relaBuildVec.end(), relaVec.begin(), relaVec.end());
				bmco_information_f2(theLogger, "%s|%s|return from top!",std::string("0"),std::string(__FUNCTION__));
				return true;
			}
			
			if (!setPartitionCount(it->inTopicName, Bmco::NumberParser::parse(it->inPartitionName)+1))
			{
				bmco_error_f3(theLogger, "%s|%s|setPartitionCount in %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->inTopicName);
				return false;
			}

			// 保存到关系记录中，最后统一写入zookeeper
			//m_relaBuildVec.insert(m_relaBuildVec.end(), relaVec.begin(), relaVec.end());
			m_relaBuildVec.push_back(*it);

			if (!buildRelationBeforeNewBol(it->inTopicName))
			{
				bmco_error_f3(theLogger, "%s|%s|buildRelationBeforeNewBol %s failed!", 
						std::string("0"),std::string(__FUNCTION__), it->inTopicName);
				return false;
			}
		}

		return true;
	}

	// 查询挂载分区最少的bol的实例号，前提是同一进程的实例
	bool CloudDynamicFlexTaskLoop::chooseInstanceBuildRela(std::string topicName,
			bolInstance &ansBolInstance, enum directionMark mark)
	{
		std::vector<MetaMQRela>::iterator itRela;
		relaInfo relainfo;
		std::vector<relaInfo> relaVec;
		std::string tmpTopicName;
		bolInstance tmpBolInstance;
		std::map<bolInstance, Bmco::UInt32> instanceRelaNumMap;
		std::map<bolInstance, Bmco::UInt32>::iterator itMap;
		instanceRelaNumMap.clear();
		ansBolInstance.bolName = "";
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			if (DM_Before == mark)
			{
				tmpTopicName = itRela->OUT_TOPIC_NAME.c_str();
			}
			else if (DM_After == mark)
			{
				tmpTopicName = itRela->IN_TOPIC_NAME.c_str();
			}
			else
			{
				bmco_error_f2(theLogger, "%s|%s|no such mark", 
					std::string("0"),std::string(__FUNCTION__));
				return false;
			}
			
			if (0 == topicName.compare(tmpTopicName))
			{
				tmpBolInstance.instanceId = itRela->Instance_id;
				tmpBolInstance.bolName = itRela->BOL_CLOUD_NAME.c_str();
				itMap = instanceRelaNumMap.find(tmpBolInstance);
				if (itMap != instanceRelaNumMap.end())
				{
					instanceRelaNumMap[tmpBolInstance]++;
				}
				else
				{
					instanceRelaNumMap.insert
						(std::pair<bolInstance, Bmco::UInt32>(tmpBolInstance, 1));
				}
			}
		}

		if (0 == instanceRelaNumMap.size())
		{
			if (DM_After == mark)
			{
				bmco_information_f2(theLogger, "%s|%s|reach the bottom topic", 
					std::string("0"),std::string(__FUNCTION__));
				return true;
			}
			else
			{
				bmco_error_f2(theLogger, "%s|%s|instanceRelaNumMap is empty", 
					std::string("0"),std::string(__FUNCTION__));
				return false;
			}
		}
		
		itMap = instanceRelaNumMap.begin();
		Bmco::UInt16 ansInstance = itMap->second;
		ansBolInstance.bolName = itMap->first.bolName;
		ansBolInstance.instanceId = itMap->first.instanceId;
		itMap++;
		for (;itMap!= instanceRelaNumMap.end();itMap++)
		{
			if (ansInstance > itMap->second)
			{
				ansInstance = itMap->second;
				ansBolInstance.bolName = itMap->first.bolName;
				ansBolInstance.instanceId = itMap->first.instanceId;
			}
		}

		bmco_information_f4(theLogger, "%s|%s|instanceId = %?d, bolName = %s", 
			std::string("0"),std::string(__FUNCTION__), ansBolInstance.instanceId, 
			ansBolInstance.bolName);
		return true;
	}

	bool CloudDynamicFlexTaskLoop::shrinkPartitionAndBolFlow(std::string topicName)
	{
		bmco_information(theLogger, "shrinkPartitionAndBolFlow()");

		// 获取可以停掉的bol
		std::string ShrinkBol;
		if (!chooseSuitAbleBolForStop(topicName, ShrinkBol))
		{
			bmco_error_f2(theLogger, "%s|%s|no SuitAbleBolForStop",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		// 如果只有ShrinkBol处理本topic，则不可以n-1
		if (isTheOnlyBolForTheTopic(topicName, ShrinkBol))
		{
			bmco_error_f4(theLogger, "%s|%s|Only %s doWithTheTopic %s can't do shrink", 
				std::string("0"),std::string(__FUNCTION__),ShrinkBol,topicName);
			return false;
		}

		if (!setRemoteBolStatus(ShrinkBol, BOL_MAINTAIN))
		{
			bmco_error_f2(theLogger, "%s|%s|setBolMaintains failed",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		if (!hasStoppedALLHLAProcessTimeOut(ShrinkBol))
		{
			return false;
		}

		// 将该bol消息关系置为失效
		std::vector<MetaMQRela>::iterator itRela;
		std::vector<Bmco::UInt32> seqNoVec;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			if (0 == ShrinkBol.compare(itRela->BOL_CLOUD_NAME.c_str()))
			{
				seqNoVec.push_back(itRela->SEQ_ID);
			}
		}

		if (!setBolMessageRelaInvalid(seqNoVec))
		{
			bmco_error_f2(theLogger, "%s|%s|setBolMessageRelaInvalid failed",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		if (!setRelaWithConsumeBolInstance(topicName, ShrinkBol))
		{
			bmco_error_f2(theLogger, "%s|%s|setRelaWithConsumeBolInstance failed",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		if (!modifyMQRelaTable())
		{
			bmco_error_f2(theLogger, "%s|%s|modifyMQRelaTable failed",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	bool CloudDynamicFlexTaskLoop::chooseSuitAbleBolForStop(std::string topicName, 
		 std::string &ShrinkBol)
	{
		// 获取所有消费本主题的bol名称
		std::set<std::string> bolNameSet;
		std::vector<MetaMQRela>::iterator itRela;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			if (0 == topicName.compare(itRela->IN_TOPIC_NAME.c_str()))
			{
				bolNameSet.insert(std::string(itRela->BOL_CLOUD_NAME.c_str()));
			}
		}
		
		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);

		std::set<std::string>::iterator itSet;
		std::vector<struct bolinfo>::iterator itSession;
		for (itSet = bolNameSet.begin();itSet != bolNameSet.end();itSet++)
		{
			std::vector<std::string>::iterator itNode;
			for (itSession = sessionBolInfo.begin();
					itSession != sessionBolInfo.end();itSession++)
			{
				// 找出一个可以用于动态伸缩的bol
				if (0 == itSession->bol_cloud_name.compare(*itSet)
					&& ("1" == itSession->auto_flex_flag))
				{
					ShrinkBol = itSession->bol_cloud_name;
					bmco_information_f3(theLogger, "%s|%s|shrink bol : %s", 
						std::string("0"), std::string(__FUNCTION__), ShrinkBol);
					return true;
				}
			}
		}
		
		ShrinkBol = "";
		bmco_information_f2(theLogger, "%s|%s|no bol can be shrinked", 
			std::string("0"), std::string(__FUNCTION__));
		return false;
	}

	// 将指定的bol改变状态
	bool CloudDynamicFlexTaskLoop::setRemoteBolStatus(std::string bolName,
			BolStatus status)
	{
		std::string des_ipaddr;
		//std::string des_dir;
		//std::string des_user;

		std::vector<struct bolinfo> sessionBolInfo;
		g_sessionData.getBolInfo(sessionBolInfo);
		std::vector<struct bolinfo>::iterator it;
		for (it = sessionBolInfo.begin();
			it != sessionBolInfo.end();it++)
		{
			if (0 == bolName.compare(it->bol_cloud_name))
			{
				des_ipaddr = it->ip_addr;
				//des_dir = it->nedir;
				//des_user = it->userName;
			}
		}
	
		std::string outStr;
		Timestamp now;
		LocalDateTime dt;
		std::string nowStr;
		dt = now;
		nowStr = DateTimeFormatter::format(dt, DateTimeFormat::SORTABLE_FORMAT);

		MetaBolInfo::Ptr ptr = new MetaBolInfo(BOL_NORMAL, "", "");
			MetaBolInfoOp *p = dynamic_cast<MetaBolInfoOp *>
				(ctlOper->getObjectPtr(MetaBolInfoOp::getObjName()));
		BolStatus tmpStatus = BOL_NORMAL;
		Bmco::UInt32 tmpCrc32CheckSum = 0;
		if (!p->Query(tmpStatus, tmpCrc32CheckSum))
		{
			return false;
		}

		if (tmpStatus == status)
		{
			bmco_information_f3(theLogger, "%s|%s|status %?d has been set", 
				std::string("0"),std::string(__FUNCTION__),status);
			return true;
		}
		
		try
		{
			HTTPClientSession cs(des_ipaddr.c_str(), 9090);
			Json::Value head(Json::objectValue);
			Json::Value content(Json::objectValue);
			Json::Value rootReq(Json::objectValue);

			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);

			head["processcode"] = "changebolstatus";
			head["reqseq"] = "012014120101";
			head["reqtime"] = stNow;

			content["bol_name"] = bolName;
			content["old_status"] = tmpStatus;
			content["new_status"] = status;

			rootReq["head"] = head;
			rootReq["content"] = content;

			Json::FastWriter jfw;
			std::string oss = jfw.write(rootReq);
			
			//HTTPRequest request("POST", "/echoBody");
			HTTPRequest request;
			request.setContentLength((int) oss.length());
			request.setContentType("text/plain");
			cs.sendRequest(request) << oss;
			HTTPResponse response;
			std::string rbody;
			cs.receiveResponse(response) >> rbody;

			Json::Value rootRsp;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(rbody, rootRsp)) 
			{
				bmco_error_f2(theLogger, "%s|%s|Failed to execute jsonReader",std::string("0"),std::string(__FUNCTION__));
				return false;
			}
			if (!rootRsp.isMember("content")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no content",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			Json::Value value = rootRsp["content"];
			std::string respcode = value["respcode"].asString();
			if (respcode != "0")
			{
				return false;
			}
		}
		catch(Bmco::Exception &e)
		{
			return false;
		}

		return true;
	}

	bool CloudDynamicFlexTaskLoop::hasStoppedALLHLAProcessTimeOut(std::string ShrinkBol)
	{
		Bmco::Timestamp recordTime;
		Bmco::Timestamp now;
		// 默认等待4秒
		Bmco::Int64 timeOut = 4000000;
		std::vector<struct dictinfo> sessionDictInfo;
		g_sessionData.getDictInfo(sessionDictInfo);
		std::vector<struct dictinfo>::iterator itDict;
		for (itDict = sessionDictInfo.begin();
			itDict != sessionDictInfo.end();itDict++)
		{
			// 在zookeeper字典路径下找到超时时间
			if ((0 == itDict->key.compare("WaitTimeForStopHLA"))
				&& (0 == itDict->id.compare("1000")))
			{
				timeOut = Bmco::NumberParser::parse(itDict->value);
				break;
			}
		}
			
		bool stopAllHLAProecss = false;
		// 在超时时间内
		while (now - recordTime < timeOut)
		{
			if (defineHLAProcessShutdownAlready(ShrinkBol))
			{
				bmco_information_f2(theLogger, "%s|%s|all HLA process has stopped",std::string("0"),std::string(__FUNCTION__));
				stopAllHLAProecss = true;
				break;
			}
			now.update();
		}

		if (!stopAllHLAProecss)
		{
			bmco_error_f2(theLogger, "%s|%s|some HLA process has not stop",std::string("0"),std::string(__FUNCTION__));
			if (!setRemoteBolStatus(ShrinkBol, BOL_NORMAL))
			{
				bmco_error_f2(theLogger, "%s|%s|set remote bol normal failed",std::string("0"),std::string(__FUNCTION__));
			}
			return false;
		}

		return true;
	}

	// 确认bol的HLA进程已经停止完毕
	bool CloudDynamicFlexTaskLoop::defineHLAProcessShutdownAlready(
			std::string bolName)
	{
		bmco_debug(theLogger, "defineHLAProcessShutdownAlready()");

		std::string bpcbInfoPath = "/status/bpcb/" + bolName;
		std::string cloudConfigStr;
		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(bpcbInfoPath))
			{
				bmco_error_f3(theLogger,"%s|%s|node:%s has been stopped!",std::string("0"),std::string(__FUNCTION__),
					bpcbInfoPath);
				return false;
			}
			
			if (0 != CloudDatabaseHandler::instance()->getNodeData(bpcbInfoPath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", std::string("0"),std::string(__FUNCTION__), bpcbInfoPath);
				return false;
			}

			Json::Value root;
			Json::Reader jsonReader;

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return false;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			Json::Value valueArray = root["record"];

			Bmco::UInt64 bpcbid;
			Bmco::UInt64 status;
			std::vector<struct flexinfo> tmpVec;
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				bpcbid = boost::lexical_cast<Bmco::UInt64>(value["bpcb_id"].asString());
				status = boost::lexical_cast<Bmco::UInt64>(value["status"].asString());
				if ((999 < bpcbid)
					&& (1 == status))
				{
					bmco_information_f3(theLogger, "%s|%s|bpcbid %?d", 
						std::string("0"),std::string(__FUNCTION__), bpcbid);
					return false;
				}
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	// 修改消息关系表，将主键在seqNoVec中的记录失效
	bool CloudDynamicFlexTaskLoop::setBolMessageRelaInvalid
		(std::vector<Bmco::UInt32> seqNoVec)
	{
		std::string seqNoZKStr;
		std::string seqNoVCStr;
		std::string cloudConfigStr;
		std::string nodePath = ZNODE_ZOOKEEPER_MQ_RELAION;

		if (!CloudDatabaseHandler::instance()->nodeExist(nodePath))
		{
			bmco_error_f1(theLogger, "need to create %s!", nodePath);
			return false;
		}

		try
		{
			if (0 != CloudDatabaseHandler::instance()->getNodeData(nodePath, cloudConfigStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", 
					std::string("0"),std::string(__FUNCTION__), nodePath);
				return false;
			}
			if (cloudConfigStr.empty())
			{
				cloudConfigStr = "{}";
			}
			Json::Value root;
			Json::Reader jsonReader;
			
			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return false;
			}

			std::vector<Bmco::UInt32>::iterator itSeq;
			Json::Value valueNewArray;
			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
		        for (itSeq = seqNoVec.begin();itSeq != seqNoVec.end();itSeq++)
				{
					seqNoVCStr = Bmco::format("%?d", *itSeq);
					seqNoZKStr = value["seq_id"].asString();
					if (seqNoVCStr == seqNoZKStr)
					{
						value["status"] = "0";
					}
					valueNewArray[index] = value;
					/*if (seqNoVCStr != seqNoZKStr)
					{
						pRecordNewNode.push_back(std::make_pair("", pColumnNode));
					}
					else
					{
						pColumnNode.put<std::string>("status", "0");
						pRecordNewNode.push_back(std::make_pair("", pColumnNode));
					}*/
				}
				// pRecordNode = pRecordNewNode;
			}

			root["record"] = valueNewArray;
			Json::FastWriter jfw;
			std::string oss = jfw.write(root);
			if (!CloudDatabaseHandler::instance()->setNodeData(nodePath, oss))
			{
				bmco_error(theLogger, "Faild to Set rela invalid");
				return false;
			}
			else
			{
				bmco_information(theLogger, "***Set rela invalid successfully***");
			}
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when recordHostBaseInfo!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	bool CloudDynamicFlexTaskLoop::setRelaWithConsumeBolInstance
		(std::string topicName,  
		std::string ShrinkBol)
	{
		relaInfo relainfo;
		std::vector<relaInfo> relaVec;
		Bmco::Int32 inPartitionNum = 0;
		Bmco::Int32 outPartitionNum = 0;
		std::string loggerStr;
		bolInstance ansBolInstance;
		std::vector<MetaMQRela>::iterator itRela;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{		
			// 是需要重新建立关系的topic和消费的bol
			if (0 == topicName.compare(itRela->IN_TOPIC_NAME.c_str())
				&& 0 == ShrinkBol.compare(itRela->BOL_CLOUD_NAME.c_str()))
			{
				// 确定失效的分区新的挂载实例
				if (!chooseInstanceBuildRela(topicName, ansBolInstance, DM_After))
				{
					return false;
				}
				relainfo.flowId = itRela->Flow_ID.c_str();
				relainfo.processName = itRela->PROCESS_NAME.c_str();
				// 新bol和实例号
				relainfo.bolName = ansBolInstance.bolName;
				relainfo.instanceId = Bmco::format("%?d",ansBolInstance.instanceId);
				relainfo.inTopicName = itRela->IN_TOPIC_NAME.c_str();
				relainfo.outTopicName = itRela->OUT_TOPIC_NAME.c_str();
				relainfo.inPartitionName = itRela->IN_PARTITION_NAME.c_str();
				relainfo.outPartitionName = itRela->OUT_PARTITION_NAME.c_str();
				relainfo.inMqName = itRela->IN_MQ_NAME.c_str();
				relainfo.outMqName = itRela->OUT_MQ_NAME.c_str();
				relainfo.subsys = itRela->subsys_type.c_str();
				relainfo.domain = itRela->domain_type.c_str();
				relaVec.push_back(relainfo);
				loggerStr = Bmco::format("%s|%s|%s|%s|%s|%s", 
					relainfo.flowId, relainfo.processName, 
					relainfo.bolName, relainfo.instanceId, 
					relainfo.inTopicName, relainfo.outTopicName);
				loggerStr += Bmco::format("%s|%s|%s|%s|%s|%s", 
					relainfo.inPartitionName, relainfo.outPartitionName, 
					relainfo.inMqName, relainfo.outMqName,
					relainfo.subsys, relainfo.domain);
				bmco_information_f3(theLogger, "%s|%s|shrink new relainfo %s", std::string("0"),std::string(__FUNCTION__), loggerStr);
			}
		}

		// 保存到关系记录中，最后统一写入zookeeper
		m_relaBuildVec.insert(m_relaBuildVec.end(), relaVec.begin(), relaVec.end());
		return true;
	}

	// 获取消息堆积数
	bool CloudDynamicFlexTaskLoop::accumulateMessageNum(const Bmco::Timespan &intervalTime,
		std::map<std::string, Bmco::Int64> &TopicAccumulateNumVec, 
		std::map<std::string, mqCalcOffsetData> &TopicUpDownOffsetVec)
	{
		bmco_debug(theLogger, "accumulateMessageNum()");

		std::vector<MetaMQTopic>::iterator itTopic;
		std::vector<MetaMQRela>::iterator itRela;
		std::string inTopicName;
		std::string inPartition;
		std::string inMqname;
		std::string outTopicName;
		std::string outPartition;
		std::string outMqName;
		
		// 每一个分区的消息积压数
		Bmco::Int64 accumulateBypartition = 0;
		mqCalcOffsetData result;
		TopicAccumulateNumVec.clear();
		std::map<std::string, Bmco::Int64>::iterator itAccu;

		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			inPartition = itRela->IN_PARTITION_NAME.c_str();
			inTopicName = itRela->IN_TOPIC_NAME.c_str();
			inMqname = itRela->IN_MQ_NAME.c_str();
			outTopicName = itRela->OUT_TOPIC_NAME.c_str();
			outPartition = itRela->OUT_PARTITION_NAME.c_str();
			outMqName = itRela->OUT_MQ_NAME.c_str();
			
			if (inPartition.empty()
				|| inTopicName.empty()
				|| inMqname.empty()
				|| outTopicName.empty()
				|| outPartition.empty()
				|| outMqName.empty())
			{
				bmco_information(theLogger, "this is the head of the flow or not kafka, neglect it!\n");
				continue;
			}
			
			// 通过上下游的关系计算
			if (!getMessageAccumuNumByPatition(inTopicName, 
				inPartition, inMqname, outTopicName, 
				outPartition, outMqName,
				accumulateBypartition, result))
			{
				bmco_error_f1(theLogger, "%s getOffsetError Skip it", inTopicName);
				continue;
				//return false;
			}

			// 将partition维度的统计加入
			TopicAccumulateNumVec.insert(std::pair<std::string, Bmco::Int64>(inTopicName+"#"+inPartition, accumulateBypartition));

			// 对同一主题的堆积消息数进行累加
			itAccu = TopicAccumulateNumVec.find(inTopicName);
			if (itAccu != TopicAccumulateNumVec.end())
			{
				TopicAccumulateNumVec[inTopicName] += accumulateBypartition;
			}
			else
			{
				TopicAccumulateNumVec.insert(std::pair<std::string, Bmco::Int64>(inTopicName, accumulateBypartition));
			}

			mqCalcOffsetData calcResult;
			calcRWSpeed(inTopicName+"#"+inPartition, intervalTime, result, calcResult);
			TopicUpDownOffsetVec.insert(std::pair<std::string, mqCalcOffsetData>(inTopicName+"#"+inPartition, calcResult));
		}
		
		updateLastAccumulateNumMap();
		return true;
	}

	bool CloudDynamicFlexTaskLoop::getMessageAccumuNumByPatition(std::string inTopicName,
	 	std::string inPartitionName, std::string inMqName,
	 	std::string outTopicName, std::string outPartitionName, 
	 	std::string outMqName, Bmco::Int64 &accumulateNum, mqCalcOffsetData &result)
	{
		Bmco::Int64 upStreamOffset = 0;
		Bmco::Int64 downStreamOffset = 0;

		MetaMQDef::Ptr pMQDef = new MetaMQDef("", "", "", "", "", 0, 0);
		if (!m_DefPtr->getMQDefByDefName(inMqName, pMQDef))
		{
			bmco_error(theLogger, "no such mqname!!!");
			accumulateNum = 0;
			return false;
		}	

		m_brokerlist = pMQDef->Broker_list1.c_str();

		if (2 == pMQDef->mq_type)
		{
			bmco_information(theLogger, "this is acct service mq, neglect it!");
			accumulateNum = 0;
			return false;
		}
		
		if (!CloudKafkaMessageHandler::instance(m_brokerlist)->calcuMessageGrossByPartition(inTopicName, inPartitionName, upStreamOffset))
		{
			return false;
		}

		if (!m_DefPtr->getMQDefByDefName(outMqName, pMQDef))
		{
			bmco_error(theLogger, "no such mqname!!!");
			accumulateNum = 0;
			return false;
		}

		if (2 == pMQDef->mq_type)
		{
			bmco_information(theLogger, "this is acct service mq, neglect it!");
			accumulateNum = 0;
			return false;
		}

		m_brokerlist = pMQDef->Broker_list1.c_str();

		if (!CloudKafkaMessageHandler::instance(m_brokerlist)->calcuReadMessageOffsetByPartition(outTopicName, outPartitionName, downStreamOffset))
		{
			return false;
		}

		accumulateNum = upStreamOffset - downStreamOffset;
		accumulateNum = (0 > accumulateNum) ? 0 : accumulateNum;

		result.upStreamOffset = upStreamOffset;
		result.downStreamOffset = downStreamOffset;
		std::string logStr = Bmco::format("topic %s partition %s"
			" upStreamOffset = %?d, downStreamOffset = %?d, accumulateNum = %?d", 
			inTopicName, inPartitionName, upStreamOffset, downStreamOffset, accumulateNum);
		bmco_information_f1(theLogger, "%s\n", logStr);
		return true;
	}

	void CloudDynamicFlexTaskLoop::doBrokenDownBolOperation()
	{
		bmco_debug(theLogger, "doBrokenDownBolOperation()");

		std::string brokenBolName;
		// 检查是否有bol宕机
		if (!checkIfExistBolBrokenDown(brokenBolName))
		{
			bmco_information(theLogger, "all bol do well");
			return;
		}

		std::vector<MetaMQRela>::iterator itRela;
		std::vector<Bmco::UInt32> seqNoVec;
		std::set<std::string> topicSet;
		for (itRela = m_relaInfoVec.begin();itRela != m_relaInfoVec.end();itRela++)
		{
			if (0 == brokenBolName.compare(itRela->BOL_CLOUD_NAME.c_str()))
			{
				// 需要将该bol消息关系置为失效
				seqNoVec.push_back(itRela->SEQ_ID);
				// 需要托管由该bol处理的topic分区
				topicSet.insert(itRela->IN_TOPIC_NAME.c_str());
			}
		}
		
		// 将该bol消息关系置为失效
		if (!setBolMessageRelaInvalid(seqNoVec))
		{
			bmco_error_f2(theLogger, "%s|%s|setBolMessageRelaInvalid failed",std::string("0"),std::string(__FUNCTION__));
			return;
		}

		std::set<std::string>::iterator itSet;
		std::string topicName;
		// 对于宕掉的bol的所有消费关系的topic进行关系处理
		for (itSet = topicSet.begin();itSet != topicSet.end();itSet++)
		{
			topicName = *itSet;

			// 如果只有ShrinkBol处理本topic，则需要发送严重警告
			if (isTheOnlyBolForTheTopic(topicName, brokenBolName))
			{
				bmco_warning_f4(theLogger, "%s|%s|Only %s doWithTheTopic %s core down seriously", 
					std::string("0"),std::string(__FUNCTION__),brokenBolName,topicName);
				return;
			}

			// 托管宕掉的bol的主题关系到其他bol的实例
			if (!setRelaWithConsumeBolInstance(topicName, brokenBolName))
			{
				bmco_error_f2(theLogger, "%s|%s|setRelaWithConsumeBolInstance failed",std::string("0"),std::string(__FUNCTION__));
				return;
			}
		}

		// 修改zookeeper上的消息队列关系表
		if (!modifyMQRelaTable())
		{
			bmco_error_f2(theLogger, "%s|%s|modifyMQRelaTable failed",std::string("0"),std::string(__FUNCTION__));
			return;
		}
	}

	// 是否有bol宕机了，返回宕机的bol名称，每次只检查一个
	bool CloudDynamicFlexTaskLoop::checkIfExistBolBrokenDown(std::string &brokenBolName)
	{
		Bmco::Util::AbstractConfiguration& config = Bmco::Util::Application::instance().config();
		std::vector<std::string> nodes;
		std::string nodePath = ZNODE_ZOOKEEPER_SLAVE_DIR;

		// 获取当前存在的节点
		if (!CloudDatabaseHandler::instance()->GetChildrenNode(nodePath, nodes))
		{
			bmco_information_f2(theLogger, "%s|%s|GetChildrenNode failed, return.",std::string("0"),std::string(__FUNCTION__));
			return false;
		}
		std::vector<struct kpiinfo> sessionKpiInfo;
		g_sessionData.getKpiInfo(sessionKpiInfo);
		std::vector<std::string>::iterator itNode;
		std::vector<struct kpiinfo>::iterator itKpi;
		for (itKpi = sessionKpiInfo.begin();
				itKpi != sessionKpiInfo.end();itKpi++)
		{
			// 各个节点上报的状态正常的bol，但是临时节点已经消失
			// 被认为是宕机的bol
			if ("1001" == itKpi->kpi_id
				&& "0" == itKpi->kpi_value)
			{
				itNode = find(nodes.begin(), nodes.end(), itKpi->bol_cloud_name);
				// 记录宕机的bol名称
				if (itNode == nodes.end())
				{
					brokenBolName = itKpi->bol_cloud_name;
					bmco_warning_f3(theLogger, "%s|%s|%s has broken!!!", 
						std::string("0"), std::string(__FUNCTION__), brokenBolName);
					return true;
				}
			}
			
		}

		return false;
	}
	// 获取Common的指标信息同步到ZooKeeper
	void CloudDynamicFlexTaskLoop::refreshKPIInfoByMaster(std::map<std::string, Bmco::Int64> topicMessageNumMap, 
		std::map<std::string, mqCalcOffsetData> TopicUpDownOffsetVec)
	{
		bmco_debug(theLogger, "refreshKPIInfoByMaster()");
		
		MetaKPIInfoOp *tmpKPIPtr = NULL;
		std::vector<MetaKPIInfo> vecKPIInfo;
		tmpKPIPtr = dynamic_cast<MetaKPIInfoOp*>(ctlOper->getObjectPtr(MetaKPIInfoOp::getObjName()));

		if (!tmpKPIPtr->getAllKPIInfo(vecKPIInfo))
		{
			bmco_error(theLogger, "Failed to execute getAllKPIInfo on MetaShmKPIInfoTable");
		}

		std::string cloudKPIStr;
		std::string KPIInfoPath = "/status/kpi/common";
		bmco_debug_f1(theLogger, "KPIInfoPath=%s", KPIInfoPath);
		
		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(KPIInfoPath))
			{
				if (!CloudDatabaseHandler::instance()->createNode(KPIInfoPath))
				{
					bmco_error_f1(theLogger, "Faild to createNode: %s", KPIInfoPath);
					return;
				}
			}
			if (0 != CloudDatabaseHandler::instance()->getNodeData(KPIInfoPath, cloudKPIStr))
			{
				bmco_error_f3(theLogger, "%s|%s|data error at %s", 
					std::string("0"),std::string(__FUNCTION__), KPIInfoPath);
				return;
			}
			if (cloudKPIStr.empty())
			{
				cloudKPIStr = "{}";
			}

			Json::Value root(Json::objectValue);
			Json::Value field(Json::objectValue);
			Json::Value record(Json::arrayValue);

			field["kpi_id"] = "指标编号";
			field["seq_no"] = "Topic+Partition";
			field["bol_name"] = "bol名称";
			field["kpi_value"] = "指标值";
			field["kpi_value2"] = "指标值2";
			field["status_time"] = "变更时间";

			Bmco::UInt32 msgNum = 0;
			Bmco::Timestamp now;
			LocalDateTime dtNow(now);
			std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);
			std::map<std::string, Bmco::Int64>::iterator it;
			for (it = topicMessageNumMap.begin();it != topicMessageNumMap.end();it++)
			{
				Json::Value array(Json::objectValue);
				array["kpi_id"] = "1002";
				array["seq_no"] = it->first;
				array["bol_name"] = "";
				array["kpi_value"] = Bmco::format("%?d", it->second);
				array["kpi_value2"] = "";
				array["status_time"] = stNow;
				record[msgNum++] = array;
			}
			
			std::map<std::string, mqCalcOffsetData>::iterator itOff;
			for (itOff = TopicUpDownOffsetVec.begin();itOff != TopicUpDownOffsetVec.end();itOff++)
			{
				{
					Json::Value array(Json::objectValue);
					array["kpi_id"] = "1013";
					array["seq_no"] = itOff->first;
					array["bol_name"] = "";
					array["kpi_value"] = Bmco::format("%0.2f", itOff->second.upStreamOffset);
					array["kpi_value2"] = "";
					array["status_time"] = stNow;
					record[msgNum++] = array;
				}
				{
					Json::Value array(Json::objectValue);
					array["kpi_id"] = "1014";
					array["seq_no"] = itOff->first;
					array["bol_name"] = "";
					array["kpi_value"] = Bmco::format("%0.2f", itOff->second.downStreamOffset);
					array["kpi_value2"] = "";
					array["status_time"] = stNow;
					record[msgNum++] = array;
				}
			}

			for (int i = 0; i < vecKPIInfo.size(); i++)
			{
				if (1003 == vecKPIInfo[i].KPI_ID)
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
				array["bol_cloud_name"] = std::string(vecKPIInfo[i].Bol_Cloud_Name.c_str());
				array["kpi_value"] = std::string(vecKPIInfo[i].KPI_Value.c_str());
				array["kpi_value2"] = std::string("");
				array["status_time"] = stModityTime;
				record[msgNum++] = array;
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
			else
			{
				bmco_debug(theLogger, "Set Master KPI info successfully.");
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
	
	void CloudDynamicFlexTaskLoop::calcRWSpeed(std::string fullname, 
		const Bmco::Timespan &intervalTime, 
		const mqCalcOffsetData &result, mqCalcOffsetData &calcResult)
	{
		std::map<std::string, mqCalcOffsetData>::iterator itAgo;
		itAgo = m_LastAccumulateNumMap.find(fullname);
		if (m_LastAccumulateNumMap.end() != itAgo)
		{
			int interval = intervalTime.totalSeconds();
			calcResult.upStreamOffset = (result.upStreamOffset - itAgo->second.upStreamOffset)/interval;
			calcResult.downStreamOffset = (result.downStreamOffset - itAgo->second.downStreamOffset)/interval;
			std::string logStr = Bmco::format("upStreamOffset %f %f"
				"downStreamOffset %f %f totalSeconds = %?d", 
				result.upStreamOffset, itAgo->second.upStreamOffset, result.downStreamOffset, itAgo->second.downStreamOffset, interval);
			logStr += Bmco::format("calcResult %f %f", calcResult.upStreamOffset, calcResult.downStreamOffset);
			bmco_information_f1(theLogger, "%s\n", logStr);
			calcResult.upStreamOffset = (0 > calcResult.upStreamOffset) ? 0.00 : calcResult.upStreamOffset;
			calcResult.downStreamOffset = (0 > calcResult.downStreamOffset) ? 0.00 : calcResult.downStreamOffset;
		}
		// 第一次统计速度
		else
		{
			//calcResult.upStreamOffset = result.upStreamOffset;
			//calcResult.downStreamOffset = result.upStreamOffset;
			calcResult.upStreamOffset = 0.0;
			calcResult.downStreamOffset = 0.0;
		}

		m_CurrAccumulateNumMap.insert(std::pair<std::string, mqCalcOffsetData>(fullname, result));
	}

	void CloudDynamicFlexTaskLoop::updateLastAccumulateNumMap()
	{
		m_LastAccumulateNumMap.clear();
		m_LastAccumulateNumMap.swap(m_CurrAccumulateNumMap);
		m_CurrAccumulateNumMap.clear();
	}

	void CloudDynamicFlexTaskLoop::setLastTimeStamp() 
	{
	    _lastTimeStamp.update();
	}

	void CloudDynamicFlexTaskLoop::getIntervalTime(Bmco::Timespan& timeCost) 
	{
	    timeCost = _lastTimeStamp.elapsed();
	}
}

