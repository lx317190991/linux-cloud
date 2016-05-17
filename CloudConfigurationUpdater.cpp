#include "Bmco/StringTokenizer.h"
#include "Bmco/NumberFormatter.h"
#include "Bmco/Util/IniFileConfiguration.h"
#include "IniFileConfigurationNew.h"
//#include "OmcInfoUpdater.h"
#include "CloudAgentConstants.h"
#include "CloudConfigurationUpdater.h"
#include "Bmco/Format.h"
#include "Bmco/FileStream.h"
#include "Bmco/File.h"
#include <string>
#include <vector>
#include <sstream>
#include <boost/typeof/typeof.hpp>
#include <boost/lexical_cast.hpp>
#include "json/json.h"

// using namespace boost::property_tree;
// using namespace boost::gregorian;
// using namespace boost;

using Bmco::File;
using Bmco::Path;

namespace BM35
{
	template<typename T>
	T CloudConfigurationUpdater::setDefaultValue(const std::string &value,
		T defValue)
	{
		if (value.empty())
		{
			return defValue;
		}

		return boost::lexical_cast<T>(value);
	}

	int CloudConfigurationUpdater::updateCloudData()
	{
		m_logger.debug("CloudConfigurationUpdater::updateCloudData()");
		if(0 != _updateMQDef())
			return -1;

		if(0 != _updateMQRela())
			return -1;

		if(0 != _updateMQService())
			return -1;

		if(0 !=_updateMQTopic())
			return -1;

		if (0 != _updateChunkInfoTask())
			return -1;
		
		if (0 != _updateProgramTask())
			return -1;

		if (0 != _updateRegularTask())
			return -1;

		if (0 != _updateCrontabTask())
			return -1;
		
		return 0;
	}

	////mq_id|mq名称|mq描述|Broker1信息|Broker2信息|创建时间|变更时间|保留1
	int CloudConfigurationUpdater::_updateMQDef()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateMQDef()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_MQ_MQ);

		Json::Value root;
		Json::Reader jsonReader;
		
		try
		{
			if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
			{
				bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
				zNodePath);
				return -1;
			}
			
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaMQDef> vecMemMQDef;
			MetaMQDefOp *p = dynamic_cast<MetaMQDefOp *>(
			                          m_ctlOper->getObjectPtr(MetaMQDefOp::getObjName()));

			if (!p->getAllMQDef(vecMemMQDef))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaMQDef definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemMQDef.size());
			for(std::vector<MetaMQDef>::iterator itMQDef = vecMemMQDef.begin();
				itMQDef != vecMemMQDef.end();itMQDef++)
			{
				std::string sTmp = Bmco::format("%s|%s|%s|%s|%s|%?d|",
					std::string(itMQDef->mq_id.c_str()),std::string(itMQDef->mq_name.c_str()),std::string(itMQDef->mq_desc.c_str()),
					std::string(itMQDef->Broker_list1.c_str()),std::string(itMQDef->Broker_list2.c_str()),itMQDef->Create_date);
				sTmp += Bmco::format("%?d",itMQDef->Modi_date);

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			std::vector<MetaMQDef> vecMQDef;
			vecMQDef.clear();
			Timestamp now;
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("mq_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("mq_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("mq_desc")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_desc",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("broker_list1")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no broker_list1",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("mq_type")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_type",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}

				MetaMQDef tmp = MetaMQDef(value["mq_id"].asString().c_str(),
											value["mq_name"].asString().c_str(),
											value["mq_desc"].asString().c_str(),
											value["broker_list1"].asString().c_str(),
											setDefaultValue(value["broker_list2"].asString(), std::string("")).c_str(),
											setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)),
											setDefaultValue(value["modi_date"].asString(), Bmco::UInt64(0)),
											setDefaultValue(value["reserve1"].asString(), std::string("")).c_str(),
											setDefaultValue(value["reserve2"].asString(), std::string("")).c_str(),
											boost::lexical_cast<Bmco::UInt16>(value["mq_type"].asString()));
				vecMQDef.push_back(tmp);
			}

			if (!p->replace(vecMQDef))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaMQDef.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncMQDef!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateMQDef end!");			
		return 0;

	}

	////序列ID|流程ID|进程名|实例ID|BOL名称|输入PARTITION名称|输入Topic名称|输入MQ名称
	////|输出PARTITION名称|输出Topic名称|输出MQ名称|状态|创建时间|变更时间|子系统名|域名|工号|保留1|保留2
	int CloudConfigurationUpdater::_updateMQRela()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateMQRela()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_MQ_RELAION);

		Json::Value root;
		Json::Reader jsonReader;

		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaMQRela> vecMemMQRela;
			MetaMQRelaOp *p = dynamic_cast<MetaMQRelaOp *>(
			                          m_ctlOper->getObjectPtr(MetaMQRelaOp::getObjName()));

			if (!p->getAllMQRela(vecMemMQRela))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaMQRela definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemMQRela.size());
			for(std::vector<MetaMQRela>::iterator itMQ = vecMemMQRela.begin();
				itMQ != vecMemMQRela.end();itMQ++)
			{
				std::string sTmp = Bmco::format("%?d|%s|%s|%?d|%s|%s|",
					itMQ->SEQ_ID,std::string(itMQ->Flow_ID.c_str()),std::string(itMQ->PROCESS_NAME.c_str()),
					itMQ->Instance_id,std::string(itMQ->BOL_CLOUD_NAME.c_str()),std::string(itMQ->IN_PARTITION_NAME.c_str()));
				sTmp += Bmco::format("%s|%s|%s|%s|%s|?d",std::string(itMQ->IN_TOPIC_NAME.c_str()),std::string(itMQ->IN_MQ_NAME.c_str()),
					std::string(itMQ->OUT_PARTITION_NAME.c_str()),std::string(itMQ->OUT_TOPIC_NAME.c_str()),std::string(itMQ->OUT_MQ_NAME.c_str()),itMQ->STATUS);
				sTmp += Bmco::format("%?d|%?d|%s|%s|%s",itMQ->Create_date,itMQ->Modi_date,
					std::string(itMQ->subsys_type.c_str()),std::string(itMQ->domain_type.c_str()),std::string(itMQ->OPERATOR_id.c_str()));

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			
			std::vector<MetaMQRela> vecMQRela;
			vecMQRela.clear();
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("seq_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no seq_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("flow_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no flow_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("process_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no process_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("instance_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no instance_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("bol_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no bol_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("out_partition_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no out_partition_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("out_topic_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no out_topic_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("out_mq_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no out_mq_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("status")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no status",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				MetaMQRela tmp = MetaMQRela(boost::lexical_cast<Bmco::UInt32>(value["seq_id"].asString()),
											value["flow_id"].asString().c_str(),
											value["process_name"].asString().c_str(),
											boost::lexical_cast<Bmco::UInt16>(value["instance_id"].asString()),
											value["bol_name"].asString().c_str(),
											setDefaultValue(value["in_partition_name"].asString(), std::string("")).c_str(),
											setDefaultValue(value["in_topic_name"].asString(), std::string("")).c_str(),
											setDefaultValue(value["in_mq_name"].asString(), std::string("")).c_str(),
											setDefaultValue(value["out_partition_name"].asString(), std::string("")).c_str(),
											value["out_topic_name"].asString().c_str(),
											value["out_mq_name"].asString().c_str(),
											boost::lexical_cast<Bmco::UInt32>(value["status"].asString()),
											setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)),
											setDefaultValue(value["modi_date"].asString(), Bmco::UInt64(0)),
											setDefaultValue(value["subsys_type"].asString(), std::string("")).c_str(),
											setDefaultValue(value["domain_type"].asString(), std::string("")).c_str(),
											setDefaultValue(value["operator_id"].asString(), std::string("")).c_str());
				vecMQRela.push_back(tmp);
			}

			if (!p->replace(vecMQRela))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaMQRela.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncRela!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
			
		bmco_information(m_logger, "_updateMQRela end!");			
		return 0;

		}


	////Service名称|Service描述|Topic名称|MQ名称|状态|创建时间|变更时间|保留1|保留2
	int CloudConfigurationUpdater::_updateMQService()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateMQService()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_MQ_SERVICE);

		Json::Value root;
		Json::Reader jsonReader;

		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaMQService> vecMemMQService;
			MetaMQServiceOp *p = dynamic_cast<MetaMQServiceOp *>(
			                          m_ctlOper->getObjectPtr(MetaMQServiceOp::getObjName()));

			if (!p->getAllMQService(vecMemMQService))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaMQService definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemMQService.size());
			for(std::vector<MetaMQService>::iterator itMQ = vecMemMQService.begin();
				itMQ != vecMemMQService.end();itMQ++)
			{
				std::string sTmp = Bmco::format("%s|%s|%s|%s|%?d|%?d|",
					std::string(itMQ->Service_name.c_str()),std::string(itMQ->Service_desc.c_str()),std::string(itMQ->Topic_name.c_str()),
					std::string(itMQ->MQ_NAME.c_str()),itMQ->STATUS,itMQ->Create_date);

				sTmp += Bmco::format("%?d",itMQ->Modi_date);

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			
			std::vector<MetaMQService> vecMQService;
			vecMQService.clear();
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("service_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no service_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("topic_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no topic_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("mq_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("status")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no status",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				
				MetaMQService tmp = MetaMQService(value["service_name"].asString().c_str(),
													setDefaultValue(value["service_desc"].asString(), std::string("")).c_str(),
													value["topic_name"].asString().c_str(),
													value["mq_name"].asString().c_str(),
													boost::lexical_cast<Bmco::UInt32>(value["status"].asString()),
													setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)),
													setDefaultValue(value["modi_date"].asString(), Bmco::UInt64(0)));
				vecMQService.push_back(tmp);
			}

			if (!p->replace(vecMQService))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaMQService.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncService!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateMQService end!");			
		return 0;

	}

	////STopic_id|Topic名称|Topic描述|优先级|Partition个数|状态|MQ名称|创建时间|变更时间|保留1|保留2
	int CloudConfigurationUpdater::_updateMQTopic()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateMQTopic()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_MQ_TOPIC);
		
		Json::Value root;
		Json::Reader jsonReader;

		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaMQTopic> vecMemMQTopic;
			MetaMQTopicOp *p = dynamic_cast<MetaMQTopicOp *>(
			                          m_ctlOper->getObjectPtr(MetaMQTopicOp::getObjName()));

			if (!p->getAllMQTopic(vecMemMQTopic))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaMQTopic definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemMQTopic.size());
			for(std::vector<MetaMQTopic>::iterator itMQ = vecMemMQTopic.begin();
				itMQ != vecMemMQTopic.end();itMQ++)
			{
				std::string sTmp = Bmco::format("%s|%s|%s|%?d|%?d|%?d|",
					std::string(itMQ->Topic_id.c_str()),std::string(itMQ->Topic_name.c_str()),std::string(itMQ->Topic_desc.c_str()),
					itMQ->Priority,itMQ->Partition_number,itMQ->STATUS);

				sTmp += Bmco::format("%s|%s|%s|%?d|%?d",std::string(itMQ->MQ_NAME.c_str()),
					std::string(itMQ->subsys_type.c_str()),
					std::string(itMQ->domain_type.c_str()),
					itMQ->Create_date,itMQ->Modi_date);

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			
			std::vector<MetaMQTopic> vecMQTopic;
			vecMQTopic.clear();
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("topic_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no topic_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("topic_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no topic_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("partition_number")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no partition_number",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("status")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no status",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("mq_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no mq_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}

				MetaMQTopic tmp = MetaMQTopic(value["topic_id"].asString().c_str(),
												value["topic_name"].asString().c_str(),
												value["topic_desc"].asString().c_str(),
												setDefaultValue(value["priority"].asString(), Bmco::UInt32(1)),
												boost::lexical_cast<Bmco::UInt32>(value["partition_number"].asString()),
												boost::lexical_cast<Bmco::UInt32>(value["status"].asString()),
												value["mq_name"].asString().c_str(),
												setDefaultValue(value["subsys_type"].asString(), std::string("")).c_str(),
												setDefaultValue(value["domain_type"].asString(), std::string("")).c_str(),
												setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)),
												setDefaultValue(value["modi_date"].asString(), Bmco::UInt64(0)));
				vecMQTopic.push_back(tmp);
			}

			if (!p->replace(vecMQTopic))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaMQService.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncTopic!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateMQTopic end!");			
		return 0;
	}

	bool CloudConfigurationUpdater::_updateXMLDictFile()
	{
		std::string zDCNodePath(ZNODE_ZOOKEEPER_DICT);
		std::string zSRNodePath(ZNODE_ZOOKEEPER_SRDICT);
		// xml文件流
		std::string SRFileContent; 

		Json::Value root;
		Json::Reader jsonReader;
		
		if (!CloudDatabaseHandler::instance()->nodeExist(zSRNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zDCNodePath);
			return false;
		}

		if (!CloudDatabaseHandler::instance()->nodeExist(zDCNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zSRNodePath);
			return false;
		}

		// 配置的固定目录
		std::string dictConfigStr;

		try
		{
			// 获取记录成xml文件的路径
			CloudDatabaseHandler::instance()->getNodeData(zDCNodePath, dictConfigStr);
			// 获取XML文件流
			CloudDatabaseHandler::instance()->getNodeData(zSRNodePath, SRFileContent);

			if (!jsonReader.parse(dictConfigStr, root)) 
			{
				return false;
			}

			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return false;
			}

			Json::Value valueArray = root["record"];
			std::string fileName = "";
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no id",std::string("0"),std::string(__FUNCTION__));
					return false;
				}
				if (!value.isMember("key")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no key",std::string("0"),std::string(__FUNCTION__));
					return false;
				}
				if (!value.isMember("value")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no value",std::string("0"),std::string(__FUNCTION__));
					return false;
				}

				if ((value["id"].asString() == "1001")
					&& (value["key"].asString() == "XMLDictFilePath"))
				{
					fileName = value["value"].asString();
					break;
				}
			}
			
			if (fileName.empty())
			{
				bmco_error_f2(m_logger,"%s|%s|no confid 1001 / XMLDictFilePath is null", 
					std::string("0"), std::string(__FUNCTION__));
				return false;
			}
			
			std::string file(fileName);
			File delFile(file);
			if (delFile.exists())
			{
				delFile.remove();
			}
			
			Bmco::FileOutputStream fos(file, std::ios::binary);
			// 将有变化的数据重新生成xml文件
			fos << SRFileContent;
			fos.close();
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return false;
		}	
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return false;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return false;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncXML!",
									std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	bool CloudConfigurationUpdater::notifyChangedToBolProcess
		(Bmco::UInt64 iBpcbID, Bmco::UInt32 iAction)
	{
		MetaBpcbInfoOp* bpcbPtr = dynamic_cast<MetaBpcbInfoOp*>
			(m_ctlOper->getObjectPtr(MetaBpcbInfoOp::getObjName()));

		if (!bpcbPtr->updateActionByBpcbID(iBpcbID, 
				iAction))
		{
			bmco_error_f2(m_logger,"%s|%s|updateActionByBpcbID failed!",std::string("0"),std::string(__FUNCTION__));
			return false;
		}

		return true;
	}

	int CloudConfigurationUpdater::_updateCrontabTask()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateCrontabTask()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_CLUSTER);
		zNodePath.append("/");
		zNodePath.append(m_bolName);
		zNodePath.append("/crontab");
		
		Json::Value root;
		Json::Reader jsonReader;

		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaBolCrontab> vecMemCrontab;
			MetaBolCrontabOp *p = dynamic_cast<MetaBolCrontabOp *>(
			                          m_ctlOper->getObjectPtr(MetaBolCrontabOp::getObjName()));

			if (!p->QueryAll(vecMemCrontab))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaBolCrontab definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemCrontab.size());
			for(std::vector<MetaBolCrontab>::iterator itCrontab = vecMemCrontab.begin();
				itCrontab != vecMemCrontab.end();itCrontab++)
			{
				std::string sTmp = Bmco::format("%?d|%s|%?d|%?d|%?d|%?d|",
					itCrontab->ID,m_bolName,itCrontab->ProgramID,itCrontab->Minute,
					itCrontab->Hour,itCrontab->Day);
				sTmp += Bmco::format("%?d|%?d|%?d|%b||%?d",
					itCrontab->Month,itCrontab->Year,itCrontab->Week,itCrontab->IsValid,
					itCrontab->NextExecuteTime);

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换

			std::vector<MetaBolCrontab> vecCrontab;
			vecCrontab.clear();
			Timestamp now;
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("task_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no task_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("task_day")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no task_day",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("task_month")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no task_month",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				
				MetaBolCrontab tmp = MetaBolCrontab(boost::lexical_cast<Bmco::UInt32>(value["task_id"].asString()),
													setDefaultValue(value["task_minute"].asString(), Bmco::UInt32(0)),
													setDefaultValue(value["task_hour"].asString(), Bmco::UInt32(0)),
													boost::lexical_cast<Bmco::UInt32>(value["task_day"].asString()),
													boost::lexical_cast<Bmco::UInt32>(value["task_month"].asString()),
													setDefaultValue(value["task_year"].asString(), Bmco::UInt32(0)),
													setDefaultValue(value["task_week"].asString(), Bmco::UInt32(0)),
													setDefaultValue(value["program_id"].asString(), Bmco::UInt32(0)),
													setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)),
													(setDefaultValue(value["Is_Valid"].asString(), Bmco::UInt32(0)) == 0 ? false : true),
													setDefaultValue(value["next_exec_time"].asString(), Bmco::UInt64(0)));
				vecCrontab.push_back(tmp);
		    }

			if (!p->replace(vecCrontab))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaBolCrontab.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncService!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateCrontabTask end!");			
		return 0;
	}

	int CloudConfigurationUpdater::_updateRegularTask()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateRegularTask()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_CLUSTER);
		zNodePath.append("/");
		zNodePath.append(m_bolName);
		zNodePath.append("/regular");

		Json::Value root;
		Json::Reader jsonReader;

		std::stringstream stream;
    	
		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}
			
			///查询老数据并打印
			std::vector<MetaRegularProcessTask> vecMemRegular;
			MetaRegularProcessTaskOp *p = dynamic_cast<MetaRegularProcessTaskOp *>(
			                          m_ctlOper->getObjectPtr(MetaRegularProcessTaskOp::getObjName()));

			if (!p->QueryAll(vecMemRegular))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaBolCrontab definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemRegular.size());
			for(std::vector<MetaRegularProcessTask>::iterator itRegular = vecMemRegular.begin();
				itRegular != vecMemRegular.end();itRegular++)
			{
				std::string sTmp = Bmco::format("%?d|%s|%?d|%?d|%?d|",
					itRegular->ID,m_bolName,itRegular->ProgramID,itRegular->InstanceID,
					itRegular->FlowID);
				sTmp += Bmco::format("%b|%b|%?d|%b|%s",
					itRegular->IsValid,itRegular->IsCapValid,itRegular->Priority,itRegular->IsNoMonitor,
					std::string(itRegular->ExCommand.c_str()));

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			// bmco_information_f3(m_logger, "%s|%s|after update data record is %?d.",std::string("0"),std::string(__FUNCTION__),recordNum);
			std::vector<MetaRegularProcessTask> vecRegular;
			vecRegular.clear();
			Timestamp now;
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("task_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no task_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("program_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no program_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				MetaRegularProcessTask tmp = MetaRegularProcessTask(boost::lexical_cast<Bmco::UInt32>(value["task_id"].asString()),
													 				boost::lexical_cast<Bmco::UInt32>(value["program_id"].asString()),
																	setDefaultValue(value["instance_id"].asString(), Bmco::UInt32(0)),
																	setDefaultValue(value["flow_id"].asString(), Bmco::UInt32(0)),
													 				(setDefaultValue(value["is_valid"].asString(), Bmco::UInt32(1)) == 0 ? false : true),
													 				setDefaultValue(value["modify_date"].asString(), Bmco::UInt64(0)),
													 				false,
													 				0,
																	setDefaultValue(value["excommand"].asString(), std::string("")).c_str(),
																	// 默认监控
																	(setDefaultValue(value["is_monitor"].asString(), Bmco::UInt32(1)) == 1 ? false : true));
				vecRegular.push_back(tmp);

		    }

			if (!p->replace(vecRegular))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaBolCrontab.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncService!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateRegularTask end!");			
		return 0;
	}

	void CloudConfigurationUpdater::replaceStr(std::string &sourceStr, 
		std::string &oldStr, std::string &newStr)
	{
	    std::string::size_type iPos = 0;
	    iPos = sourceStr.find(oldStr, iPos);
		while (iPos != std::string::npos)
		{
			sourceStr.replace(iPos, oldStr.size(), newStr);
			iPos = sourceStr.find(oldStr, iPos);
		}
	}

	int CloudConfigurationUpdater::_updateProgramTask()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateProgramTask()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_CLUSTER);
		//zNodePath.append("/");
		//zNodePath.append(m_bolName);
		zNodePath.append("/programdef");

		Json::Value root;
		Json::Reader jsonReader;

		std::stringstream stream;
    	
		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaProgramDef> vecMemProgram;
			MetaProgramDefOp *p = dynamic_cast<MetaProgramDefOp *>(
			                          m_ctlOper->getObjectPtr(MetaProgramDefOp::getObjName()));

			if (!p->queryAll(vecMemProgram))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaBolCrontab definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemProgram.size());
			for(std::vector<MetaProgramDef>::iterator itProgram = vecMemProgram.begin();
				itProgram != vecMemProgram.end();itProgram++)
			{
				std::string sTmp = Bmco::format("%?d|%s|%?d|%?d|",
					itProgram->ID,m_bolName,itProgram->MaxInstance,itProgram->MinInstance);
				sTmp += Bmco::format("%s|%s|%s|%s|%?d",
					std::string(itProgram->ExeName.c_str()),std::string(itProgram->StaticParams.c_str()), 
					std::string(itProgram->DynamicParams.c_str()), std::string(itProgram->DisplayName.c_str()),
					itProgram->Priority);

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			// bmco_information_f3(m_logger, "%s|%s|after update data record is %?d.",std::string("0"),std::string(__FUNCTION__),recordNum);
			std::vector<MetaProgramDef> vecProgram;
			vecProgram.clear();
			Timestamp now;
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
		    	std::string sourceStr = setDefaultValue(value["static_params"].asString(), std::string(""));
				std::string oldStr = "${common.confpath}";

				// 替代特殊符号，表示运行路径
				replaceStr(sourceStr, oldStr, m_confPath);
				if (!value.isMember("program_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no program_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("exe_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no exe_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}

				MetaProgramDef tmp = MetaProgramDef(boost::lexical_cast<Bmco::UInt32>(value["program_id"].asString()),
													setDefaultValue(value["max_instance"].asString(), Bmco::UInt32(1)),
													setDefaultValue(value["min_instance"].asString(), Bmco::UInt32(1)),
													setDefaultValue(value["process_type"].asString(), Bmco::UInt32(1)),
													setDefaultValue(value["life_cycle_type"].asString(), Bmco::UInt32(1)),
													setDefaultValue(value["business_type"].asString(), Bmco::UInt32(1)),
													setDefaultValue(value["bol_bpcbid"].asString(), Bmco::UInt32(0)),
													value["exe_name"].asString().c_str(),
													sourceStr.c_str(),
													setDefaultValue(value["dynamic_params"].asString(), std::string("")).c_str(),
													setDefaultValue(value["display_name"].asString(), std::string("")).c_str(),
													now,
													setDefaultValue(value["priority"].asString(), Bmco::UInt32(1)));
				vecProgram.push_back(tmp);
		    }

			if (!p->replace(vecProgram))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaBolCrontab.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncService!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateProgramTask end!");			
		return 0;
	}

	int CloudConfigurationUpdater::_updateChunkInfoTask()
	{
		bmco_information(m_logger, "CloudConfigurationUpdater::_updateChunkInfoTask()");

		std::string cloudConfigStr;
        std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_CLUSTER);
		zNodePath.append("/");
		zNodePath.append(m_bolName);
		zNodePath.append("/chunk");

		Json::Value root;
		Json::Reader jsonReader;

		if (!CloudDatabaseHandler::instance()->nodeExist(zNodePath))
		{
			bmco_error_f3(m_logger,"%s|%s|node:%s does not exist",std::string("0"),std::string(__FUNCTION__),
			zNodePath);
			return -1;
		}

		try
		{
			CloudDatabaseHandler::instance()->getNodeData(zNodePath, cloudConfigStr);

			if (!jsonReader.parse(cloudConfigStr, root)) 
			{
				return -1;
			}

			///查询老数据并打印
			std::vector<MetaChunkInfo> vecMemChunkInfo;
			MetaChunkInfoOp *p = dynamic_cast<MetaChunkInfoOp *>(
			                          m_ctlOper->getObjectPtr(MetaChunkInfoOp::getObjName()));

			if (!p->Query(vecMemChunkInfo))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to query MetaBolCrontab definition.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
			
			bmco_information_f3(m_logger, "%s|%s|before update data record is %?d.",std::string("0"),std::string(__FUNCTION__),vecMemChunkInfo.size());
			for(std::vector<MetaChunkInfo>::iterator itChunk = vecMemChunkInfo.begin();
				itChunk != vecMemChunkInfo.end();itChunk++)
			{
				std::string sTmp = Bmco::format("%?d|%s|%?d|%?d|%s|%b|",
					itChunk->id, m_bolName, itChunk->regionId, itChunk->bytes,
					std::string(itChunk->name.c_str()), itChunk->isGrow);
				sTmp += Bmco::format("%b|%b|%b|%s|%s",
					itChunk->isShrink,itChunk->isMappedFile,itChunk->isSnapShot,std::string(itChunk->lockName.c_str()),
					std::string(itChunk->owner.c_str()));

				bmco_information_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),sTmp);
			}

			///取zookeper数据并进行替换
			// bmco_information_f3(m_logger, "%s|%s|after update data record is %?d.",std::string("0"),std::string(__FUNCTION__),recordNum);
			std::vector<MetaChunkInfo> vecChunkInfo;
			vecChunkInfo.clear();
			Timestamp now;
			if (!root.isMember("record")) 
			{
				bmco_error_f2(m_logger, "%s|%s|no record",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}

			Json::Value valueArray = root["record"];
			for (int index = 0; index < valueArray.size(); ++index) 
			{
				Json::Value value = valueArray[index];
				if (!value.isMember("chunk_id")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no chunk_id",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("chunk_name")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no chunk_name",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				if (!value.isMember("chunk_owner")) 
				{
					bmco_error_f2(m_logger, "%s|%s|no chunk_owner",std::string("0"),std::string(__FUNCTION__));
					return -1;
				}
				MetaChunkInfo tmp = MetaChunkInfo(boost::lexical_cast<Bmco::UInt32>(value["chunk_id"].asString()),
												  setDefaultValue(value["region_id"].asString(), Bmco::UInt16(0)),
												  setDefaultValue(value["chunk_size"].asString(), Bmco::UInt64(0)),
 												  value["chunk_name"].asString().c_str(),
 												  (setDefaultValue(value["is_grow"].asString(), Bmco::UInt32(0)) == 0 ? false : true),
 												  (setDefaultValue(value["is_shrink"].asString(), Bmco::UInt32(0)) == 0 ? false : true),
 												  (setDefaultValue(value["is_mappedfile"].asString(), Bmco::UInt32(0)) == 0 ? false : true),
 												  (setDefaultValue(value["is_snapshot"].asString(), Bmco::UInt32(0)) == 0 ? false : true),
 												  setDefaultValue(value["lockname"].asString(), std::string("")).c_str(),
 												  value["chunk_owner"].asString().c_str(),
 												  STATE_NOTHING,
 												  setDefaultValue(value["create_date"].asString(), Bmco::UInt64(0)));
				vecChunkInfo.push_back(tmp);

		    }

			if (!p->replace(vecChunkInfo))
			{
				bmco_error_f2(m_logger, "%s|%s|Failed to execute  replace for MetaChunkInfoTable.",std::string("0"),std::string(__FUNCTION__));
				return -1;
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(Bmco::Exception &e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),e.displayText());
			return -1;
		}
		catch(std::exception& e)
		{
			bmco_error_f3(m_logger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return -1;
		}
		catch(...)
		{
			bmco_error_f2(m_logger,"%s|%s|unknown exception occured when syncService!",
									std::string("0"),std::string(__FUNCTION__));
			return -1;
		}
		
		bmco_information(m_logger, "_updateChunkInfoTask end!");			
		return 0;
	}
}

