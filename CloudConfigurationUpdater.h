#ifndef _CLOUDCONFIGURATIONUPDATER_HH__
#define _CLOUDCONFIGURATIONUPDATER_HH__
#include <sstream>
#include <set>
#include <string>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/date_time.hpp>
#include <boost/exception/all.hpp>

#include "BolCommon/ControlRegionOp.h"
#include "Bmco/Util/Application.h"
#include "CloudDatabaseHandler.h"
//using boost::multi_index_container;
//using namespace boost::multi_index;	
//namespace BMI = boost::multi_index;
namespace BM35 {
const std::string TABLE_NODE_INFO = "c_cfg_node_info";
const std::string TABLE_BUSINESS_FLOW = "c_cfg_business_flow";
const std::string TABLE_PROGRAM_DEF = "c_cfg_program_def";
const std::string TABLE_BOL_INFO = "c_cfg_bol_info";
const std::string TABLE_REGULAR_TASK = "c_cfg_regular_task";
const std::string TABLE_CRONTAB_TASK = "c_cfg_crontab_task";
const std::string TABLE_MASTER = "c_cfg_bol_master";
const std::string TABLE_CHANNEL = "c_cfg_channel";
const std::string TABLE_PREFIX_LIST[12] = {
	"",			//placeholder, align the table index
	"Table.Table.1", //c_cfg_node_info
	"Table.Table.2", //c_cfg_business_flow
	"Table.Table.3", //c_cfg_program_def
	"Table.Table.4", //c_cfg_bol_info
	"Table.Table.5", //c_cfg_regular_task
	"Table.Table.6", //c_cfg_crontab_task
	"Table.Table.7", //c_cfg_bol_master
	"Table.Table.8", //c_cfg_channel
    "Table.Table.9", //c_cfg_build_env
    "Table.Table.10", //c_cfg_node_user
    "Table.Table.11", //c_cfg_rule
};

// class Bmco::Util::AbstractConfiguration;
//This class get configuration data stream and parse it.
//Then it returns a struct for doing BOL configuration update.
//It also maintains the sequence for updating all or incremental configuration.
class CloudConfigurationUpdater {
public:
	CloudConfigurationUpdater(ControlRegionOp::Ptr& ctlOper, std::string bolName, 
			std::string confPath)
		: m_seq(-1), m_ctlOper(ctlOper), m_logger(Bmco::Util::Application::instance().logger()),
		m_bolName(bolName), m_confPath(confPath) 
	{
	}
	~CloudConfigurationUpdater() 
	{
	}
	//! Read data from cloud and sync to share memory.
	int updateCloudData(/*std::istringstream &istr, int incFlag = 1*/);

	bool notifyChangedToBolProcess(Bmco::UInt64 iBpcbID, Bmco::UInt32 iAction);
	int _updateChunkInfoTask();
	int _updateProgramTask();
	int _updateRegularTask();
	int _updateCrontabTask();
	int _updateMQDef();
	int _updateMQRela();
	int _updateMQService();
	int _updateMQTopic();
	bool _updateXMLDictFile();
	void replaceStr(std::string &sourceStr,std::string &oldStr,std::string &newStr);
	template<typename T>
	T setDefaultValue(const std::string &value, T defValue);

private:
	int m_seq; //!< Record the last sequence. It uses this value to check if any changes are missed.
	std::string m_lastTime; //!< Record the last modified time of configuration data.
	ControlRegionOp::Ptr& m_ctlOper; //!< ControlRegion pointer for doing operation on it.
	Bmco::Logger &m_logger;
	std::string m_bolName;
	std::string m_bolId;
    BolStatus m_bolStatus;
	std::string m_confPath;
};
}
#endif //_CLOUDCONFIGURATIONUPDATER_HH__
