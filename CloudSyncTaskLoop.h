#ifndef __CLOUDSYNCTASKLOOP__HH__
#define __CLOUDSYNCTASKLOOP__HH__
#include <sstream>
#include "Bmco/Process.h"
#include "Bmco/Util/Application.h"
#include "Bmco/NumberFormatter.h"
#include "BolCommon/ControlRegionOp.h"
#include "BolCommon/ControlRegionBasedTask.h"
#include "CloudConfigurationUpdater.h"
#include "CloudDatabaseHandler.h"
#include "IniFileConfigurationNew.h"
#include "CloudAgentConstants.h"

namespace BM35 {
/// CloudAgent run time task name, it will be used as thread name by taskmanager
#define BOL_CLOUDSYNC_TASK_NAME  "Bol_Cloud_Sync_Task_Loop_Thread"


/// This class syncs configuration of BOL.
/// which will be run in a dedicated thread and managed by Bmco::TaskManager
class CloudSyncTaskLoop: public ControlRegionBasedTask<Bmco::Logger>
{
public:
 	CloudSyncTaskLoop(ControlRegionOp::Ptr& ptr): 
        ControlRegionBasedTask<Bmco::Logger>(BOL_CLOUDSYNC_TASK_NAME,ptr,Application::instance().logger()),
            m_config(Bmco::Util::Application::instance().config()) {}

	virtual ~CloudSyncTaskLoop(void)
	{
	}

	/// do check and initlization before task loop
	virtual bool beforeTask()
	{
		bmco_information(theLogger, "CloudSyncTaskLoop::beforeTask()");
		if (init()) 
		{
            return ControlRegionBasedTask<Bmco::Logger>::beforeTask();
        } 
		else 
		{
            return false;
        }
	}
					
	/// do uninitlization after task loop
	virtual bool afterTask()
	{
		bmco_information(theLogger, "CloudSyncTaskLoop::afterTask()");
		return true;
	}
	
	/// perform business logic
	virtual bool handleOneUnit()
	{	
		//do the monitoring and update operation
	    CloudConfigurationUpdater updater(ctlOper, m_bolName, m_confPath);

		bool isChanged = false;
	    if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_MQ_MQ, isChanged))
    	{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", ZNODE_ZOOKEEPER_MQ_MQ);
			return false;
    	}
		if (isChanged)
			updater._updateMQDef();

		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_MQ_TOPIC, isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", ZNODE_ZOOKEEPER_MQ_TOPIC);
			return false;
    	}
		if (isChanged)
			updater._updateMQTopic();

		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_MQ_SERVICE, isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", ZNODE_ZOOKEEPER_MQ_SERVICE);
			return false;
    	}
		if (isChanged)
			updater._updateMQService();
			
		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_MQ_RELAION, isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", ZNODE_ZOOKEEPER_MQ_RELAION);
			return false;
    	}
		if (isChanged)
			updater._updateMQRela();

		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(appendZKPathName("/chunk"), isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", appendZKPathName("/chunk"));
			return false;
    	}
		if (isChanged)
		{
			updater._updateChunkInfoTask();
			updater.notifyChangedToBolProcess(BOL_PROCESS_MEMMANAGER,
				PROCESS_ACTION_PARAM_REFRESH);
		}

		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag((ZNODE_ZOOKEEPER_CLUSTER + "/programdef"), isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", (ZNODE_ZOOKEEPER_CLUSTER + "/programdef"));
			return false;
    	}
		if (isChanged)
		{
			updater._updateProgramTask();
			updater.notifyChangedToBolProcess(BOL_PROCESS_PROCESSMGR, 
				PROCESS_ACTION_PARAM_REFRESH);
		}

		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(appendZKPathName("/regular"), isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", appendZKPathName("/regular"));
			return false;
    	}
		if (isChanged)
		{
			updater._updateRegularTask();
			updater.notifyChangedToBolProcess(BOL_PROCESS_PROCESSMGR, 
				PROCESS_ACTION_PARAM_REFRESH);
		}
		
		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(appendZKPathName("/crontab"), isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", appendZKPathName("/crontab"));
			return false;
    	}
		if (isChanged)
			updater._updateCrontabTask();

		// 字典管理监控
		if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_SRDICT, isChanged))
		{
    		bmco_error_f1(theLogger, "getNodeChangedFlag %s failed!", ZNODE_ZOOKEEPER_SRDICT);
			return false;
    	}
		if (isChanged)
			updater._updateXMLDictFile();

		//refreshLogLevel();	//响应日志等级变更
		this->sleep(3000);
		return true;
	}

	std::string appendZKPathName(std::string childPath)
	{
		std::string zNodePath;
		zNodePath.assign(ZNODE_ZOOKEEPER_CLUSTER);
		zNodePath.append("/");
		zNodePath.append(m_bolName);
		zNodePath.append(childPath);
		return zNodePath;
	}

protected:

	//! Doing initialization.
	bool init() {
        bmco_information(theLogger, "CloudSyncTaskLoop::init()");
        m_bolName = m_config.getString(CONFIG_INFO_BOL_NAME);
		m_confPath = m_config.getString("common.confpath");
		return true;
	}

	//! refresh #0 log level
	void refreshLogLevel() {
		MetaBolConfigOp* c = dynamic_cast<MetaBolConfigOp*>(ctlOper->getObjectPtr(MetaBolConfigOp::getObjName()));
		assert (NULL != c);
		MetaBolConfigInfo::Ptr cfgpayload(new MetaBolConfigInfo("logging.logger.CloudAgent.level"));
		if(c->Query(cfgpayload)) {
			theLogger.setLevel(cfgpayload->_strval.c_str());
		} else {
			theLogger.setLevel("warning");
		}
	}

private:
	std::string m_bolName;
	std::string m_confPath;
    Bmco::Util::AbstractConfiguration &m_config;
};

} //namespace BM35 

#endif //__CLOUDSYNCTASKLOOP__HH__

