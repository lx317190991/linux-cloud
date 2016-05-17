#ifndef __CLOUDAGENTHEARTBEATLOOP_HH__
#define __CLOUDAGENTHEARTBEATLOOP_HH__
#include "Bmco/Task.h"
#include "Bmco/Util/Application.h"
#include "BolCommon/ControlRegionOp.h"
#include "Bmco/DateTimeFormatter.h"
#include "BolCommon/ControlRegionBasedTask.h"
using Bmco::Util::Application;
using Bmco::DateTimeFormatter;
namespace BM35 {
/// run time task name, it will be used as thread name by taskmanager
static const char BOL_CLOUDAGENT_HB_TASK_NAME[] = "Bol_CloudAgent_HeartBeat_Thread";

/// This class keep heartbeat 
/// it will be run in a dedicated thread and managed by Bmco::TaskManager
class CloudAgentHeartBeatLoop: public ControlRegionBasedTask<Bmco::Logger>
{
public:
    CloudAgentHeartBeatLoop(ControlRegionOp::Ptr& ptr): 
		ControlRegionBasedTask<Bmco::Logger>(BOL_CLOUDAGENT_HB_TASK_NAME,ptr,Application::instance().logger())
	{}
	virtual ~CloudAgentHeartBeatLoop(void)
	{
	}
	/// do check and initlization before task loop
	virtual bool beforeTask(){
		return ControlRegionBasedTask<Bmco::Logger>::beforeTask();
	}
	
	/// do uninitlization after task loop
	virtual bool afterTask(){
		return ControlRegionBasedTask<Bmco::Logger>::afterTask();
	}
    
	/// this function execute kernel runtime jobs periodically 
	/// in a dedicated task thead managed by taskmanager
	virtual bool handleOneUnit()
	{
		bmco_trace_f3(theLogger,"%s|%s|keeping heartbeating... %s",
			std::string("0"),std::string(__FUNCTION__),DateTimeFormatter::format(Application::instance().uptime()));
		MetaBpcbInfoOp* bpcbPtr = dynamic_cast<MetaBpcbInfoOp*>(ctlOper->getObjectPtr(MetaBpcbInfoOp::getObjName()));
		if(bpcbPtr == NULL)	{
			bmco_error_f2(theLogger,"%s|%s|getObjectPtr from MetaBpcbInfoOp error",std::string("0"),std::string(__FUNCTION__));
		}
		else {
			if(!bpcbPtr->updateHeartBeatByBpcbID(BOL_PROCESS_CLOUDAGENT)) {
				bmco_error_f2(theLogger,"%s|%s|Failed to execute updateHeartBeatByBpcbID on MetaShmBpcbInfoTable"
				,std::string("0"),std::string(__FUNCTION__));
			}
		}
		this->sleep(KERNEL_HB_INTERVAL);
		return true;
	}
private:
};
} //namespace BM35 
#endif//__CLOUDAGENTHEARTBEATLOOP_HH__
