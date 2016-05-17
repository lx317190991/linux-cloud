#ifndef __CLOUDDYNAMICFLEXTASKLOOP__HH__
#define __CLOUDDYNAMICFLEXTASKLOOP__HH__
#include <sstream>
#include "CloudKafkaMessageHandler.h"
#include "Bmco/Process.h"
#include "Bmco/Util/Application.h"
#include "Bmco/Data/SessionFactory.h"
#include "Bmco/Data/Session.h"
#include "Bmco/Data/RecordSet.h"
#include "Bmco/Data/Column.h"
#include "Bmco/Data/MySQL/Connector.h"
#include "IniFileConfigurationNew.h"
#include "Bmco/Util/IniFileConfiguration.h"
#include "Bmco/NumberFormatter.h"
#include "BolCommon/ControlRegionOp.h"
#include "BolCommon/ControlRegionBasedTask.h"
#include "BolCommon/MetaKPIInfoOp.h"
#include <boost/algorithm/string.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include "CloudConfigurationUpdater.h"
#include "CloudDatabaseHandler.h"
#include "CloudScheduleTaskLoop.h"
#include "CloudCmdTaskLoop.h"

using Bmco::StringTokenizer;
using namespace Bmco::Data;

namespace BM35
{
	const std::string TABLE_MQ_RELA = "c_info_mq_rela";
	const std::string TABLE_KPI_INFO = "c_info_kpi";

	typedef struct _relaInfo
	{
		std::string flowId;
		std::string processName;
		std::string bolName;
		std::string instanceId;
		std::string inTopicName;
		std::string outTopicName;
		std::string inPartitionName;
		std::string outPartitionName;
		std::string inMqName;
		std::string outMqName;
		std::string subsys;
		std::string domain;
	}relaInfo;

	typedef struct _bolInstance
	{
		std::string bolName;
		Bmco::UInt16 instanceId;
		bool operator < (const struct _bolInstance &r) const
		{
			return (instanceId < r.instanceId);
		}
	}bolInstance;

	enum directionMark
	{
		DM_Before = 0,
		DM_After = 1
	};

	typedef struct mqCalcOffsetDataStruct
	{
		double upStreamOffset;
		double downStreamOffset;
		mqCalcOffsetDataStruct()
		{
			upStreamOffset = 0.0;
			downStreamOffset = 0.0;
		}
	}mqCalcOffsetData;

	/// CloudAgent run time task name, it will be used as thread name by taskmanager
	#define BOL_CLOUDDYNAMICFLEX_TASK_NAME  "Bol_Cloud_DynamicFlex_Task_Loop_Thread"

	/// This class collect information and send them to Mysql.
	/// which will be run in a dedicated thread and managed by Bmco::TaskManager
	class CloudDynamicFlexTaskLoop: public ControlRegionBasedTask<Bmco::Logger>
	{
	public:
		CloudDynamicFlexTaskLoop(ControlRegionOp::Ptr& ptr, std::string bolName):
			ControlRegionBasedTask<Bmco::Logger>(BOL_CLOUDDYNAMICFLEX_TASK_NAME, ptr,
			                                     Bmco::Util::Application::instance().logger())
		{
			g_sessionData.setBolName(bolName);
			m_relaBuildVec.clear();
		}
		virtual ~CloudDynamicFlexTaskLoop(void)
		{
			m_relaBuildVec.clear();
			m_DefPtr = NULL;
		}
		/// do check and initlization before task loop
		virtual bool beforeTask()
		{
			bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::beforeTask", std::string("0"),std::string(__FUNCTION__));
			if (!init())
			{
				return false;
			}
			else
			{
				bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::recordBOLInfo", std::string("0"),std::string(__FUNCTION__));
				recordBOLInfo();
				bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::recordFlexInfo", std::string("0"),std::string(__FUNCTION__));
				recordFlexInfo();
				bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::recordParentKPIPath", std::string("0"),std::string(__FUNCTION__));
				recordParentKPIPath();
				bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::recordDictInfo", std::string("0"),std::string(__FUNCTION__));
				recordDictInfo();
				bmco_debug_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::beforeTask", std::string("0"),std::string(__FUNCTION__));
				return true;
			}
		}


		// do uninitlization after task loop
		virtual bool afterTask()
		{
			theLogger.debug("CloudDynamicFlexTaskLoop::afterTask()");
			return true;
		}

		// perform business logic
		virtual bool handleOneUnit()
		{
			// bmco_information_f2(theLogger, "%s|%s|CloudDynamicFlexTaskLoop::handleOneUnit", std::string("0"),std::string(__FUNCTION__));
			bool nodeChanged = false;
			bool bolChanged = false;
		    if (!CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_CLUSTER+"/node", nodeChanged)
				|| !CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_CLUSTER+"/bolcfg", bolChanged))
	    	{
	    		bmco_error_f2(theLogger, "%s|%s|node or bolcfg getNodeChangedFlag failed!", 
					std::string("0"),std::string(__FUNCTION__));
				return false;
	    	}

			if (nodeChanged || bolChanged)
				recordBOLInfo();

			// Master的工作
			if (!isMasterCloudAgent())
			{
				this->sleep(4000);
				return true;
			}

			bool isChanged = false;
		    CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_FlEX, isChanged);

			if (isChanged)
				recordFlexInfo();

		    // CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_STATUS_KPI, isChanged);

			// if (isChanged)
			// recordParentKPIPath();
			
		    CloudDatabaseHandler::instance()->getNodeChangedFlag(ZNODE_ZOOKEEPER_DICT, isChanged);

			if (isChanged)
				recordDictInfo();

			recordParentKPIPath();

			// test
			if (m_testControl)
			{
				doRuleCheckDynamicFlex();
			}
			
			this->sleep(10000);
			return true;
		}

	protected:

		//! Doing initialization.
		bool init()
		{
			m_needExtendFlex = false;
			m_needShrinkFlex = false;
			m_testControl = true;
			m_topicInfoMap.clear();
			m_relaInfoVec.clear();
			m_DefPtr = dynamic_cast<MetaMQDefOp*>(ctlOper->getObjectPtr(MetaMQDefOp::getObjName()));
			return true;
		}

		// 记录本机bol名称
		// void recordHostBaseInfo();

		// 记录kpi路径下的每个bol的信息
		void recordParentKPIPath();

		// 记录按bol划分的bol信息
		void recordKPIInfo(std::string bolName, std::vector<struct kpiinfo> &tmpVec);

		// 记录bol相关信息
		void recordBOLInfo();

		// 记录zookeeper规则表信息
		void recordFlexInfo();

		// void recordMQDefInfo();

		void recordDictInfo();

		bool recordTopicInfo(std::string currTopicName, 
			std::string &domainName);

		bool recordRelaInfo();

		// 判断当前的CloudAgent是否执行Master工作
		bool isMasterCloudAgent();

		// 上报Master的信息
		bool refreshMasterInfo(std::string MasterBol);

		// 判断本主题除了该bol处理是否还有其他bol处理
		bool isTheOnlyBolForTheTopic(std::string topicName,
			std::string bolName);

		// 动态伸缩入口
		void doRuleCheckDynamicFlex();

		// 做n+1的操作判断接口
		void doExtendOperationOrNot(std::string topicName, std::string code, 
					std::string interval, std::string value, 
					std::map<std::string, Bmco::Int64> topicMessageNumMap);

		// 做n-1的操作判断接口
		void doShrinkOperationOrNot(std::string topicName, std::string code, 
			std::string interval, std::string value, 
			std::map<std::string, Bmco::Int64> topicMessageNumMap);

		// 选择一个bol为伸展操作的启动
		bool chooseSuitAbleBolForLaunch(std::string domainName, std::string &launchBol);

		// 执行n+1操作
		bool extendPartitionAndBolFlow(std::string topicName);

		// 获取首次启动的bol对应进程的实例号
		bool getFirstStartInstanceId(std::string processName, 
				Bmco::UInt32 &instanceId);

		// 启动bol的进程实例
		bool launchInstanceReq(std::string bolName,std::string processName);

		// 修改topic定义表
		bool modifyMQTopicTable();

		// 修改消息队列关系表
		bool modifyMQRelaTable();

		// 实时设置topic的分区数
		bool setPartitionCount(std::string topicName, 
				const Bmco::UInt32 &partitionNum);

		// 实时获取topic的分区数
		bool getPartitionCount(std::string topicName, 
				Bmco::UInt32 &partitionNum);

		// 建立新增bol之后的队列关系
		bool buildRelationAfterNewBol(std::string topicName);

		// 建立新增bol之前的队列关系
		bool buildRelationBeforeNewBol(std::string topicName);

		// 执行n-1操作
		bool shrinkPartitionAndBolFlow(std::string topicName);

		// 选择一个负载最少的实例
		bool chooseInstanceBuildRela(std::string topicName, 
			bolInstance &ansBolInstance, enum directionMark mark);

		// 选择一个合适的bol停掉
		bool chooseSuitAbleBolForStop(std::string topicName, 
			std::string &ShrinkBol);

		// 设置云管理范围内bol状态
		bool setRemoteBolStatus(std::string bolName, BolStatus status);

		// 在设置的监控时间内是否所有的HLA进程都已经停掉
		bool hasStoppedALLHLAProcessTimeOut(std::string ShrinkBol);

		// 检查HLA进程是否都已经停掉
		bool defineHLAProcessShutdownAlready(std::string bolName);

		// 将消息队列关系指定的seqNo置为无效
		bool setBolMessageRelaInvalid(std::vector<Bmco::UInt32> seqNoVec);

		// 设置n-1后生产消费新的挂载点
		bool setRelaWithConsumeBolInstance(std::string topicName, 
			std::string ShrinkBol);

		// 启动bol节点
		bool launchBolNode(const std::string bolName);

		// 收集topic及对应的堆积消息数
		bool accumulateMessageNum(const Bmco::Timespan &intervalTime,
			std::map<std::string, Bmco::Int64> &TopicAccumulateNumVec,
			std::map<std::string, mqCalcOffsetData> &TopicUpDownOffsetVec);

		bool getMessageAccumuNumByPatition(std::string inTopicName,
			std::string inPartitionName, std::string inMqName,
			std::string outTopicName, std::string outPartitionName,
			std::string outMqName, Bmco::Int64 &accumulateNum, 
			mqCalcOffsetData &result);
		
		// 宕机处理逻辑操作
		void doBrokenDownBolOperation();

		// 检查是否存在宕机的bol
		bool checkIfExistBolBrokenDown(std::string &brokenBolName);

		void refreshKPIInfoByMaster(std::map<std::string, Bmco::Int64> topicMessageNumMap, 
			std::map<std::string, mqCalcOffsetData> TopicUpDownOffsetVec);

		// fullname : topic#partition
		// intervalTime : 时间间隔
		// result : 本次获取的读写偏移量和堆积数
		// calcResult : 计算后的读写速度和堆积数
		void calcRWSpeed(std::string fullname, 
		    const Bmco::Timespan &intervalTime, 
			const mqCalcOffsetData &result, mqCalcOffsetData &calcResult);

		void updateLastAccumulateNumMap();

		void setLastTimeStamp();

		void getIntervalTime(Bmco::Timespan& timeCost);

	private:
		// CloudDatabaseHandler zkHandle;
		std::string m_brokerlist;
		std::string m_zookeeperlist;
		// SessionContent m_sessionContent;
		std::vector<relaInfo> m_relaBuildVec;
		bool m_needExtendFlex;  // 是否需要n+1标识
		bool m_needShrinkFlex;  // 是否需要n-1标识
		Bmco::Timestamp m_exLatestMonitorTime; // n+1触发监控时间
		Bmco::Timestamp m_shLatestMonitorTime; // n-1触发监控时间
		bool m_testControl;
		std::map<std::string, Bmco::UInt32> m_topicInfoMap;
		std::vector<MetaMQRela> m_relaInfoVec;
		MetaMQDefOp *m_DefPtr;

		// 本次获取的读写偏移量和堆积消息数
		std::map<std::string, mqCalcOffsetData> m_CurrAccumulateNumMap;
		// 上次获取的读写偏移量和堆积消息数
		// 每次上报完毕需要更新
		std::map<std::string, mqCalcOffsetData> m_LastAccumulateNumMap;
		// 上次获取的时间
		Bmco::Timestamp _lastTimeStamp;
	};

} //namespace BM35

#endif //__CLOUDDynamicFlexTASKLOOP__HH__


