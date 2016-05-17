#ifndef __CLOUDCMDTASKLOOP__HH__
#define __CLOUDCMDTASKLOOP__HH__
#include <sstream>
#include "Bmco/Process.h"
#include "Bmco/Util/Application.h"
#include "BolCommon/ControlRegionOp.h"
#include "BolCommon/ControlRegionBasedTask.h"
#include "CloudAgentConstants.h"
#include "CloudDatabaseHandler.h"
#include "CloudConfigurationUpdater.h"
#include "IniFileConfigurationNew.h"
#include "Bmco/Net/HTTPServer.h"
#include "Bmco/Net/HTTPServerParams.h"
#include "Bmco/Net/AbstractHTTPRequestHandler.h"
#include "Bmco/Net/HTTPRequestHandlerFactory.h"
#include "Bmco/Net/HTTPClientSession.h"
#include "Bmco/Net/HTTPRequest.h"
#include "Bmco/Net/HTTPServerRequest.h"
#include "Bmco/Net/HTTPResponse.h"
#include "Bmco/Net/HTTPServerResponse.h"
#include "Bmco/Net/ServerSocket.h"

using Bmco::Net::HTTPServer;
using Bmco::Net::HTTPServerParams;
using Bmco::Net::HTTPRequestHandler;
using Bmco::Net::AbstractHTTPRequestHandler;
using Bmco::Net::HTTPRequestHandlerFactory;
using Bmco::Net::HTTPClientSession;
using Bmco::Net::HTTPRequest;
using Bmco::Net::HTTPServerRequest;
using Bmco::Net::HTTPResponse;
using Bmco::Net::HTTPServerResponse;
using Bmco::Net::HTTPMessage;
using Bmco::Net::ServerSocket;
using Bmco::Util::ServerApplication;
using Bmco::Util::Application;
using Bmco::Util::Option;
using Bmco::Util::OptionSet;

namespace BM35
{
	/// CloudAgent run time task name, it will be used as thread name by taskmanager
#define BOL_CLOUDCMD_TASK_NAME  "Bol_Cloud_Cmd_Task_Loop_Thread"
	class processCmdHandler
	{
	public:
		processCmdHandler() 
			: theLogger(Bmco::Util::Application::instance().logger())
		{
		}

		// 接收客户端请求命令处理程序
		void processCommand(const std::string &cmdRequestJsonStr, 
			std::string &cmdResponeJsonStr);

		// 设置bol状态
		bool setBolStatus(std::string& bolName, 
			Bmco::UInt16 oldStatus, Bmco::UInt16 newStatus);

		// 返回响应给客户端
		void sendAckProcess(std::string reqSeq, 
			std::string resultCode, std::string resultDesc, 
			std::string &cmdResponeJsonStr);

		void setControlRegionPoint(ControlRegionOp::Ptr& ptr)
		{
			m_ctlOper = ptr;
		}

		void setBolNameInfoPoint(std::string bolName)
		{
			m_bolName = bolName;
		}

		bool init()
		{
			return true;
		}

		static processCmdHandler *instance() 
		{
	        boost::mutex::scoped_lock lock(_instanceMutex);
	        if (_pInstance == NULL) 
			{
	            _pInstance = new processCmdHandler();
	        }
	        if (_pInstance != NULL) 
			{
	            try 
				{
	                if (!_pInstance->init()) 
					{
	                    delete _pInstance;
	                    _pInstance = NULL;
	                    return NULL;
	                }
	            } 
				catch (...) 
	            {
	                delete _pInstance;
	                _pInstance = NULL;
	            }
	        }
	        return _pInstance;
    	}

	private:
		Bmco::Logger& theLogger;
		ControlRegionOp::Ptr m_ctlOper;
		static processCmdHandler *_pInstance;
		static boost::mutex _instanceMutex;
		std::string m_bolName;
	};

	// 统一服务端处理接口
	class CmdProcessRequestHandler: public HTTPRequestHandler
	{
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response);
	};

	// 工厂类，对不同的URL返回不同的处理对象，云代理这边统一为'/'
	class RequestHandlerFactory: public HTTPRequestHandlerFactory
	{
	public:
		HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request)
		{
			//if (request.getURI() == "/xxx)
			//	return new EchoBodyRequestHandler;
			//else
			//	return 0;
			return new CmdProcessRequestHandler;
		}
	};

	/// This class collect information and send them to Mysql.
	/// which will be run in a dedicated thread and managed by Bmco::TaskManager
	class CloudCmdTaskLoop: public ControlRegionBasedTask<Bmco::Logger>
	{
	public:
		CloudCmdTaskLoop(ControlRegionOp::Ptr& ptr, std::string bolName, Bmco::UInt32 port)
			: svs(port), 
			srv(new RequestHandlerFactory, svs, new HTTPServerParams),
			ControlRegionBasedTask<Bmco::Logger>(BOL_CLOUDCMD_TASK_NAME, ptr,
			                                     Bmco::Util::Application::instance().logger())
		{
			m_bolName = bolName;
			m_port = port;
		}
		virtual ~CloudCmdTaskLoop(void)
		{
		}
		/// do check and initlization before task loop
		virtual bool beforeTask()
		{
			theLogger.debug("CloudCmdTaskLoop::beforeTask()");
			if (!init())
			{
				return false;
			}
			else
			{
				return true;
			}
		}


		/// do uninitlization after task loop
		virtual bool afterTask()
		{
			theLogger.debug("CloudCmdTaskLoop::afterTask()");
			// Stop the HTTPServer
			srv.stop();
			return true;
		}

		/// perform business logic
		virtual bool handleOneUnit()
		{
			this->sleep(4000);
			return true;
		}

	protected:

		//! Doing initialization.
		bool init()
		{
			processCmdHandler::instance()->setControlRegionPoint(ctlOper);
			processCmdHandler::instance()->setBolNameInfoPoint(m_bolName);

			// 设定端口
			//ServerSocket svs(9090);
			//HTTPServerParams* pParams = new HTTPServerParams;
			//pParams->setKeepAlive(false);

			//http服务线程
			//HTTPServer srv(new RequestHandlerFactory, svs, pParams);

			try
			{
				//启动服务
				srv.start();
				bmco_information_f1(theLogger, "http server [%?d] has been set!",
					m_port);
			}
			catch (Bmco::AssertionViolationException &e)
			{
				bmco_error_f2(theLogger, "http server [%?d] set failed!, because [%s]",
					m_port, e.what());
				return false;
			}
			
			return true;
		}

		//! refresh #0 log level
		void refreshLogLevel()
		{
			MetaBolConfigOp *c = dynamic_cast<MetaBolConfigOp *>(ctlOper->getObjectPtr(MetaBolConfigOp::getObjName()));
			assert (NULL != c);
			MetaBolConfigInfo::Ptr cfgpayload(new MetaBolConfigInfo("logging.logger.CloudAgent.level"));
			if (c->Query(cfgpayload))
			{
				theLogger.setLevel(cfgpayload->_strval.c_str());
			}
			else
			{
				theLogger.setLevel("warning");
			}
		}
	private:
		std::string m_bolName;
		ServerSocket svs;     // 服务socket
		HTTPServer srv;       // http服务线程
		Bmco::UInt32 m_port;
		//static std::string ERR_CONTENT_EMPTY;
		//static std::string ERR_CONTENT_BOL_MSG_QUEUE;
		//static std::string ERR_CONTENT_BOL_HEART_BEAT;
		//static std::string ERR_CONTENT_BOL_NORMAL;
		//static std::string ERR_CONTENT_BOL_OFFLINE;
		//static std::string ERR_CONTENT_GET_CORE_PARAM;
		//static std::string ERR_CONTENT_SET_CORE_PARAM;
	};

} //namespace BM35

#endif //__CLOUDCMDTASKLOOP__HH__



