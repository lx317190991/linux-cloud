#include "Bmco/Util/ServerApplication.h"
#include "Bmco/Util/Option.h"
#include "Bmco/Util/OptionSet.h"
#include "Bmco/Util/HelpFormatter.h"
#include "Bmco/Task.h"
#include "Bmco/TaskManager.h"
#include "Bmco/DateTimeFormatter.h"

#include "Bmco/SharedPtr.h"
#include "Bmco/AutoPtr.h"
#include "Bmco/NumberParser.h"
#include "Bmco/Thread.h"
#include "Bmco/Runnable.h"
#include <fstream>
#include <iostream>
#include "CloudScheduleTaskLoop.h"
#include "CloudAgentConstants.h"
#include "CloudAgentHeartBeatLoop.h"
#include "CloudSyncTaskLoop.h"
#include "CloudCmdTaskLoop.h"
#include "CloudDynamicFlexTaskLoop.h"

using namespace Bmco;
using namespace Bmco::Util;

namespace BM35
{

	class CloudAgent: public ServerApplication
	{
	public:
		CloudAgent()
			: _helpRequested(false),
			_fistRunner(true),
			_bunlock(false),
			_fileDefaultPath(false),
			_versionShowFlag(false){}
		
		virtual ~CloudAgent() {}

	protected:
		void initialize(Application& self)
		{
			loadConfiguration(); // load default configuration files, if present
			loadConfiguration(config().getString("application.dir")+"../etc/BolPublic.ini");
			if (!_fileDefaultPath)
			{
				loadConfiguration(config().getString("application.dir")+"../etc/CloudAgent.ini");
			}
			ServerApplication::initialize(self);
			bmco_information_f2(logger(), "%s,%s,CloudAgent is now starting up...",
			                    std::string("0"), std::string(__FUNCTION__));
		}

		void uninitialize()
		{
			CloudDatabaseHandler::releaseStaticHandle();
			CloudKafkaMessageHandler::releaseStaticHandle();
			BIP::named_mutex::remove(_uniqName.c_str());
			bmco_information_f2(logger(), "%s,%s,CloudAgent is now shutting down...",
			                    std::string("0"), std::string(__FUNCTION__));
			ServerApplication::uninitialize();
		}

		int main(const std::vector<std::string>& args)
		{
			if (_versionShowFlag)
			{
				displayVersion();
			}
			else if (_helpRequested) 
			{
				displayHelp();
			} 
			else 
			{
				try 
				{
					std::string basename = config().getString("application.baseName");
					_bolName  = config().getString("info.bol_name");
					std::string controlPath = config().getString("memory.control.path");
					std::string controlName = config().getString("memory.control.name");
					logger().debug(Bmco::format("bolname: %s path: %s  name: %s", _bolName,controlPath,controlName), __FILE__, __LINE__);
					if (_bolName.empty()) throw NotFoundException("info.bol_name");
						_uniqName = basename + "_" + _bolName;
				} 
				catch (Bmco::NotFoundException& e) 
				{
					bmco_error_f3(logger(), "%s|%s|please make sure configure file has included item %s."
					              , std::string("0"), std::string(__FUNCTION__), e.message());
					return Application::EXIT_CONFIG;
				}
				
				bmco_trace_f3(logger(), "%s|%s|the unique name is %s"
				              , std::string("0"), std::string(__FUNCTION__), _uniqName);

				// only unlock the singleton mutex
				if (_bunlock) 
				{
					BIP::named_mutex::remove(_uniqName.c_str());
					return Application::EXIT_OK;
				}

				// make sure only one process exists
				if (!checkSinglton()) 
				{
					bmco_error_f2(logger(), "%s|%s|please make sure current process is not running and call -u to release the lock if necessary.",
					              std::string("0"), std::string(__FUNCTION__));
					return Application::EXIT_TEMPFAIL;
				}

				//connect to control region
				ControlRegionParams::Ptr ctlAttrPtr(new ControlRegionParams());
				CtlAttrExtractor()(config(), ctlAttrPtr);
				ControlRegionOp::Ptr ctlOptPtr(new ControlRegionOp(ctlAttrPtr, writer_t));
				if (!ctlOptPtr->initialize())
				{
					logger().information("Process #0 is not started yet!");
					return Application::EXIT_DATAERR;
				}
                else
                {
					CloudConfigurationUpdater updater(ctlOptPtr, config().getString(CONFIG_INFO_BOL_NAME), 
						config().getString("common.confpath"));
                    if (updater.updateCloudData() != 0) {
                        bmco_error(logger(), "Failed to get cloud data.");
                        return false;
                    }

					Bmco::UInt32 port = config().getInt("http.clientport", 8041);

                    // run worker thread.
                    TaskManager tm;
                    tm.start(new CloudAgentHeartBeatLoop(ctlOptPtr));
                    tm.start(new CloudSyncTaskLoop(ctlOptPtr));
                    tm.start(new CloudCmdTaskLoop(ctlOptPtr, _bolName, port));
					tm.start(new CloudDynamicFlexTaskLoop(ctlOptPtr, _bolName));
					tm.start(new CloudScheduleTaskLoop(ctlOptPtr));
                    waitForTerminationRequest();
                    tm.cancelAll();
                    tm.joinAll();
                }
			}
			return Application::EXIT_OK;
		}

		void defineOptions( Bmco::Util::OptionSet& options)
		{
			ServerApplication::defineOptions(options);

			options.addOption(
			    Option("help", "h", "display help information on command line arguments")
			    .required(false)
			    .repeatable(false));

			options.addOption(
			    Option("define", "D", "define a configuration property")
			    .required(false)
			    .repeatable(true)
			    .argument("name=value")
			    .callback(OptionCallback<CloudAgent>(this, &CloudAgent::handleDefine)));

			options.addOption(
			    Option("config-file", "f", "load configuration data from a file")
			    .required(false)
			    .repeatable(true)
			    .argument("file")
			    .callback(OptionCallback<CloudAgent>(this, &CloudAgent::handleConfig)));

			options.addOption(
			    Option("unlock", "u", "unlock the global mutex manually.")
			    .required(false)
			    .repeatable(false)
			    .callback(OptionCallback<CloudAgent>(this, &CloudAgent::handleUnlock)));

			options.addOption(
			    Option("version", "v", "show the version")
			    .required(false)
			    .repeatable(false)
			    .callback(OptionCallback<CloudAgent>(this, &CloudAgent::handleVersion)));
		}
		
		void handleOption(const std::string& name, const std::string& value)
		{
			ServerApplication::handleOption(name, value);

			if (name == "help")
				_helpRequested = true;
		}

		void handleDefine(const std::string& name, const std::string& value)
		{
			defineProperty(value);
		}

		void handleUnlock(const std::string& name, const std::string& value)
		{
			_bunlock = true;
		}

		void handleConfig(const std::string& name, const std::string& value)
		{
			_fileDefaultPath = true;
			loadConfiguration(value);
		}
		void handleVersion(const std::string& name, const std::string& value)
		{
			_versionShowFlag = true;
		}
		void defineProperty(const std::string& def)
		{
			std::string name;
			std::string value;
			std::string::size_type pos = def.find('=');
			if (pos != std::string::npos)
			{
				name.assign(def, 0, pos);
				value.assign(def, pos + 1, def.length() - pos);
			}
			else name = def;
			config().setString(name, value);
		}
		void handleHelp(const std::string& name, const std::string& value)
		{
			_helpRequested = true;
			displayHelp();
			stopOptionsProcessing();
		}
		void displayHelp()
		{
			HelpFormatter helpFormatter(options());
			helpFormatter.setCommand(commandName());
			helpFormatter.setUsage("OPTIONS");
			helpFormatter.setHeader("Billing-On-Line 3.5 CloudAgent process");
			helpFormatter.format(std::cout);
		}
		void printProperties(const std::string& base)
		{
			AbstractConfiguration::Keys keys;
			config().keys(base, keys);
			if (keys.empty())
			{
				if (config().hasProperty(base))
				{
					std::string msg;
					msg.append(base);
					msg.append(" = ");
					msg.append(config().getString(base));
					logger().information(msg);
				}
			}
			else
			{
				for (AbstractConfiguration::Keys::const_iterator it = keys.begin(); it != keys.end(); ++it)
				{
					std::string fullKey = base;
					if (!fullKey.empty()) fullKey += '.';
					fullKey.append(*it);
					printProperties(fullKey);
				}
			}
		}
		bool checkSinglton()
		{
			try
			{
				BIP::named_mutex excl_mtx(BIP::create_only, _uniqName.c_str());
				_fistRunner = true;
				return true;
			}
			catch (...)
			{
				_fistRunner = false;
				return false;
			}
		}

		void displayVersion() 
		{
		    std::string version = "debug version";
#ifdef MODULE_VERSION
		    version = MODULE_VERSION;
#endif
		    std::cout << "version: " << version << std::endl;
		}
		
	private:
		bool _helpRequested;
		bool _fistRunner;
		bool _bunlock;
		bool _versionShowFlag;
		bool _fileDefaultPath;
		std::string _uniqName;
		std::string _bolName;

	};
} //namespace BM35

BMCO_SERVER_MAIN(BM35::CloudAgent)

