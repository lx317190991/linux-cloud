#include "Bmco/Util/ServerApplication.h"
#include "Bmco/Util/Option.h"
#include "Bmco/Util/OptionSet.h"
#include "Bmco/Util/HelpFormatter.h"
#include "Bmco/Util/AbstractConfiguration.h"
#include "Bmco/NumberParser.h"
#include "ZnodeTest.h"
#include "CloudDatabaseHandler.h"

using namespace Bmco;
using namespace Bmco::Util;

namespace BM35
{
class CommandNodeTest: public Application
{
public:
	CommandNodeTest(): _helpRequested(true){}    
    virtual ~CommandNodeTest(){}
protected:
    void initialize(Application& self)
    {
        loadConfiguration(config().getString("application.dir")+"../etc/"+
			config().getString("application.name")+".ini"); // load default configuration files, if present
        Application::initialize(self);
        logger().information("zondetest is now starting up...");		
    }
        
    void uninitialize()
    {
        logger().information("zondetest is now shutting down...");
        Application::uninitialize();
    }
	
	int main(const std::vector<std::string>& args)
    {
    	if(Bmco::NumberParser::parse(config().getString("application.argc")) != 3)
    	{
    		displayHelp();
    		return -1;
    	}

		if ("0" == config().getString("application.argv[1]"))
		{
			std::string path = config().getString("application.argv[2]");

			bool isChanged = false;
			while (!isChanged)
			{
				CloudDatabaseHandler::instance()->getNodeChangedFlag(path, isChanged);
			}
			
			std::string cloudConfigStr;
			if (0 != CloudDatabaseHandler::instance()->getNodeData(path, cloudConfigStr))
			{
				std::cout<<"get "<<path<<" failed!"<<std::endl;
			}
			std::cout<<"cloudConfigStr = "<<cloudConfigStr<<std::endl;
		}
		else
		{
			return Application::EXIT_DATAERR;
		}

        return Application::EXIT_OK;
    }
    
    void defineOptions(OptionSet& options)
    {
        Application::defineOptions(options);
		setUnixOptions(true);
        //options.addOption(
        //    Option("help", "h", "display help information on command line arguments")
        //    .required(false)
        //    .repeatable(false));

    }

    void handleOption(const std::string& name, const std::string& value)
    {
        Application::handleOption(name, value);

        if (name == "help")
            _helpRequested = true;
    }

    void handleDefine(const std::string& name, const std::string& value)
    {
        defineProperty(value);
    }

    void handleConfig(const std::string& name, const std::string& value)
    {
        loadConfiguration(value);
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
        helpFormatter.setHeader("Billing-On-Line 3.5 znode process, which is known as one of the process of HLA commands");
		helpFormatter.setFooter(" ZnodeTest xxx[znodepath] xxx[infile]\n"
								);
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
	
private:
    bool _helpRequested;
};

}//end namespace BM35

BMCO_APP_MAIN(BM35::CommandNodeTest)
