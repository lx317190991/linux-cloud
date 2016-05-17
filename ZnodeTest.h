#ifndef _CLOUDAGENT_ZNODETES_HH__
#define _CLOUDAGENT_ZNODETES_HH__
#if BMCO_OS == BMCO_OS_LINUX
#include <unistd.h>
#include <sys/wait.h>
#endif
#include<sys/types.h> //对于此程序而言此头文件用不到
#include<stdio.h>
#include<stdlib.h>
#include "Bmco/Process.h"

#include "Bmco/Util/Application.h"
#include "Bmco/FileStream.h"
#include "Bmco/File.h"
#include <time.h>
#include <iostream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include "Bmco/Util/IniFileConfiguration.h"
#include "Bmco/NumberFormatter.h"
#include "Bmco/AutoPtr.h"
#include "IniFileConfigurationNew.h"
#include "CloudDatabaseHandler.h"
#include "CloudAgentConstants.h"

using Bmco::Util::Application;

namespace BM35 {
class ZnodeTestTask
{
   public:
	   ZnodeTestTask()
		   :theLogger(Application::instance().logger()),m_config(Bmco::Util::Application::instance().config()) 
	   {
	   }

	      	   
	   virtual ~ZnodeTestTask() {
	   }


     bool setNodeData(const std::string& zNodePath,const std::string& filePath)
     {
     	std::ostringstream oss;
     	 Bmco::AutoPtr<IniFileConfigurationNew> config = new IniFileConfigurationNew(filePath);
     	 config->save(oss);
     	 if (CloudDatabaseHandler::instance()->setNodeData(zNodePath, oss.str()))
			 {
          bmco_information(theLogger, "set node successfully");
               return true;
			 }
			 else
			{
				bmco_information(theLogger, "fail to set node");
				return false;
			 }
     	   
     }

	bool setTxtData(const std::string& zNodePath,const std::string& filePath)
	{
		std::string content;
		Bmco::FileInputStream fos(filePath, std::ios::binary);
		fos >> content;
		if (!CloudDatabaseHandler::instance()->setNodeData(zNodePath, content))
		{
			return false;
		}

		return true;
	}

  private:

	  
	  Bmco::Logger&	theLogger;  
	  Bmco::Util::AbstractConfiguration &m_config;
};

}
#endif	//_CLOUDAGENT_ZNODETES_HH__

