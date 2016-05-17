#include <time.h>
#include <iostream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include "Bmco/Util/IniFileConfiguration.h"
#include "Bmco/NumberFormatter.h"
#include "CloudCmdTaskLoop.h"
#include "CheckPoint.h"
#include <boost/typeof/typeof.hpp>
#include <boost/lexical_cast.hpp>
#include "json/json.h"

/*命令处理类实现代码 */
namespace BM35
{
	processCmdHandler *processCmdHandler::_pInstance = NULL;
	boost::mutex processCmdHandler::_instanceMutex;
	// #define ERR_CONTENT_EMPTY "Succussfully";
	//#define ERR_CONTENT_BOL_MSG_QUEUE "Message queue is not empty";
	//#define ERR_CONTENT_BOL_HEART_BEAT "Heart beat check failure";
	#define ERR_CONTENT_BOL_NORMAL "Failed to set BOL status";
	//#define ERR_CONTENT_BOL_OFFLINE "Failed to set BOL status offline";
	//#define ERR_CONTENT_GET_CORE_PARAM "Failed to get core parameters";
	//#define ERR_CONTENT_SET_CORE_PARAM "Failed to set core parameters";

	void CmdProcessRequestHandler::handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
	{
		// if (request.getChunkedTransferEncoding())
		//	 response.setChunkedTransferEncoding(true);
		// else if (request.getContentLength() != HTTPMessage::UNKNOWN_CONTENT_LENGTH)
		// 	 response.setContentLength(request.getContentLength());
		
		response.setContentType(request.getContentType());
		
		std::istream& istr = request.stream();
		std::string inputStr;
		std::string outputStr;
		
		//std::getline(istr, outputStr);
		istr >> inputStr;
		processCmdHandler::instance()->processCommand(inputStr, outputStr);
		
		std::ostream& ostr = response.send();
		ostr << outputStr;
		//std::streamsize n = StreamCopier::copyStream(istr, ostr);
	}

	// 根据bolname的目前状态进行设置新的状态
	bool processCmdHandler::setBolStatus(std::string& bolName, 
			Bmco::UInt16 oldStatus, Bmco::UInt16 newStatus)
	{
		if (m_bolName != bolName)
		{
			bmco_error_f1(theLogger, "it's not the [%s]'s CloudAgent!", 
				bolName);
			return false;
		}
		
		MetaBolInfo::Ptr ptr = new MetaBolInfo(BOL_NORMAL, "", "");
			MetaBolInfoOp *p = dynamic_cast<MetaBolInfoOp *>
				(m_ctlOper->getObjectPtr(MetaBolInfoOp::getObjName()));
		BolStatus tmpStatus = BOL_NORMAL;
		Bmco::UInt32 tmpCrc32CheckSum = 0;
		if (!p->Query(tmpStatus, tmpCrc32CheckSum))
		{
			return false;
		}

		// 旧状态不相符
		if (tmpStatus != oldStatus)
		{
			return false;
		}

		// 1.上线切换维保
		// 2.维保切换上线
		// 3.切换离线
		if (((oldStatus == BOL_NORMAL) && (newStatus == BOL_MAINTAIN))
			|| ((oldStatus == BOL_MAINTAIN) && (newStatus == BOL_NORMAL))
			|| (newStatus == BOL_OFFLINE))
		{
			if (!p->Update(static_cast<BolStatus>(newStatus)))
			{
				// bmco_error_f3(theLogger, "Failed to update bol[%s] from [%s] to [%?d].", 
				//	 bolName, tokensRuleStr[6], status);
				return false;
			}
		}

		return true;
	}

	void processCmdHandler::sendAckProcess(std::string reqSeq, 
			std::string resultCode, std::string resultDesc, 
			std::string &cmdResponeJsonStr)
	{
		cmdResponeJsonStr = "{}";
			
		Json::Value root(Json::objectValue);
		Json::Value head(Json::objectValue);

		Bmco::Timestamp now;
		LocalDateTime dtNow(now);
		std::string stNow = DateTimeFormatter::format(dtNow, DateTimeFormat::SORTABLE_FORMAT);

		head["reqseq"] = reqSeq;
		head["resptime"] = stNow;
		head["resptype"] = "1";
		head["respcode"] = resultCode;
		head["respdesc"] = resultDesc;
		root["head"] = head;

		Json::FastWriter jfw;
		cmdResponeJsonStr = jfw.write(root);
	}

	void processCmdHandler::processCommand(const std::string &cmdRequestJsonStr, 
		std::string &cmdResponeJsonStr)
	{
		Json::Value root;
		Json::Reader jsonReader;
		try
		{
			if (!jsonReader.parse(cmdRequestJsonStr, root)) 
			{
				bmco_error_f2(theLogger, "%s|%s|Failed to execute jsonReader",std::string("0"),std::string(__FUNCTION__));
				return;
			}
			if (!root.isMember("head")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no head",std::string("0"),std::string(__FUNCTION__));
				return;
			}
			if (!root.isMember("content")) 
			{
				bmco_error_f2(theLogger, "%s|%s|no content",std::string("0"),std::string(__FUNCTION__));
				return;
			}

			Json::Value pHeadNode = root["head"];
			Json::Value pContentNode = root["content"];
			
			std::string cmd = pHeadNode["processcode"].asString();
			std::string reqSeq = pHeadNode["reqseq"].asString();
			std::string bolName = pContentNode["bol_name"].asString();
			Bmco::UInt16 oldStatus = boost::lexical_cast<Bmco::UInt16>(pContentNode["old_status"].asString());
			Bmco::UInt16 newStatus = boost::lexical_cast<Bmco::UInt16>(pContentNode["new_status"].asString());

			std::string logStr = Bmco::format("http request: cmd[%s], reqSeq[%s], bolName[%s], "
				"oldStatus[%?d], newStatus[%?d]", 
				cmd, reqSeq, bolName, oldStatus, newStatus);
			bmco_information_f3(theLogger, "%s|%s|%s", std::string("0"), std::string(__FUNCTION__), logStr);

			std::string resultCode;
			std::string resultDesc;

			if (cmd == "changebolstatus")
			{
				if (setBolStatus(bolName, oldStatus, newStatus))
				{
					resultCode = "0";
					resultDesc = "";
				}
				else
				{
					resultCode = "1";
					resultDesc = ERR_CONTENT_BOL_NORMAL;
				}

				sendAckProcess(reqSeq, resultCode, resultDesc, cmdResponeJsonStr);
			}
			else
			{
			}
		}
		catch (boost::bad_lexical_cast &e)  
		{
			bmco_error_f3(theLogger,"%s|%s|%s",std::string("0"),std::string(__FUNCTION__),std::string(e.what()));
			return;
		}		
		catch(...)
		{
			bmco_error_f2(theLogger,"%s|%s|unknown exception occured when syncRela!",
									std::string("0"),std::string(__FUNCTION__));
			return;
		}
	}
}

