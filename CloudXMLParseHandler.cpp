#include "CloudXMLParseHandler.h"
#include "AESDecode.h"

/*int utf8togb2312(char *sourcebuf, size_t sourcelen, char *destbuf, size_t destlen)
{
#if BMCO_OS == BMCO_OS_WINDOWS_NT

	char buf[MEMLEN] = {'\0'}, buf2[MEMLEN] = {'\0'};

	MultiByteToWideChar(CP_UTF8, 0, (LPCSTR)sourcebuf, -1, (LPWSTR)buf, MEMLEN);    //将utf8转换为Unicode， buf[]={37 00 38 00 7D 59 38 00 37 00 00 00}

	WideCharToMultiByte (CP_ACP, 0, (LPCWSTR)buf, -1, destbuf, MEMLEN, NULL, NULL);    //将Unicode转换为ANSI， buf2[]={37 38 BA C3 38 37 00}
  return 0;
#endif

	iconv_t cd;
	#if BMCO_OS == BMCO_OS_AIX
	if( (cd = iconv_open("GB18030","UTF-8")) ==0 )
	return -1;
	#endif
	#if  BMCO_OS == BMCO_OS_HPUX
	if( (cd = iconv_open("gb18030","utf8")) ==0 )
		return -1;	
	#endif
	#if BMCO_OS == BMCO_OS_LINUX
	if( (cd = iconv_open("gb2312","utf-8")) ==0 )
		return -1;
	#endif
	
	memset(destbuf,0,destlen);
	char **source = &sourcebuf;
	char **dest = &destbuf;
	if(-1 == iconv(cd,source,&sourcelen,dest,&destlen))
		return -1;
	iconv_close(cd);

	return 0;
}*/
namespace BM35{

bool CloudXMLParse::loadParamFromXMLFile(std::string fileName)
{
	try
	{
		E_Step findConnectStrStep = E_FINDZKTYPEKET;
		std::string nodeName;
		std::string nodeValue;
		Bmco::XML::InputSource src(fileName);//设置源数据 
		Bmco::XML::DOMParser parser;//创建一个分析者
		
		//开始分析，并返回指向分析后数据的指针
		Bmco::AutoPtr<Bmco::XML::Document> pDoc = parser.parse(&src);
		//创建一个节点迭代器，默认迭代显示全部node  
		Bmco::XML::NodeIterator it(pDoc, Bmco::XML::NodeFilter::SHOW_ALL);
		//此时指向第一个
		Bmco::XML::Node *pNode = it.nextNode();
		while (pNode)  
		{  
			/*Bmco::XML::NamedNodeMap* map = pNode->attributes();
			if (map)
			{  
				for (int i = 0; i < map->length(); i++)//属性肯定至少0个，用循环一个个取出  
				{  
					Bmco::XML::Node* attr = map->item(i);
					nodeName = attr->nodeName();
					nodeValue = attr->nodeValue();

					std::cout<<"mapName:"<<nodeName.c_str()
						<<" mapValue:"<<nodeValue.c_str()<<std::endl;
				}
			}*/
				
			nodeName = Bmco::XML::fromXMLString(pNode->nodeName()).c_str();
			nodeValue = Bmco::XML::fromXMLString(pNode->nodeValue()).c_str();

			if (E_FINDCONNECTVALUE == findConnectStrStep)
			{
				m_ZKConnectStr = nodeValue;
				break;
			}

			if ((E_FINDCONNECTKEY == findConnectStrStep)
				&& (0 == nodeName.compare("connect_str")))
			{
				findConnectStrStep = E_FINDCONNECTVALUE;
			}
			
			if ((E_FINDZKTYPEVALUE == findConnectStrStep)
				&& (0 == nodeValue.compare("zookeeper_cloud")))
			{
				findConnectStrStep = E_FINDCONNECTKEY;
			}

			if ((E_FINDZKTYPEKET == findConnectStrStep)
				&& 0 == nodeName.compare("db_id"))
			{
				findConnectStrStep = E_FINDZKTYPEVALUE;
			}

			pNode = it.nextNode(); 
		}

		pDoc = NULL;
		if (E_FINDCONNECTVALUE != findConnectStrStep)
		{
			bmco_information_f2(theLogger, "%s|%s|no zookeeper config content, please check!", std::string("0"), std::string(__FUNCTION__));
			return false;
		}
	}
	catch (Bmco::Exception &e)
	{
		bmco_error_f3(theLogger, "%s|%s|%s", std::string("0"),std::string(__FUNCTION__),e.displayText());
		return false;
	}
	catch (...)
	{
		bmco_error_f2(theLogger, "%s|%s|unknown exception occured when loadParamFromXMLFile!", std::string("0"),std::string(__FUNCTION__));
		return false;
	}

	return true;
}

std::string CloudXMLParse::getZKConncetStr()
{
	return m_ZKConnectStr;
}

std::string CloudXMLParse::getDecodeConnectStr()
{
	std::string decrypted = CryptAESObject::defaultManager().aes_cloud_decode(m_ZKConnectStr);

	if (decrypted.empty())
	{
		return std::string("");
	}
	
	return decrypted;
}

/*std::string CloudXMLParse::getDecodeConnectStr()
{
	if (16 >= m_ZKConnectStr.length())
	{
		return std::string("");
	}
	
	std::string decrypted;
	std::string::size_type posBegin = 0;
	std::string::size_type posEnd = m_ZKConnectStr.length();
	std::string key = m_ZKConnectStr.substr(posBegin, 16);
	std::string iv = "I___LOVE___CLOUD";
	std::string sourceStr = m_ZKConnectStr.substr(16, posEnd-16);

	bmco_information_f3(theLogger, "key[%s] iv[%s], str[%s]", key, iv, sourceStr);
	
	std::vector<unsigned char> keyVec(key.begin(), key.end());
	std::vector<unsigned char> ivVec(iv.begin(), iv.end());
	CloudAESHandler *handler = new CloudAESHandler();
	
	if (!handler->AESdecode(sourceStr, keyVec, ivVec, decrypted))
	{
		delete handler;
		handler = NULL;
		return std::string("");
	}
	
	delete handler;
	handler = NULL;
	return decrypted;
}*/
}

