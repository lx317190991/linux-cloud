#include "CloudAgentConstants.h"
#include "CloudDatabaseHandler.h"
#include "CloudXMLParseHandler.h"
#include "Bmco/File.h"

namespace BM35
{
	CloudDatabaseHandler *CloudDatabaseHandler::_pInstance = NULL;
	boost::mutex CloudDatabaseHandler::_instanceMutex;
	// std::map<std::string, CloudDatabaseHandler::MonitorData> CloudDatabaseHandler::m_dataVec;
	std::map<std::string, bool> CloudDatabaseHandler::m_changePath;
	static void CloudAgent_watcher_global(zhandle_t *zh, int type, int state,
	                                      const char *path, void* watcherCtx)
	{
		if (type == ZOO_SESSION_EVENT)
		{
			if (state == ZOO_CONNECTED_STATE)
			{
				//NOTICE, connection is established but nothing to do.
			}
			else if (state == ZOO_EXPIRED_SESSION_STATE 
				|| state == ZOO_AUTH_FAILED_STATE)
			{
				//NOTICE, connection is expired.
				static_cast<CloudDatabaseHandler *>(watcherCtx)->clearZkHandle();
			}
		}
	}
	void CloudDatabaseHandler::createNodeCompletion(int rc, const char *name, const void *data)
	{
		SyncData *handler = static_cast<SyncData *>(const_cast<void *>(data));
		if (rc == ZOK || rc == ZNODEEXISTS)
		{
			int curFlag = 0;
			if (handler->data == handler->tmpPath)
			{
				handler->isDataReady = true;
				//handler->condition.notify_one();
				handler->signal();
			}
			else
			{
				size_t pos = handler->data.find('/', handler->tmpPath.size() + 1);
				if (pos == std::string::npos)
				{
					handler->tmpPath = handler->data;
					// »Áπ˚ «¡Ÿ ±Ω⁄µ„£¨◊Ó∫Û“ªº∂Ω®¡¢
					curFlag = handler->flag;
				}
				else if (pos < handler->data.size())
				{
					handler->tmpPath = handler->data.substr(0, pos);
					curFlag = 0;
				}

				bmco_information_f1(handler->logger, "Create next level node: [%s]", handler->tmpPath);
				int ret = zoo_acreate(handler->zkHandle, (char *)handler->tmpPath.c_str(), NULL, 0,
				                      &ZOO_OPEN_ACL_UNSAFE, curFlag,
				                      createNodeCompletion, data);
				if (ret != ZOK)
				{
					bmco_error_f1(handler->logger, "Error to call zoo_acreate: [%d]", ret);
					// lixiao add
					handler->isDataReady = false;
					//handler->condition.notify_one();
					handler->signal();
					return;
				}
			}
		}
		// ¥¥Ω® ß∞‹
		else
		{
			bmco_error_f2(handler->logger, "create [%s] failed rc = [%d]", handler->tmpPath, rc);
			//handler->condition.notify_one();
			handler->signal();
		}
	}
	/*void CloudDatabaseHandler::changedNodeDataWatcher(zhandle_t *zh, int type, int state,
	                                                  const char *path, void *watcherCtx)
	{
		SyncData *handler = static_cast<SyncData *>(watcherCtx);
		if (state == ZOO_CONNECTED_STATE)
		{
			if (type == ZOO_CHANGED_EVENT)
			{
				int ret = zoo_aget(zh, path, 2, nodeDataCompletion, watcherCtx);
				if (ret)
				{
					bmco_error_f1(handler->logger, "Failed to get value: %d", ret);
					return;
				}
			}
		}
		else
		{
			// re-get and set watch on node again.
			//zkConf_awget(zh, path);
		}
	}*/
	void CloudDatabaseHandler::changedNodeDataWatcher(zhandle_t *zh, int type, int state,
	                                                  const char *path, void *watcherCtx)
	{
		SyncData *handler = static_cast<SyncData *>(watcherCtx);
		if (state == ZOO_CONNECTED_STATE)
		{
			if (type == ZOO_CHANGED_EVENT
				|| type == ZOO_CHILD_EVENT)
			{
				/*int ret = zoo_aget(zh, path, 2, nodeDataCompletion, watcherCtx);
				if (ret)
				{
					bmco_error_f1(handler->logger, "Failed to get value: %d", ret);
					return;
				}*/
				m_changePath[path] = true;
			}
		}
		else
		{
			// re-get and set watch on node again.
			//zkConf_awget(zh, path);
		}
	}
	/*void CloudDatabaseHandler::setNodeDataCompletion(int rc, const struct Stat *stat, const void *data)
	{
		//CloudAgent_dump_stat(stat);
		SyncData *handler = static_cast<SyncData *>(const_cast<void *>(data));
		if (rc == ZOK)
		{
			handler->isDataReady = true;
		}
		handler->condition.notify_one();
	}*/
	void CloudDatabaseHandler::nodeDataCompletion(int rc, const char *value, int value_len,
	                                              const struct Stat *stat, const void *data)
	{
	    SyncData *handler = static_cast<SyncData *>(const_cast<void *>(data));
		if (rc == ZOK)
		{
			handler->data.assign(value, value_len);
			bmco_information_f2(handler->logger, "changed data path[%s], data[%s]", handler->nodePath.c_str(), handler->data.c_str());
		}
	}
	void CloudDatabaseHandler::nodeChangedDataCompletion(int rc, const char *value, int value_len,
	                                                     const struct Stat *stat, const void *data)
	{
		/*if (rc == ZOK) {
		    SyncData *handler = static_cast<SyncData *>(const_cast<void *>(data));
		    handler->data.assign(value, value_len);
		    handler->condition.notify_one();
		}*/
	}

	bool CloudDatabaseHandler::loadXMLParamForZK(const std::string fileName, std::string &hostName)
	{
		// ƒ¨»œ∑≈‘⁄/etcœ¬√Ê
		std::string wholefileName = m_config.getString("application.dir")+"../etc/"+fileName;
		Bmco::File xmlFile(wholefileName);
		if (!xmlFile.exists())
		{
			bmco_information_f1(theLogger, "[%s] is not found", wholefileName);
			return false;
		}
		
		CloudXMLParse *xmlPtr = new CloudXMLParse();
		if (!xmlPtr->loadParamFromXMLFile(wholefileName))
		{
			delete xmlPtr;
			xmlPtr = NULL;
			return false;
		}

		// ¡¨Ω”¥Æ‘› ±≤ªº”√‹
		// hostName = xmlPtr->getDecodeConnectStr();
		hostName = xmlPtr->getZKConncetStr();
		if (hostName.empty())
		{
			bmco_error_f1(theLogger, "hostName[%s] is null", hostName);
			delete xmlPtr;
			xmlPtr = NULL;
			return false;
		}
		
		delete xmlPtr;
		xmlPtr = NULL;
		return true;
	}

	bool CloudDatabaseHandler::isValidHandle()
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle) 
		{
			return true;
		}

		return false;
	}

	void CloudDatabaseHandler::setZKLogFilePoint()
	{
		std::string defaultPath = m_config.getString("application.dir") + "../log/zookeeper.log";
		m_logPath = m_config.getString("zkConf.zookeeper.path", defaultPath);

		m_fp = fopen(m_logPath.c_str(), "w+");
		if (m_fp != NULL)
		{
			zoo_set_log_stream(m_fp);
		}
	#ifdef MODULE_VERSION
		zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
	#else
		zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
	#endif
	}
	
	bool CloudDatabaseHandler::init()
	{
		//bmco_information(theLogger, "CloudDatabaseHandler::init");
		//if (m_zkHandle) 
		//{
		//	return true;
		//}
		
		ECLUSIVE_LOCK _threadguard(rwlock);
	
		int timeout = 300000;//
		std::string hostStr;
		std::string fileName = m_config.getString("zkConf.zookeeper.loginfile", "dbconnect.xml");

		if (!loadXMLParamForZK(fileName, hostStr))
		{
			hostStr = m_config.getString("zkConf.zookeeper.host");
		}
		
		bmco_information_f1(theLogger, "ZKconnectKey = %s", hostStr);
		m_zkHandle = zookeeper_init(hostStr.c_str(), CloudAgent_watcher_global,
		                            timeout, NULL, this, 0);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Failed to create handle of zookeeper...");
			return false;
		}

		refreshChangedMap();

		// Bmco::Int32 recv_time = zoo_recv_timeout(m_zkHandle);
		// bmco_information_f1(theLogger, "recv_time = %?d", recv_time);

		//bool bRetVal0 = false;
		//int opPetriFlag = m_config.getInt("cloudpetri.opflag");
		//CFileOperInterface fileOperIntf;
		//std::string petriResultFile0;
		//petriResultFile0 = m_config.getString("cloudpetri.path") + "DrawID_100007.result";

		//if (!createGroupNode()
		//{
		//	return false;
		//}
		
		return true;
	}

	bool CloudDatabaseHandler::createGroupNode()
	{
	    std::string sBolName = m_config.getString(CONFIG_INFO_BOL_NAME);
	    std::string sNodePath = Bmco::format("%s%s%s",std::string(ZNODE_ZOOKEEPER_SLAVE_DIR),std::string("/"),sBolName);
		if (!createNode(sNodePath, ZOO_EPHEMERAL))
		{
			bmco_error(theLogger, "Failed to create node, the zookeeper server may be broken down!");
			return false;
		}

		return true;
	}

	void CloudDatabaseHandler::clearZkHandle() 
	{	
		ECLUSIVE_LOCK _threadguard(rwlock);
		bmco_information(theLogger, "CloudDatabaseHandler::clearZkHandle() called");
		if (m_zkHandle != NULL)
		{
			int ret = zookeeper_close(m_zkHandle);
			m_zkHandle = NULL;
			bmco_information_f1(theLogger, "CloudAgent_watcher_global with: [%d]", ret);
		}

		/*if (m_fp != NULL)
		{
			fclose(m_fp);
		}*/
	}

	int CloudDatabaseHandler::createRootNodeAsync(const SyncData &sync)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection to zookeeper server is establishted...");
			return ZINVALIDSTATE;
		}
		
		bmco_information_f1(theLogger, "Create first level node: [%s]", sync.tmpPath);

		int ret = zoo_acreate(m_zkHandle, (char *)sync.tmpPath.c_str(), NULL, 0,
		                      &ZOO_OPEN_ACL_UNSAFE, sync.curFlag,
		                      createNodeCompletion, (void *)&sync);

		return ret;
	}

	bool CloudDatabaseHandler::createNode(std::string nodePath, int flag)
	{
		bmco_information_f2(theLogger, "Create node: [%s], flag: [%d]", nodePath, flag);
		
		if (nodePath[0] != '/' || nodePath[nodePath.size() - 1] == '/')
		{
			bmco_error_f1(theLogger, "Error node path [%s], it should begin with '/' and cannot be end with '/'", nodePath);
			return false;
		}
		
		SyncData sync(m_zkHandle, theLogger);
		size_t pos = nodePath.find('/', 1);
		if (pos == std::string::npos || pos == nodePath.size() - 1)
		{
			sync.tmpPath = nodePath;
			sync.curFlag = ZOO_EPHEMERAL;
		}
		else if (pos < nodePath.size())
		{
			sync.tmpPath = nodePath.substr(0, pos);
			sync.curFlag = 0;
		}
		
		sync.data = nodePath;
		sync.flag = flag;

		int ret = createRootNodeAsync(sync);
		if (ZOK != ret)
		{
			bmco_error_f1(theLogger, "Error get the configure value: [%d]", ret);
			return false;
		}
		
		//boost::unique_lock<boost::mutex> lock(sync.mutex);
		//sync.condition.wait(lock);
		sync.wait();
		return sync.isDataReady;
	}
	bool CloudDatabaseHandler::nodeExist(std::string nodePath)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		bmco_debug_f1(theLogger, "if nodeExist: [%s]", nodePath);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection to zookeeper server is establishted...");
			return false;
		}
		struct Stat s;
		int ret = zoo_exists(m_zkHandle, nodePath.c_str(), 0, &s);
		if (ret == ZOK)
		{
			return true;
		}
		else
		{
			bmco_error_f1(theLogger, "zoo_exists [%d]", ret);
			return false;
		}
	}
	int CloudDatabaseHandler::getNodeData(std::string nodePath, std::string& data)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection to zookeeper server is establishted...");
			return -1;
		}
		bmco_debug_f1(theLogger, "CloudDatabaseHandler::getNodeData() with: [%s]", nodePath);
		SyncData sync(m_zkHandle, theLogger);
		int len = 100 * 1024;
		char *buf = new char[len];
		int ret = zoo_get(m_zkHandle, (char *)nodePath.c_str(), 0, buf, &len, NULL);
		if (ret != ZOK)
		{
			bmco_error_f1(theLogger, "Error get the configure value: [%d]", ret);
			delete [] buf;
			return -1;
		}
		bmco_debug_f1(theLogger, "data length: %d", len);
		if (len > 0)
		{
			data.assign(buf, len);
		}
		delete [] buf;
		return 0;
	}

	/*int CloudDatabaseHandler::getChangedData(std::string nodePath, std::string& data)
	{
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check out if the connection with zookeeper server is all right...");
			return -1;
		}
		bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedData() with: [%s]", nodePath);
		SyncData sync(m_zkHandle, theLogger);
		//sync->nodePath = nodePath;
		int ret = ret = zoo_awget(m_zkHandle, nodePath.c_str(), changedNodeDataWatcher, (void *)&sync,
		                          nodeChangedDataCompletion,  (void *)&sync);
		if (ret != ZOK)
		{
			bmco_error_f1(theLogger, "Error get the configure value: [%d]", ret);
			return -1;
		}

		boost::unique_lock<boost::mutex> lock(sync.mutex);
		sync.condition.wait(lock);
		data = sync.data;

		return 0;
	}*/
	
	bool CloudDatabaseHandler::setNodeData(std::string nodePath, std::string buffer)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection with zookeeper server is all right...");
			return false;
		}
		bmco_debug_f1(theLogger, "CloudDatabaseHandler::setNodeData() with: [%s]", nodePath);
		int ret = zoo_set(m_zkHandle, (char *)nodePath.c_str(), (char *)buffer.c_str(), buffer.length(), -1);
		if (ret != ZOK)
		{
			bmco_error_f1(theLogger, "Error set the value of the node: %d", ret);
			return false;
		}
		return true;
	}
	/* int CloudDatabaseHandler::getSlaveNumber()
	{
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection with zookeeper server is all right...");
			return false;
		}
		String_vector nodes;
		int ret = zoo_get_children(m_zkHandle, m_config.getString("zkConf.zookeeper.slaveNodeDir").c_str(), 0, &nodes);
		if (ret == ZOK)
		{
			return nodes.count;
		}
		else
		{
			return -1;
		}
	}*/
	bool CloudDatabaseHandler::GetChildrenNode(std::string nodePath, std::vector<std::string>& children)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check out if the connection with zookeeper server is all right...");
			return false;
		}
		struct String_vector nodes;
		memset(&nodes, 0, sizeof(nodes));		
		int ret = zoo_get_children(m_zkHandle, nodePath.c_str(), 0, &nodes);
		if (ret == ZOK)
		{
			for (int i=0; i<nodes.count; i++) 
			{
                children.push_back(nodes.data[i]);
            }
			deallocate_String_vector(&nodes);
		}
		else
		{
			// deallocate_String_vector(&nodes);
			bmco_error(theLogger, "GetChildrenNode failed!");
			return false;
		}
		
		return true;
	}

	bool CloudDatabaseHandler::getChangedData(std::string nodePath)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check out if the connection with zookeeper server is all right...");
			return false;
		}
		//bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedData() with: [%s]", nodePath);
		SyncData sync(m_zkHandle, theLogger);
		sync.nodePath = nodePath;
		int ret = ret = zoo_awget(m_zkHandle, nodePath.c_str(), changedNodeDataWatcher, (void *)&sync,
								  nodeChangedDataCompletion,  (void *)&sync);
		if (ret != ZOK)
		{
			bmco_error_f1(theLogger, "Error get the configure value: [%d]", ret);
			return false;
		}

		return true;
	}

	// º‡ ”Ω⁄µ„µƒ±‰ªØ
	bool CloudDatabaseHandler::getNodeChangedFlag(std::string nodePath, bool& isChanged)
	{
		// bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedDataAsync[%s]", nodePath);
		std::map<std::string, bool>::iterator it;

		isChanged = false;
		// ≈–∂œº‡ ”’‚∏ˆnodePathµƒ µÃÂ «∑ÒΩ®¡¢
		it = m_changePath.find(nodePath);

		// “—æ≠¥¥Ω®¡Àº‡ ” µÃÂ
		if (it != m_changePath.end())
		{
			// ºÃ–¯π€≤Ï «∑Ò“—æ≠”…zookeeperªÿµ˜¥¶¿Ì¡À
			// ªπ√ª”–±ª¥¶¿Ì£¨∑µªÿºÃ–¯œ¬∏ˆ—≠ª∑
			if (!it->second)
			{
				return true;
			}
			bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedDataAsync [%s] notify", 
				nodePath);
			isChanged = true;
			m_changePath[nodePath] = false;
			// ◊Ë»˚“Ï≤ΩªÒ»°±‰ªØµƒ ˝æ›
			if (!getChangedData(nodePath))
			{
				return false;
			}
		}
		// µ⁄“ª¥Œ√ª”–‘Ú¥¥Ω®º‡ ” µÃÂ
		else
		{
			bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedToData[%s]", nodePath);
			// ◊¢≤·Ω®¡¢º‡ ” µÃÂ
			m_changePath.insert(std::pair<std::string, bool>(nodePath, false));
			// ◊Ë»˚“Ï≤ΩªÒ»°±‰ªØµƒ ˝æ›
			if (!getChangedData(nodePath))
			{
				return false;
			}
		}

		return true;
	}
	
	bool CloudDatabaseHandler::removeNode(std::string nodePath)
	{
		SHARABLE_LOCK _threadguard(rwlock);
		if (m_zkHandle == NULL)
		{
			bmco_error(theLogger, "Please check if the connection to zookeeper server is establishted...");
			return false;
		}
		int ret = zoo_delete(m_zkHandle, nodePath.c_str(), -1);
		if (ret == ZOK)
		{
			return true;
		}
		else
		{
			bmco_error_f1(theLogger, "removeNode ret = %?d", ret);
			return false;
		}
	} 

	void CloudDatabaseHandler::releaseStaticHandle()
	{
		if (_pInstance != NULL)
		{
			delete _pInstance;
			_pInstance = NULL;
		}		
	}

	void CloudDatabaseHandler::rebuildLogFile()
	{
		Path p2(m_logPath);
		File file(p2);
		Bmco::UInt64 size = file.getSize();
		if (100*1024*1024 < size)
		{
			if (m_fp != NULL)
			{
				fclose(m_fp);
			}
			
			file.remove();
			file.createFile();

			setZKLogFilePoint();
		}
	}

	void CloudDatabaseHandler::refreshChangedMap()
	{
		// zookeeper÷ÿ¡¨∫Û–Ë“™÷ÿ–¬◊¢≤·º‡Ã˝Watcher£¨ƒ¨»œ‘⁄∂œ¡¨∆⁄º‰ ˝æ›”–±‰ªØ°
		// ÷ÿ–¬À¢–¬‘∆∂À ˝æ›µΩ∫ÛÃ®
		std::map<std::string, bool>::iterator it = m_changePath.begin();
		for (;it != m_changePath.end();it++)
		{
			it->second = true;
		}
	}

	/*bool CloudDatabaseHandler::getChangedDataAsync(std::string nodePath, std::string& data)
	{
		bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedDataAsync[%s]", nodePath);
		std::map<std::string, MonitorData>::iterator it;
		// ≈–∂œº‡ ”’‚∏ˆnodePathµƒ µÃÂ «∑ÒΩ®¡¢
		it = m_dataVec.find(nodePath);

		// “—æ≠¥¥Ω®¡Àº‡ ” µÃÂ
		if (it != m_dataVec.end())
		{
			bmco_information(theLogger, "CloudDatabaseHandler::hasnot solve");
			// ºÃ–¯π€≤Ï «∑Ò“—æ≠”…zookeeperªÿµ˜¥¶¿Ì¡À
			// ªπ√ª”–±ª¥¶¿Ì£¨∑µªÿºÃ–¯œ¬∏ˆ—≠ª∑
			if (!it->second.notifyReady)
			{
				return true;
			}
			bmco_information(theLogger, "CloudDatabaseHandler::getChangedDataAsync notify");
			bmco_information_f1(theLogger, "CloudDatabaseHandler::changeddata[%s]", nodePath);
			data = it->second.data;
			m_dataVec.erase(it);
		}
		// √ª”–‘Ú¥¥Ω®º‡ ” µÃÂ
		else
		{
			bmco_information_f1(theLogger, "CloudDatabaseHandler::getChangedToData[%s]", nodePath);
			// ◊Ë»˚“Ï≤ΩªÒ»°±‰ªØµƒ ˝æ›
			if (!getChangedData(nodePath))
			{
				return false;
			}
			
			data = "";
			// ◊¢≤·Ω®¡¢º‡ ” µÃÂ
			MonitorData tempData;
			tempData.data = "";
			tempData.notifyReady = false;
			m_dataVec.insert(std::pair<std::string, MonitorData>(nodePath, tempData));
		}

		return true;
	}*/
}
