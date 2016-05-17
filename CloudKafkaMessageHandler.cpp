#include "CloudKafkaMessageHandler.h"
#include "Bmco/NumberFormatter.h"
#include "Bmco/Util/IniFileConfiguration.h"
//#include <boost/algorithm/string.hpp>
#include "Bmco/StringTokenizer.h"
#include "Bmco/NumberParser.h"
#include "ailk.KafkaMsg.pb.h"

namespace BM35
{	
	CloudKafkaMessageHandler *CloudKafkaMessageHandler::_pInstance = NULL;
	boost::mutex CloudKafkaMessageHandler::_instanceMutex;
	bool CloudKafkaMessageHandler::init(std::string brokerList, 
			std::string zookeeperList)
	{
		/*if (!add_consume_new(brokerList, zookeeperList))
		{
			return false;
		}

		if (!add_worker_new(brokerList, zookeeperList))
		{
			return false;
		}*/

		if (m_brokerList != brokerList
			|| m_zookeeperList != zookeeperList)
		{
			releaseKafkaHandle();
			if (!add_consume_new(brokerList, zookeeperList))
			{
				return false;
			}

			m_brokerList = brokerList;
			m_zookeeperList = zookeeperList;

			/*if (!add_worker_new(brokerList, zookeeperList))
			{
				return false;
			}*/
		}
		
		return true;
	}

	bool CloudKafkaMessageHandler::add_consume_new(std::string brokerList, 
			std::string zookeeperList)
	{
		if (m_consume == NULL)
		{
			dmsg::Config *config = new dmsg::Config();;
			config->setConf(std::string("queue.buffering.max.messages"), std::string("1"));
			config->setConf(std::string("batch.num.messages"), std::string("1"));
			m_consume = new dmsg::Consumer(config);
			m_brokerList = brokerList;
			m_zookeeperList = zookeeperList;
			if (!m_consume->init(m_brokerList, m_zookeeperList))
			{
				bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not init successfully", m_brokerList);
				return false;
			}
			if (NULL == m_consume)
			{
				bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not connect successfully", m_brokerList);
				return false;
			}

			sleep(1);
		}

		return true;
	}

	/*bool CloudKafkaMessageHandler::add_worker_new(std::string brokerList, 
			std::string zookeeperList)
	{
		if (m_worker == NULL)
		{
			m_worker = new kafkacpp::KafkaWorker(RD_KAFKA_CONSUMER);
			m_brokerList = brokerList;
			m_zookeeperList = zookeeperList;
			if (!m_worker->init(m_brokerList, m_zookeeperList))
			{
				bmco_error_f1(theLogger, "The kafka brokerList:%s worker does not init successfully", m_brokerList);
				return false;
			}
			if (NULL == m_worker)
			{
				bmco_error_f1(theLogger, "The kafka brokerList:%s worker does not connect successfully", m_brokerList);
				return false;
			}
		}

		return true;
	}*/

	// 获取本主题分区的消息总数
	/*bool CloudKafkaMessageHandler::calcuMessageGrossByPartition(std::string topicName,
		 	std::string partitionName, Bmco::Int64 &offset)
	{
		Bmco::Int64 partiton = Bmco::NumberParser::parse64(partitionName);
		kafkacpp::Consumer tConsume;
		if (!tConsume.init(m_brokerList, m_zookeeperList))
		{
			bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not init successfully", m_brokerList);
			//delete tConsume;
			//tConsume = NULL;
			return false;
		}
		//if (NULL == tConsume)
		//{
		//	bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not connect successfully", m_brokerList);
		//	return false;
		//}
		offset = tConsume.getPartitionOffset(topicName, partiton);
		// offset = m_consume->getPartitionOffset(topicName, partiton);
		if (-1 == offset)
		{
			bmco_error_f2(theLogger, "topic %s partition %?d getPartitionOffset failed",
				topicName, partiton);
			//delete tConsume;
			//tConsume = NULL;
			return false;
		}
		
		//delete tConsume;
		//tConsume = NULL;
		return true;
	}*/
	bool CloudKafkaMessageHandler::calcuMessageGrossByPartition(std::string topicName,
	 	std::string partitionName, Bmco::Int64 &offset)
	{
		Bmco::Int64 partiton = Bmco::NumberParser::parse64(partitionName);
		// setLastTestTimeStamp();
		offset = m_consume->getPartitionOffset(topicName, partiton, 500);
		// getTestIntervalTime(topicName);
		if (0 > offset)
		{
			bmco_error_f3(theLogger, "topic %s partition %?d getPartitionOffset %?d failed",
				topicName, partiton, offset);
			return false;
		}
		
		return true;
	}

	/*bool CloudKafkaMessageHandler::calcuReadMessageOffsetByPartition(std::string topicName,
	 	std::string partitionName, Bmco::Int64 &offset)
	{
		Bmco::Int64 partiton = Bmco::NumberParser::parse64(partitionName);
		char *buf = NULL;
		size_t len = 0;
		int64_t iTempOffset = -1;
		offset = -1;
		
		kafkacpp::Consumer tConsume;
		if (!tConsume.init(m_brokerList, m_zookeeperList))
		{
			bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not init successfully", m_brokerList);
			//delete tConsume;
			//tConsume = NULL;
			return false;
		}
		//if (NULL == tConsume)
		//{
		//	bmco_error_f1(theLogger, "The kafka brokerList:%s consumer does not connect successfully", m_brokerList);
		//	return false;
		//}
		Bmco::Int64 ConsumerStartOffset = 0;
		ConsumerStartOffset = tConsume.getPartitionOffset(topicName, partiton);
		if (-1 == ConsumerStartOffset)
		{
			offset = 0;
			bmco_information_f3(theLogger, "topic %s partition %?d BEGIN offset : %?d", 
				topicName, partiton, offset);
			//delete tConsume;
			//tConsume = NULL;
			return true;
		}
		
		// 在最后放入的消息，也就是上游的读偏移量
		if (0 != tConsume.consumeOneMessage(topicName, partiton, ConsumerStartOffset, 500, buf, len, iTempOffset))
		{
			bmco_error_f2(theLogger, "topic %s partition %?d consumeOneMessage failed",
				topicName, partiton);
			if (buf != NULL)
			{
				free(buf);
			}
			//delete tConsume;
			//tConsume = NULL;
			return false;
		}

		ailk::KafkaMsg_pb kafkaMsg;
        if (!kafkaMsg.ParseFromArray(buf, len))
        {
	    	free(buf);
            return false;
        }
        offset = kafkaMsg.lposition();
		free(buf);

		// if (8 > len) 
		// {
		//	 bmco_error(theLogger, "len less than 8");
			 //delete tConsume;
			 //tConsume = NULL;
		//	 return false;
		// }
		
		// memcpy(&offset, buf, 8);
		
		bmco_information_f2(theLogger, "len %?d | offset %?d", len, offset);
		//delete tConsume;
		//tConsume = NULL;

		return true;
	}*/
	bool CloudKafkaMessageHandler::calcuReadMessageOffsetByPartition(std::string topicName,
	 	std::string partitionName, Bmco::Int64 &offset)
	{
		Bmco::Int64 partiton = Bmco::NumberParser::parse64(partitionName);
		char *buf = NULL;
		size_t len = 0;
		int64_t iTempOffset = -1;
		offset = -1;

		Bmco::Int64 ConsumerStartOffset = 0;
		// setLastTestTimeStamp();
		ConsumerStartOffset = m_consume->getPartitionOffset(topicName, partiton, 500);
		// getTestIntervalTime(topicName);
		if (-1 == ConsumerStartOffset)
		{
			offset = 0;
			bmco_information_f3(theLogger, "topic %s partition %?d BEGIN offset : %?d", 
				topicName, partiton, offset);
			return true;
		}

		// m_consume->setConsumeStartOffset(topicName, partiton, ConsumerStartOffset);
		// setLastTestTimeStamp();
		// 在最后放入的消息，也就是上游的读偏移量
		if (0 != m_consume->consumeOneMessage(topicName, partiton, ConsumerStartOffset, 500, buf, len, iTempOffset))
		{
			// getTestIntervalTime(topicName);
			m_consume->resetConsumeStat(topicName, partiton);
			bmco_error_f2(theLogger, "topic %s partition %?d consumeOneMessage failed",
				topicName, partiton);
			if (buf != NULL)
			{
				free(buf);
			}
			return false;
		}
		// getTestIntervalTime(topicName);

		m_consume->resetConsumeStat(topicName, partiton);

		ailk::KafkaMsg_pb kafkaMsg;
        if (!kafkaMsg.ParseFromArray(buf, len))
        {
        	bmco_error_f2(theLogger, "kafkaMsg.ParseFromArray failed | len %?d | buf %s", len, buf);
	    	free(buf);
            return false;
        }
        offset = kafkaMsg.lposition();
		free(buf);
		
		bmco_information_f2(theLogger, "len %?d | offset %?d", len, offset);

		return true;
	}

	/*Bmco::Int32 CloudKafkaMessageHandler::getPartitionCount(std::string topicName)
	{
		Bmco::Int32 num = m_worker->getPartitionCount(topicName);

		if (0 > num)
		{
			return 0;
		}

		return num;
	}*/

	void CloudKafkaMessageHandler::releaseStaticHandle()
	{
		if (_pInstance != NULL)
		{
			delete _pInstance;
			_pInstance = NULL;
		}	
	}

	void CloudKafkaMessageHandler::releaseKafkaHandle()
	{
		if (m_consume != NULL)
		{
			delete m_consume;
			m_consume = NULL;
		}

		/*if (m_worker != NULL)
		{
			delete m_worker;
			m_worker = NULL;
		}*/
	}

	std::string CloudKafkaMessageHandler::getZookeeperList()
	{
		if (_pInstance == NULL) 
		{
			return "";
		}
		
		return m_zookeeperList;
	}

	/*void CloudKafkaMessageHandler::setLastTestTimeStamp() 
	{
	    _lastTestTimeStamp.update();
	}

	void CloudKafkaMessageHandler::getTestIntervalTime(std::string topicName) 
	{
	    Bmco::Timespan timeCost = _lastTestTimeStamp.elapsed();
		int interval = timeCost.totalMilliseconds();
		bmco_information_f2(theLogger, "topic: %s useTime: %?d\n", topicName, interval);
	}*/
}
