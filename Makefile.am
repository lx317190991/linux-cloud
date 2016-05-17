AUTOMAKE_OPTIONS=foreign

bin_PROGRAMS=CloudAgent

BOOSTINCLUDEDIR ?= /usr/local/include

BOOSTLIBDIR ?= /usr/local/lib

BMCOINCLUDEDIR ?= /usr/local/include

BMCOLIBDIR ?= /usr/local/lib

ZOOKEEPERLIBDIR ?= /usr/local/lib

ZOOKEEPERINCLUDEDIR ?= /usr/local/include

KAFKALIBDIR ?= /usr/local/lib

KAFKAINCLUDEDIR ?= /usr/local/include

BOLINCLUDEDIR ?= ../../bol

HLAINCLUDEDIR ?= ../../hla


CloudAgent_THECPPS = ailk.KafkaMsg.pb.cc CloudAgentConstants.cpp CloudAgentMain.cpp CloudConfigurationUpdater.cpp CloudScheduleTaskLoop.cpp \
	CloudCmdTaskLoop.cpp CloudDatabaseHandler.cpp  IniFileConfigurationNew.cpp CloudKafkaMessageHandler.cpp CloudDynamicFlexTaskLoop.cpp CloudXMLParseHandler.cpp

ailk.KafkaMsg.pb.cc:
	protoc -I=./ --cpp_out=./  ailk.KafkaMsg.proto

CloudAgent_SOURCES = $(CloudAgent_THECPPS) 

THEHEADERS = CloudAgentConstants.h CloudAgentHeartBeatLoop.h CloudConfigurationUpdater.h CloudScheduleTaskLoop.h \
	CloudCmdTaskLoop.h CloudDatabaseHandler.h  IniFileConfigurationNew.h CloudKafkaMessageHandler.h CloudDynamicFlexTaskLoop.h CloudXMLParseHandler.h

include_HEADERS = $(THEHEADERS)

CloudAgent_LDADD = ${KAFKALIBDIR}/libdmsg.a ${KAFKALIBDIR}/libprotobuf.a

CloudAgent_CPPFLAGS = ${DEV_INCLUDES} -I ${BOLINCLUDEDIR}/../../../include -I ${BOLINCLUDEDIR}/../../../etc -I ${BMCOINCLUDEDIR}/Util/include -I ${BMCOINCLUDEDIR}/Net/include -I ${BOOSTINCLUDEDIR} -I ${BMCOINCLUDEDIR} -I ${BOLINCLUDEDIR} -I ../ -I ${BOLINCLUDEDIR}/BolCommon \ 
						-I ${ZOOKEEPERINCLUDEDIR} -I ${ZOOKEEPERINCLUDEDIR}/../generated -I ${KAFKAINCLUDEDIR} -I ${KAFKAINCLUDEDIR}/../dep/include/librdkafka 

CloudAgent_LDFLAGS = ${DEV_LDADDS} -L${BOLINCLUDEDIR}/../../../lib -L/usr/local/lib  -lBolCommon \
    -lboost_date_time  -lboost_system  -lBmcoUtil -lBmcoData \
    -lBmcoFoundation -lBmcoXML -lBmcoNet  -lzookeeper_mt \
    -lpthread -ljsoncpp
