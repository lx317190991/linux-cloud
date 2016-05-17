#ifndef _OMCINFOUPDATER_HH__
#define _OMCINFOUPDATER_HH__

#include <Bmco/Util/Application.h>
#include "ControlRegionOp.h"


namespace BM35 {

class OmcInfoUpdater {
public:

    OmcInfoUpdater(ControlRegionOp::Ptr& ctlOper) :
        m_ctlOper(ctlOper), m_logger(Bmco::Util::Application::instance().logger()) {
    }

    ~OmcInfoUpdater() {
    }

    int omcStorage(std::vector<std::string> &vNode, std::vector<std::string> &vNetHost,
        std::vector<std::string> &vStorage);

    int omcMasterSlaveRel(std::vector<std::string> &vMaster, std::vector<std::string> &vSlave);

    int omcMasterConfig(std::vector<std::string> &vName, std::vector<std::string> &vHost,
        std::vector<std::string> &vIp, std::vector<std::string> &vState);

    int omcSlaveConfig(std::vector<std::string> &vName, std::vector<std::string> &vHost,
        std::vector<std::string> &vIp, std::vector<std::string> &vState);

    int omcMasterInvalid(std::string &masterName);

    int omcSlaveInvalid(std::string &slaveName);

    int omcMasterStart(std::string &masterName);

    int omcMasterStop(std::string &masterName);

    int omcSlaveStart(std::string &slaveName);

    int omcSlaveStop(std::string &slaveName);


private:
    ControlRegionOp::Ptr& m_ctlOper;
    Bmco::Logger &m_logger;
};

}

#endif //_OMCINFOUPDATER_HH__
