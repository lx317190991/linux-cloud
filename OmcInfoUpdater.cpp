#include <string>
#include <vector>
#include "Bmco/NumberFormatter.h"
#include "OmcInfoOp.h"
//#include "OmcInfoUpdater.h"

namespace BM35 {

const std::string TASK_ID_MASTER_CONFIG = "Cfg_GetMasterInfo.OFCS";
const std::string TASK_ID_SLAVE_CONFIG = "Cfg_GetSlaveInfo.OFCS";
const std::string TASK_ID_MS_RELATION = "Cfg_GetMSRelInfo.OFCS";
const std::string TASK_ID_STORAGE = "Cfg_GetStorageInfo.OFCS";
const std::string TASK_ID_SLAVE_STAT = "Perf_GetSlaveInfo.OFCS";
const std::string TASK_ID_MASTER_INVALID = "Ala_Master Invalid.OFCS";
const std::string TASK_ID_SLAVE_INVALID = "Ala_Slave Invalid.OFCS";
const std::string TASK_ID_MASTER_START = "Eve_MasterStartUse.OFCS";
const std::string TASK_ID_MASTER_STOP = "Eve_MasterStopUse.OFCS";
const std::string TASK_ID_SLAVE_START = "Eve_SlaveStartUse.OFCS";
const std::string TASK_ID_SLAVE_STOP = "Eve_SlaveStopUse.OFCS";

const std::string FIELD_MASTER_NAME = "MASTER_NAME";
const std::string FIELD_STAT_TIME = "STAT_TIME";
const std::string FIELD_SLAVE_NAME = "SLAVE_NAME";
const std::string FIELD_HOST_NAME = "HOST_NAME";
const std::string FIELD_IP_ADDR = "IPADDR";
const std::string FIELD_STATE = "STATE";
const std::string FIELD_NODE_ID = "NODE_ID";
const std::string FIELD_NET_HOST = "NE_HOST";
const std::string FIELD_STORAGE_SIZE = "STORAGE_SIZE";


int OmcInfoUpdater::omcStorage(std::vector<std::string> &vNode, std::vector<std::string> &vNetHost,
    std::vector<std::string> &vStorage) {
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));

    // clear old data.    
    std::vector<Information> vInfo;
    if (!p->Query(TASK_ID_STORAGE, FIELD_NODE_ID, vInfo)) {
        bmco_error(m_logger, "Failed to query FIELD_NODE_ID definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_STORAGE, FIELD_NET_HOST, vInfo)) {
        bmco_error(m_logger, "Failed to query FIELD_NET_HOST definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_STORAGE, FIELD_STORAGE_SIZE, vInfo)) {
        bmco_error(m_logger, "Failed to query FIELD_STORAGE_SIZE definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }

    // insert new data.
    Bmco::Timestamp now;
    for (int i=0; i<vNode.size(); i++) {
        Information info(TASK_ID_STORAGE.c_str(), FIELD_NODE_ID.c_str(), vNode[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vNetHost.size(); i++) {
        Information info(TASK_ID_STORAGE.c_str(), FIELD_NET_HOST.c_str(), vNetHost[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vStorage.size(); i++) {
        Information info(TASK_ID_STORAGE.c_str(), FIELD_STORAGE_SIZE.c_str(), vStorage[i].c_str(), now, 0);
        p->Insert(&info);
    }
    

    return 0;
}

int OmcInfoUpdater::omcMasterSlaveRel(std::vector<std::string> &vMaster, std::vector<std::string> &vSlave) {
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));

    // clear old data.    
    std::vector<Information> vInfo;
    if (!p->Query(TASK_ID_MS_RELATION, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query MASTER_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_MS_RELATION, FIELD_SLAVE_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query HOST_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }

    // insert new data.
    Bmco::Timestamp now;
    for (int i=0; i<vMaster.size(); i++) {
        Information info(TASK_ID_MS_RELATION.c_str(), FIELD_MASTER_NAME.c_str(), vMaster[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vSlave.size(); i++) {
        Information info(TASK_ID_MS_RELATION.c_str(), FIELD_MASTER_NAME.c_str(), vSlave[i].c_str(), now, 0);
        p->Insert(&info);
    }
    

    return 0;
}

int OmcInfoUpdater::omcMasterConfig(std::vector<std::string> &vName, std::vector<std::string> &vHost,
    std::vector<std::string> &vIp, std::vector<std::string> &vState) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));

    if (p == NULL) {
        bmco_error(m_logger, "Empty pointer of OmcInfoOp.");
        return -1;
    }
    // clear old data. 
    if (!p->Query(TASK_ID_MASTER_CONFIG, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query MASTER_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_MASTER_CONFIG, FIELD_HOST_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query HOST_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_MASTER_CONFIG, FIELD_IP_ADDR, vInfo)) {
        bmco_error(m_logger, "Failed to query IPADDR definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_MASTER_CONFIG, FIELD_STATE, vInfo)) {
        bmco_error(m_logger, "Failed to query STATE definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }

    // insert new data.
    Bmco::Timestamp now;
    for (int i=0; i<vName.size(); i++) {
        Information info(TASK_ID_MASTER_CONFIG.c_str(), FIELD_MASTER_NAME.c_str(), vName[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vHost.size(); i++) {
        Information info(TASK_ID_MASTER_CONFIG.c_str(), FIELD_HOST_NAME.c_str(), vHost[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vIp.size(); i++) {
        Information info(TASK_ID_MASTER_CONFIG.c_str(), FIELD_IP_ADDR.c_str(), vIp[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vState.size(); i++) {
        Information info(TASK_ID_MASTER_CONFIG.c_str(), FIELD_STATE.c_str(), vState[i].c_str(), now, 0);
        p->Insert(&info);
    }

    return 0;
}

int OmcInfoUpdater::omcSlaveConfig(std::vector<std::string> &vName, std::vector<std::string> &vHost,
    std::vector<std::string> &vIp, std::vector<std::string> &vState) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));

    // clear old data.    
    if (!p->Query(TASK_ID_SLAVE_CONFIG, FIELD_SLAVE_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query SLAVE_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_SLAVE_CONFIG, FIELD_HOST_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query HOST_NAME definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_SLAVE_CONFIG, FIELD_IP_ADDR, vInfo)) {
        bmco_error(m_logger, "Failed to query IPADDR definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }
    vInfo.clear();
    if (!p->Query(TASK_ID_SLAVE_CONFIG, FIELD_STATE, vInfo)) {
        bmco_error(m_logger, "Failed to query STATE definition.");
        return -1;
    }
    for (int i=0; i<vInfo.size(); i++) {
        p->Delete(vInfo[i].ulRowID);
    }

    // insert new data.
    Bmco::Timestamp now;
    for (int i=0; i<vName.size(); i++) {
        Information info(TASK_ID_SLAVE_CONFIG.c_str(), FIELD_SLAVE_NAME.c_str(), vName[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vHost.size(); i++) {
        Information info(TASK_ID_SLAVE_CONFIG.c_str(), FIELD_HOST_NAME.c_str(), vHost[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vIp.size(); i++) {
        Information info(TASK_ID_SLAVE_CONFIG.c_str(), FIELD_IP_ADDR.c_str(), vIp[i].c_str(), now, 0);
        p->Insert(&info);
    }
    for (int i=0; i<vState.size(); i++) {
        Information info(TASK_ID_SLAVE_CONFIG.c_str(), FIELD_STATE.c_str(), vState[i].c_str(), now, 0);
        p->Insert(&info);
    }
    
    return 0;
}

int OmcInfoUpdater::omcMasterInvalid(std::string &masterName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_MASTER_INVALID, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_MASTER_INVALID.c_str(), FIELD_MASTER_NAME.c_str(),
        masterName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_MASTER_INVALID.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}

int OmcInfoUpdater::omcSlaveInvalid(std::string &slaveName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_SLAVE_INVALID, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_SLAVE_INVALID.c_str(), FIELD_MASTER_NAME.c_str(),
        slaveName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_SLAVE_INVALID.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}

int OmcInfoUpdater::omcMasterStart(std::string &masterName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_MASTER_START, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_MASTER_START.c_str(), FIELD_MASTER_NAME.c_str(),
        masterName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_MASTER_START.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}


int OmcInfoUpdater::omcMasterStop(std::string &masterName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_MASTER_STOP, FIELD_MASTER_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_MASTER_STOP.c_str(), FIELD_MASTER_NAME.c_str(),
        masterName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_MASTER_STOP.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}

int OmcInfoUpdater::omcSlaveStart(std::string &slaveName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_SLAVE_START, FIELD_SLAVE_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_SLAVE_START.c_str(), FIELD_SLAVE_NAME.c_str(),
        slaveName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_SLAVE_START.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}

int OmcInfoUpdater::omcSlaveStop(std::string &slaveName) {
    std::vector<Information> vInfo;
    OmcInfoOp *p = dynamic_cast<OmcInfoOp *>(
        m_ctlOper->getObjectPtr(OmcInfoOp::getObjName()));


    // Process "Eve_SlaveStopUse.OFCS".
    if (!p->Query(TASK_ID_SLAVE_STOP, FIELD_SLAVE_NAME, vInfo)) {
        bmco_error(m_logger, "Failed to query OmcInfoOp definition.");
        return -1;
    }
    Bmco::UInt64 maxRowId = 0;
    for (int i=0; i<vInfo.size(); i++) {
        if (vInfo[i].ulRowID > maxRowId) {
            maxRowId = vInfo[i].ulRowID;
        }
    }
    maxRowId++;
    Bmco::Timestamp now;
    Information pName(TASK_ID_SLAVE_STOP.c_str(), FIELD_SLAVE_NAME.c_str(),
        slaveName.c_str(), now, maxRowId);
    p->Insert(&pName);
    Information pTime(TASK_ID_SLAVE_STOP.c_str(), FIELD_STAT_TIME.c_str(),
        Bmco::NumberFormatter::format(now.epochTime()).c_str(), now, maxRowId);
    p->Insert(&pTime);

    return 0;
}




}

