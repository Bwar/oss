/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObjectManager.cpp
* Description: OSS对象管理类
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/


#include "OssObjectManager.hpp"


oss::COssObjectManager::COssObjectManager(
        const loss::tagStatOrder& stStatOrder,
        oss::UINT uiLoadDataConnType,
        const std::string& strInsertMode)
{
    m_pStatOrder = new loss::tagStatOrder(stStatOrder);

    m_pPara = new loss::CStatPara(stStatOrder.m_szProcessName, stStatOrder.m_szArgv);
    m_pLog = new loss::COssLog(loss::Cfg::Instance()->GetOssLogDbHandler(),
            m_pStatOrder->m_szBusinessFlag,
            m_pStatOrder->m_szHostName,
            m_pStatOrder->m_szProcessName,
            loss::Cfg::Instance()->GetLogPath(),
            m_pPara->GetParaHandler()->GetLogLevel());
    if (m_pPara->GetParaHandler()->GetErrCode())
    {
        m_pLog->WriteLog(loss::FATAL, "%s", m_pPara->GetParaHandler()->GetErrMsg().c_str());
        exit(m_pPara->GetParaHandler()->GetErrCode());
    }

    m_uiOptWorldNum = m_pPara->GetParaHandler()->GetWorldId(m_vecOptWorldId);
    m_strBusinessFlag = stStatOrder.m_szBusinessFlag;
    m_uiLoadDataConnType = uiLoadDataConnType;
    m_pThread = new loss::CThread<oss::COss>();
    m_pLog->WriteLog(loss::NOTICE, "DATE %s %s PROGRAM START...",
        GetStatTargetDate().c_str(), m_pStatOrder->m_szProcessName);
    m_iterWorldId = m_vecConfWorldId.begin();
}

oss::COssObjectManager::~COssObjectManager()
{
    ClearTask();
    LoadDataLocal();
    m_pLog->WriteLog(loss::NOTICE, "DATE %s %s PROGRAM FINISHED.",
        GetStatTargetDate().c_str(), m_pStatOrder->m_szProcessName);
    if (m_pPara != NULL)
    {
        delete m_pPara;
        m_pPara = NULL;
    }
    if (m_pLog != NULL)
    {
        delete m_pLog;
        m_pLog = NULL;
    }
    if (m_pThread != NULL)
    {
        delete m_pThread;
        m_pThread = NULL;
    }

    if (m_pStatOrder != NULL)
    {
        delete m_pStatOrder;
        m_pStatOrder = NULL;
    }

    std::map<oss::UINT, loss::CDbi*>::iterator iter;
    for (iter = m_mapDbi.begin(); iter != m_mapDbi.end(); iter++)
    {
        if (iter->second != NULL)
        {
            delete iter->second;
        }
    }
    m_mapDbi.clear();
}

const std::string& oss::COssObjectManager::GetStatTargetDate() const
{
    return m_pPara->GetParaHandler()->GetStatTargetDate();
}

const std::string& oss::COssObjectManager::GetStatTargetTime() const
{
    return m_pPara->GetParaHandler()->GetStatTargetTime();
}

const std::string oss::COssObjectManager::GetOutputFileName() const
{
    std::string strOutputFileName;
    std::string strName = m_pPara->GetParaHandler()->GetOutputFileName();
    if (strName.empty())
    {
        strOutputFileName = std::string("../output/")
            + std::string(m_pStatOrder->m_szProcessName) + std::string(".txt");
    }
    else
    {
        strOutputFileName = std::string("../output/")
            + strName;
    }
    return strOutputFileName;
}

oss::UINT oss::COssObjectManager::RepetGetWorldId()
{
    oss::UINT uiWorldId = 0;
    while ((uiWorldId = GetWorldId()) != 0)
    {
        m_vecConfWorldId.push_back(uiWorldId);
        m_iterWorldId = m_vecConfWorldId.begin();
    }

    if (m_iterWorldId == m_vecConfWorldId.end())
    {
        uiWorldId = 0;
        m_iterWorldId = m_vecConfWorldId.begin();
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tget world id %u", __FILE__,
                __LINE__, __FUNCTION__, uiWorldId);
        return uiWorldId;
    }
    else
    {
        uiWorldId = (*m_iterWorldId);
        m_iterWorldId++;
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tget world id %u", __FILE__,
                __LINE__, __FUNCTION__, uiWorldId);
        return uiWorldId;
    }
}

void oss::COssObjectManager::AddTask(oss::COss* pTask, oss::UINT uiWorldId)
{
    std::vector<oss::UINT>::iterator iter
        = find(m_vecOptWorldId.begin(), m_vecOptWorldId.end(), uiWorldId);
    if (m_uiOptWorldNum == 0)
    {
        m_listTask.push_front(pTask);
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tadd world %u task to task list.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
    else if (iter != m_vecOptWorldId.end())
    {
        m_listTask.push_front(pTask);
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tadd world %u task to task list.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
    else
    {
        delete pTask;
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tignore world %u task.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
}

void oss::COssObjectManager::AddAreaTask(oss::COss* pTask, oss::UINT uiWorldId)
{
    std::vector<unsigned int>::iterator iter
        = find(m_vecOptWorldId.begin(), m_vecOptWorldId.end(), uiWorldId);

    std::string strHostArea = loss::Cfg::Instance()->GetStatHostConfHandler()->GetStatHostConf(
            loss::Cfg::Instance()->GetHostSequence()).m_szStatArea;
    std::string strWorldArea = loss::Cfg::Instance()->GetBusinessWorldHandler()
            ->GetWorldConf(m_pStatOrder->m_szBusinessFlag)->GetWorldDetail(uiWorldId).m_szStatArea;

    if (strHostArea == strWorldArea && m_uiOptWorldNum == 0)
    {
        m_listTask.push_front(pTask);
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tadd world %u task to task list.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
    else if (strHostArea == strWorldArea && iter != m_vecOptWorldId.end())
    {
        m_listTask.push_front(pTask);
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tadd world %u task to task list.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
    else
    {
        delete pTask;
        m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\tignore world %u task.",
                __FILE__, __LINE__, __FUNCTION__, uiWorldId);
    }
}

void oss::COssObjectManager::ClearTask()
{
    std::list<oss::COss*>::iterator iter;
    for (iter = m_listTask.begin(); iter != m_listTask.end(); iter++)
    {
        if (*iter != NULL)
        {
            delete (*iter);
        }
    }
    m_listTask.clear();
}

void oss::COssObjectManager::SingleThreadRun()
{
    std::list<oss::COss*>::iterator iter;

    m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\trun stat in single thread.",
            __FILE__, __LINE__, __FUNCTION__);
    for (iter = m_listTask.begin(); iter != m_listTask.end(); )
    {
        (*iter)->Run();
        delete (*iter);             //执行完统计即删除统计任务，回收系统资源（为节省系统资源考虑）
        m_listTask.erase(iter);     //这个任务已不存在，从任务列表中去除
        //从任务列表中清除后迭代器失效，需要从头赋值，iter == m_listTask.begin() == m_listTask.end() 时结束循环
        iter = m_listTask.begin();
    }
}

void oss::COssObjectManager::MultiThreadRun(oss::UINT uiMaxThreadNum)
{
    oss::UINT uiCurrentThreadNum = 0;
    std::list<oss::COss*>::iterator iter;

    m_pLog->WriteLog(loss::DEBUG_MSG, "%s:%u  %s\trun stat in multi thread.",
            __FILE__, __LINE__, __FUNCTION__);
    if (uiMaxThreadNum == 0)
    {
        for (iter = m_listTask.begin(); iter != m_listTask.end(); iter++)
        {
            m_pThread->NewThread(*iter);
        }
    }
    else
    {
        for (iter = m_listTask.begin(); iter != m_listTask.end(); iter++)
        {
            m_pThread->NewThread(*iter);
            uiCurrentThreadNum++;
            if (uiCurrentThreadNum >= uiMaxThreadNum)
            {
                m_pThread->JoinThread();
                uiCurrentThreadNum = 0;
            }
        }
    }
    m_pThread->JoinThread();
    ClearTask();
}

loss::CDbi* oss::COssObjectManager::GetDbi(
        oss::UINT uiWorldId,
        const std::string strDbPurpose)
{
    loss::CDbi* pDbi = NULL;
    std::map<oss::UINT, loss::CDbi*>::iterator iter;
    iter = m_mapDbi.find(uiWorldId);
    if (iter == m_mapDbi.end())
    {
        const loss::tagDbConfDetail* pDbConfDetail =
                &(loss::Cfg::Instance()->GetBusinessDbConfHandler()
                ->GetDbConf(m_strBusinessFlag)->GetDbConfDetail(uiWorldId, strDbPurpose));
        if (pDbConfDetail->m_ucAccess == loss::ACCESS_DIRECT)
        {
			switch (pDbConfDetail->m_ucDbType)
			{
				case loss::MYSQL_DB:
					pDbi = new loss::CMysqlDbiWithErrLog(m_pLog);
					pDbi->InitDbConn(pDbConfDetail->m_szDbHost,
							pDbConfDetail->m_szDbUser,
							pDbConfDetail->m_szDbPwd,
							pDbConfDetail->m_szDbName,
							pDbConfDetail->m_szDbCharSet,
							pDbConfDetail->m_uiDbPort);
					break;
				case loss::ORACLE_DB:
					break;
				case loss::SQL_SERVER_DB:
					break;
				case loss::DATA_AGENT:
					break;
				default:
					;
			}
        }
        else
        {
        	loss::tagAgentNode stAgentNode;
        	strncpy(stAgentNode.m_szBusinessFlag, m_strBusinessFlag.c_str(), 16);
        	strncpy(stAgentNode.m_szDbPurpose, strDbPurpose.c_str(), 16);
        	stAgentNode.m_uiWorldId = uiWorldId;
        	m_pLog->WriteLog(loss::DEBUG_MSG,"COssObjectManager::GetDbi,iWorldId:%d,strDbPurpose:%s"
        			,uiWorldId,strDbPurpose.c_str());
        	pDbi = new loss::CDataAgentClient(m_pLog,
        			loss::Cfg::Instance()->GetDataAgentAccessConfHandler()->GetDataAgentAccessConf(1));
        	pDbi->InitDbConn(stAgentNode);
        	m_pLog->WriteLog(loss::DEBUG_MSG,"After COssObjectManager::GetDbi:%d"
        	        			,pDbi);
        }
        m_mapDbi.insert(pair<oss::UINT, loss::CDbi*>(uiWorldId, pDbi));
    }
    else
    {
        pDbi = iter->second;
    }

    return pDbi;
}

int oss::COssObjectManager::LoadDataLocal()
{
    /*
    if (oss::COssObject::GetLoadFileNum() == 0)     //没有文件需要load
    {
        return 0;
    }

    int iQueryResult = 0;
    char szSql[1024] = {0};
    loss::CMysqlDbiWithErrLog oDbOper(GetLogPtr());
    std::string strIp;
    std::string strUsr;
    std::string strPass;
    std::string strDbName;
    std::string strCharacterSet;
    std::string strTableName;
    std::string strFileName;
    std::string strInsertType;
    switch (m_uiLoadDataConnType)
    {
        case PROCESS_DB:
            strIp = m_oUnixConf["process_db"]("host");
            strUsr = m_oUnixConf["process_db"]("db_usr");
            strPass = m_oUnixConf["process_db"]("db_pass");
            strDbName = m_oUnixConf["process_db"]("db_name");
            strCharacterSet = m_oUnixConf["process_db"]("character_set");
            break;
        case RESULT_DB:
            strIp = m_oUnixConf["result_db"]("host");
            strUsr = m_oUnixConf["result_db"]("db_usr");
            strPass = m_oUnixConf["result_db"]("db_pass");
            strDbName = m_oUnixConf["result_db"]("db_name");
            strCharacterSet = m_oUnixConf["result_db"]("character_set");
            break;
        default:
            m_oLog.WriteLog(ERROR, "Invalid db connect type in oss::COssObjectManager::LoadDataLocal()");
            return m_uiLoadDataConnType;
    }
    oDbOper.InitDbConn(strIp.c_str(), strUsr.c_str(),
            strPass.c_str(), strDbName.c_str(), strCharacterSet.c_str());

    m_oLog.WriteLog(INFO, "Start loading data db");
    while (COssObject::GetLoadFile(strTableName, strFileName))
    {
        sprintf(szSql, "LOAD DATA LOCAL INFILE '%s' %s INTO TABLE %s "
                "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'",
                strFileName.c_str(), m_strInsertMode.c_str(), strTableName.c_str());
        iQueryResult = oDbOper.ExecInsert(szSql);

        if (iQueryResult == 0)  // 写入成功才删除文件
        {
            if (remove(strFileName.c_str()) != 0 )  // 删除已入库的数据文件
            {
                m_oLog.WriteLog(ERROR, "delete file %s error %d.", strFileName.c_str(), errno);
            }
        }
    }
    m_oLog.WriteLog(INFO, "Complete loading data to db");

    return iQueryResult;
    */
}

