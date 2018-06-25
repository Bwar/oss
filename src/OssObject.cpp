/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObject.cpp
* Description: OSS�������
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/


#include "OssObject.hpp"

std::string oss::COssObject::m_strStatDay;
std::string oss::COssObject::m_strStatDayBegin;
std::string oss::COssObject::m_strStatDayEnd;
std::string oss::COssObject::m_strStatTime;

oss::UINT oss::COssObject::m_uiClusterId = 0;
oss::UINT oss::COssObject::m_uiExistInstanceNum = 0;
oss::UINT oss::COssObject::m_uiActiveInstanceNum = 0;
oss::UINT oss::COssObject::m_uiRunningInstanceNum = 0;
oss::UINT oss::COssObject::m_uiIsClusterInitAffect = 0;
loss::CThreadMutex oss::COssObject::m_mutex;
std::map <std::string, std::string> oss::COssObject::m_mapTableFileName;


oss::COssObject::COssObject()
{
    m_mutex.Lock();
    m_uiExistInstanceNum++;
    m_uiActiveInstanceNum++;
    m_mutex.Unlock();
    m_uiIsRun = 0;
}

oss::COssObject::COssObject(oss::UINT uiWorldId, oss::COssObjectManager* pManager)
    :   m_pManager(pManager), m_pLog(pManager->GetLogPtr())
{
    m_uiWorldId = uiWorldId;

    m_mutex.Lock();
    m_uiExistInstanceNum++;
    m_uiActiveInstanceNum++;
    m_mutex.Unlock();
    m_uiIsRun = 0;
}

oss::COssObject::~COssObject()
{
    if (m_uiIsRun == 0)     // �������û�б�ִ�о��˳�
    {
        m_mutex.Lock();
        m_uiActiveInstanceNum--;
        m_mutex.Unlock();
    }
    m_uiExistInstanceNum--;
}

int oss::COssObject::Run()
{
    m_uiIsRun = 1;

    m_mutex.Lock();
    m_uiRunningInstanceNum++;
    if (m_uiIsClusterInitAffect == 0 && m_uiRunningInstanceNum == 1
            && m_uiActiveInstanceNum == m_uiExistInstanceNum)    //����̬ʵ���������ڴ���ʵ��������������һ��ͳ���߳�����ǰ���ȫ�ֳ�ʼ��
    {
        //����ִ������˵�����ǵ�һ�����е�ͳ��ʵ��
        m_pLog->WriteLog(loss::DEBUG_MSG, "ClusterInit() start...");

        m_uiClusterId = m_pManager->GetClusterId();

        time_t lDayTime = loss::TimeStr2time_t(m_pManager->GetStatTargetDate());
        time_t lDayTimeBegin = loss::GetBeginTimeOfTheDay(lDayTime);
        time_t lDayTimeEnd = loss::GetEndTimeOfTheDay(lDayTime);
        m_strStatDayBegin = loss::time_t2TimeStr(lDayTimeBegin);
        m_strStatDayEnd = loss::time_t2TimeStr(lDayTimeEnd);
        m_strStatDay = loss::time_t2TimeStr(lDayTime, "YYYY-MM-DD");
        m_strStatTime = m_pManager->GetStatTargetTime();

        ClusterInit();
        m_uiIsClusterInitAffect = 1;            //ClusterInit()��ʼ������
        m_pLog->WriteLog(loss::DEBUG_MSG, "ClusterInit() completed...");
    }
    m_mutex.Unlock();

    Stat();

    m_mutex.Lock();
    if (1 == m_uiRunningInstanceNum && 1 == m_uiActiveInstanceNum)    //�����һ���������еļ���̬ʵ��ȥ��ɻ��ܹ���
    {
        //����ִ������˵��ֻʣ���һ��ͳ��ʵ��
        m_pLog->WriteLog(loss::DEBUG_MSG, "ClusterStat() start...");
        ClusterStat();
        m_uiIsClusterInitAffect = 0;            //ClusterStat()�������ClusterInit()���ý���
        m_pLog->WriteLog(loss::DEBUG_MSG, "ClusterStat() completed...");
    }
    m_uiRunningInstanceNum--;
    m_uiActiveInstanceNum--;    //�������ת��Ϊ�Ǽ���״̬ʵ��
    m_mutex.Unlock();

    return 0;
}

int oss::COssObject::InitOssObject(
		oss::UINT uiWorldId,
		oss::COssObjectManager* pManager)
{
    m_uiWorldId = uiWorldId;
    m_pManager = pManager;
    m_pLog = pManager->GetLogPtr();
}

int oss::COssObject::QueryData(
        const char* szSql,
        const std::string strDbPurpose)
{
    if (m_strCurrentConn != strDbPurpose)
    {
        m_pDbi = NULL;
        m_pDbi = m_pManager->GetDbi(m_uiWorldId, strDbPurpose);
        if (NULL == m_pDbi)
        {
            m_pDbi = m_pManager->GetDbi(m_uiClusterId, strDbPurpose);
        }
    }

    if (NULL != m_pDbi)
    {
        m_vecResultSet.clear();
        int iQueryResult = 0;
        iQueryResult = m_pDbi->ExecSql(szSql);
        if (0 == iQueryResult)
        {
            return m_pDbi->GetResultSet(m_vecResultSet);
        }
    }
    else
    {
       m_pLog->WriteLog(loss::CRITICAL, "Get Dbi error, can not query data "
               "from \"%s\" , sql: \"%s\"", strDbPurpose.c_str(), szSql);
       return -1;
    }
}

int oss::COssObject::WriteData(
        const char* szSql,
        const std::string strDbPurpose)
{
    if (m_strCurrentConn != strDbPurpose)
    {
        m_pDbi = NULL;
        m_pDbi = m_pManager->GetDbi(m_uiWorldId, strDbPurpose);
        if (NULL == m_pDbi)
        {
            m_pDbi = m_pManager->GetDbi(m_uiClusterId, strDbPurpose);
        }
    }

    if (NULL != m_pDbi)
    {
        return m_pDbi->ExecSql(szSql);
    }
    else
    {
       m_pLog->WriteLog(loss::CRITICAL, "Get Dbi error, can not write data to "
               "\"%s\", sql: \"%s\"", strDbPurpose.c_str(), szSql);
       return -1;
    }
}

loss::T_vecResultSet& oss::COssObject::GetResultSet()
{
    return m_vecResultSet;
}

//����Ҫִ��load data local�������ļ������������ӵ��б���
int oss::COssObject::AddLoadFile(
        const std::string& strTableName,
        const std::string& strFileName)
{
    std::map <std::string, std::string>::iterator iter;
    iter = m_mapTableFileName.find(strTableName);
    if (iter == m_mapTableFileName.end())
    {
        m_mutex.Lock();
        m_mapTableFileName.insert(std::pair<std::string, std::string>(strTableName, strFileName));
        m_mutex.Unlock();
    }
}

