/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObjectManager.hpp
* Description: OSS���������
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/

#ifndef OSSOBJECTMANAGER_HPP_
#define OSSOBJECTMANAGER_HPP_

#include <string>
#include <list>
#include <vector>
#include "log/LogBase.hpp"
#include "log/OssLog.hpp"
#include "dbi/Dbi.hpp"
#include "dbi/MysqlDbiWithErrLog.hpp"
#include "dbi/DataAgentClient.hpp"
#include "Argv.hpp"
#include "thread/Thread.hpp"
#include "config/ConfigurationHandler.hpp"
#include "config/OssComand.hpp"
#include "config/StatPara.hpp"

#include "Oss.hpp"


namespace oss
{

enum E_LOAD_DB_CONN                 // ��ҪLOAD���ݵ�Ŀ��DB
{
    PROCESS_DB = 0,                 // ������������ݿ�
    RESULT_DB = 1,                  // ���ݷ��������ݿ�
};

const std::string g_strVersionInfo = "OSS V4.0 \n\n";

class COssObjectManager
{
public:
    COssObjectManager(
            const loss::tagStatOrder& stStatOrder,
            oss::UINT uiLoadDataConnType = PROCESS_DB,
            const std::string& strInsertMode = "IGNORE");
    virtual ~COssObjectManager();

    //����ͳ�����񣬲�����ͳ������
    void AddTask(oss::COss* pTask, oss::UINT uiWorldId);

    //����ͳ����������ͳ������
    void AddAreaTask(oss::COss* pTask, oss::UINT uiWorldId);

    void SingleThreadRun();                                 // ���߳�ִ��ͳ��
    void MultiThreadRun(oss::UINT uiMaxThreadNum = 0);      // ���߳�ִ��ͳ��

public:
    const std::string& GetStatTargetDate() const;

    const std::string& GetStatTargetTime() const;

    const std::string GetOutputFileName() const;

    oss::UINT GetWorldNum() const
    {
        return loss::Cfg::Instance()->GetBusinessWorldHandler()
                ->GetWorldConf(m_pStatOrder->m_szBusinessFlag)->GetWorldNum();
    }

    oss::UINT RepetGetWorldId();

    oss::UINT GetWorldId()
    {
        return loss::Cfg::Instance()->GetBusinessWorldHandler()
                ->GetWorldConf(m_pStatOrder->m_szBusinessFlag)->GetNextWorldId();
    }

    oss::UINT GetClusterId()
    {
        return loss::Cfg::Instance()->GetBusinessBasicConfigHandler()
                ->GetBusinessBasicConfig(m_pStatOrder->m_szBusinessFlag).m_uiClusterId;
    }

    loss::CLogBase* GetLogPtr()
    {
        return m_pLog;
    }

    loss::CDbi* GetDbi(oss::UINT uiWorldId, const std::string strDbPurpose);

    //���ͳ������
    void ClearTask();

private:
    int LoadDataLocal();                            // ����д���ļ���ͳ������load�����ݿ⣬��ɾ���ļ�

    loss::CStatPara* m_pPara;                       // ������
    loss::CLogBase* m_pLog;                         // ��־
    loss::CThread<oss::COss>* m_pThread;            // �߳�
    std::map<oss::UINT, loss::CDbi*> m_mapDbi;      // ���ݿ����ָ��

private:
    oss::UINT m_uiOptWorldNum;                      // �����в���world id����
    oss::UINT m_uiLoadDataConnType;                 // ִ��load data local�����ݿ�����
    std::vector<oss::UINT> m_vecOptWorldId;         // �����в���world id
    std::vector<oss::UINT> m_vecConfWorldId;        // �����е�world id
    std::vector<oss::UINT>::iterator m_iterWorldId; // �����е�world id��ǰλ��
    std::list<oss::COss*> m_listTask;               // �����б�
    std::string m_strBusinessFlag;                  // ҵ���ʶ
    const std::string m_strInsertMode;              // ִ��load�����ķ�ʽ (ignore �� replace)
    loss::tagStatOrder* m_pStatOrder;               // ͳ��ָ��
};

}

#endif /* OSSOBJECTMANAGER_HPP_ */
