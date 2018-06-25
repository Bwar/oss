/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObjectManager.hpp
* Description: OSS对象管理类
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

enum E_LOAD_DB_CONN                 // 需要LOAD数据的目标DB
{
    PROCESS_DB = 0,                 // 报表服务器数据库
    RESULT_DB = 1,                  // 数据服务器数据库
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

    //增加统计任务，不区分统计区域
    void AddTask(oss::COss* pTask, oss::UINT uiWorldId);

    //增加统计任务，区分统计区域
    void AddAreaTask(oss::COss* pTask, oss::UINT uiWorldId);

    void SingleThreadRun();                                 // 单线程执行统计
    void MultiThreadRun(oss::UINT uiMaxThreadNum = 0);      // 多线程执行统计

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

    //清除统计任务
    void ClearTask();

private:
    int LoadDataLocal();                            // 将已写入文件的统计数据load到数据库，并删除文件

    loss::CStatPara* m_pPara;                       // 参数类
    loss::CLogBase* m_pLog;                         // 日志
    loss::CThread<oss::COss>* m_pThread;            // 线程
    std::map<oss::UINT, loss::CDbi*> m_mapDbi;      // 数据库操作指针

private:
    oss::UINT m_uiOptWorldNum;                      // 命令行参数world id数量
    oss::UINT m_uiLoadDataConnType;                 // 执行load data local的数据库连接
    std::vector<oss::UINT> m_vecOptWorldId;         // 命令行参数world id
    std::vector<oss::UINT> m_vecConfWorldId;        // 配置中的world id
    std::vector<oss::UINT>::iterator m_iterWorldId; // 配置中的world id当前位置
    std::list<oss::COss*> m_listTask;               // 任务列表
    std::string m_strBusinessFlag;                  // 业务标识
    const std::string m_strInsertMode;              // 执行load操作的方式 (ignore 或 replace)
    loss::tagStatOrder* m_pStatOrder;               // 统计指令
};

}

#endif /* OSSOBJECTMANAGER_HPP_ */
