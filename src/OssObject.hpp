/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObject.hpp
* Description: OSS对象基类
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/

#ifndef OSSOBJECT_HPP_
#define OSSOBJECT_HPP_

#include <map>
#include "UnixTime.hpp"
#include "TableConf.hpp"
#include "thread/ThreadMutex.hpp"
#include "dbi/Dbi.hpp"
#include "log/LogBase.hpp"

#include "Oss.hpp"
#include "OssObjectManager.hpp"

namespace oss
{

class COssObject : public oss::COss
{
public:
	COssObject();
    COssObject(oss::UINT uiWorldId, oss::COssObjectManager* pManager);
    virtual ~COssObject();

    int Run();

    //初始化OssObject，在使用无参构造函数COssObject()构造OssObject对象时需要在使用OssObject对象前显式调用此函数
    int InitOssObject(oss::UINT uiWorldId, oss::COssObjectManager* pManager);

    //获取需要执行的load data local的文件数量
    static int GetLoadFileNum()
    {
        return m_mapTableFileName.size();
    }

    //获取需执行load data local的文件名及对应表名，每次获取一个，返回1表示已
    //获取到一个文件名及其对应表名，返回0表示文件列表为空或已到达列表末尾
    static int GetLoadFile(std::string& strTableName, std::string& strFileName);

protected:
    //由派生类实现的统计函数
    virtual int Stat() = 0;

    //全局初始化，从OSS对象基类派生的类仅会在第一个对象构造完成之前
    //执行一次此函数，主要用于初始化类的静态成员，若无静态成员需要初
    //始化，则无需实现此函数。
    virtual int ClusterInit()
    {
        return 0;
    }

    //全服去重汇总统计
    virtual int ClusterStat()
    {
        return 0;
    }

protected:
    oss::UINT GetWorldId() const             //获取WorldId
    {
        return m_uiWorldId;
    }

    oss::UINT GetClusterId() const                   //获取虚拟的world id，表示所有world
    {
        return m_uiClusterId;
    }

    const std::string& GetStatDay() const             //获取统计目标日期
    {
        return m_strStatDay;
    }

    const std::string& GetStatDayBegin() const        //获取统计目标日开始时刻
    {
        return m_strStatDayBegin;
    }

    const std::string& GetStatDayEnd() const          //获取统计目标日结束时刻
    {
        return m_strStatDayEnd;
    }

    //获取统计时间，一般用于实时在线或实时收入统计
    const std::string GetStatTime() const
    {
        return m_strStatTime;
    }

    loss::CLogBase* GetLogPtr()
    {
        return m_pLog;
    }

    //获取结果文件名
    const std::string& GetResultFileName() const
    {
        //return m_strResultFile;
    }

    int QueryData(
            const char* szSql,
            const std::string strDbPurpose = "Log1");

    int WriteData(
            const char* szSql,
            const std::string strDbPurpose = "ReportDb");

    loss::T_vecResultSet& GetResultSet();

    //将需要执行load data local操作的文件名，表名增加到列表中
    int AddLoadFile(const std::string& strTableName, const std::string& strFileName);

private:
    /* 工具类指针（由CObjectManager负责分配和回收） */
    oss::COssObjectManager* m_pManager;             // OSS对象管理指针
    loss::CLogBase* m_pLog;                         // 写日志对象指针
    loss::CDbi* m_pDbi;                             // 数据库接口操作

    /* 统计数据 成员*/
    oss::UINT m_uiWorldId;                          //  游戏大区编号
    static oss::UINT m_uiClusterId;                 //  虚拟的world id，表示所有world

    loss::T_vecResultSet m_vecResultSet;            //  查询结果集

    static std::string m_strStatDay;                //  统计日期， YYYY-MM-DD
    static std::string m_strStatDayBegin;           //  统计目标日开始时刻 YYYY-MM-DD 00:00:00
    static std::string m_strStatDayEnd;             //  统计目标日结束时刻 YYYY-MM-DD 23:59:59
    static std::string m_strStatTime;               //  统计时间， YYYY-MM-DD HH:MI:SS

    /* RUN控制数据成员 */
    oss::UINT m_uiIsRun;                            //  当前实例是否已运行，0 未运行，1 已运行
    std::string m_strCurrentConn;                   //  当前DBI指向的数据库
    static oss::UINT m_uiExistInstanceNum;          //  存在的实例数量
    static oss::UINT m_uiActiveInstanceNum;         //  当前处于激活状态的统计实例数量
    static oss::UINT m_uiRunningInstanceNum;        //  正在运行中的统计实例数量
    static oss::UINT m_uiIsClusterInitAffect;       //  ClusterInit()函数是否已发生作用
    static loss::CThreadMutex m_mutex;              //  实例控制锁

    //需load到数据库的文件映射，键为表名，值为带路径的文件名
    static std::map <std::string, std::string> m_mapTableFileName;
};

}

#endif /* OSSOBJECT_HPP_ */
