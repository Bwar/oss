/*******************************************************************************
* Project:  DataAnalysis
* File:     OssObject.hpp
* Description: OSS�������
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

    //��ʼ��OssObject����ʹ���޲ι��캯��COssObject()����OssObject����ʱ��Ҫ��ʹ��OssObject����ǰ��ʽ���ô˺���
    int InitOssObject(oss::UINT uiWorldId, oss::COssObjectManager* pManager);

    //��ȡ��Ҫִ�е�load data local���ļ�����
    static int GetLoadFileNum()
    {
        return m_mapTableFileName.size();
    }

    //��ȡ��ִ��load data local���ļ�������Ӧ������ÿ�λ�ȡһ��������1��ʾ��
    //��ȡ��һ���ļ��������Ӧ����������0��ʾ�ļ��б�Ϊ�ջ��ѵ����б�ĩβ
    static int GetLoadFile(std::string& strTableName, std::string& strFileName);

protected:
    //��������ʵ�ֵ�ͳ�ƺ���
    virtual int Stat() = 0;

    //ȫ�ֳ�ʼ������OSS�������������������ڵ�һ�����������֮ǰ
    //ִ��һ�δ˺�������Ҫ���ڳ�ʼ����ľ�̬��Ա�����޾�̬��Ա��Ҫ��
    //ʼ����������ʵ�ִ˺�����
    virtual int ClusterInit()
    {
        return 0;
    }

    //ȫ��ȥ�ػ���ͳ��
    virtual int ClusterStat()
    {
        return 0;
    }

protected:
    oss::UINT GetWorldId() const             //��ȡWorldId
    {
        return m_uiWorldId;
    }

    oss::UINT GetClusterId() const                   //��ȡ�����world id����ʾ����world
    {
        return m_uiClusterId;
    }

    const std::string& GetStatDay() const             //��ȡͳ��Ŀ������
    {
        return m_strStatDay;
    }

    const std::string& GetStatDayBegin() const        //��ȡͳ��Ŀ���տ�ʼʱ��
    {
        return m_strStatDayBegin;
    }

    const std::string& GetStatDayEnd() const          //��ȡͳ��Ŀ���ս���ʱ��
    {
        return m_strStatDayEnd;
    }

    //��ȡͳ��ʱ�䣬һ������ʵʱ���߻�ʵʱ����ͳ��
    const std::string GetStatTime() const
    {
        return m_strStatTime;
    }

    loss::CLogBase* GetLogPtr()
    {
        return m_pLog;
    }

    //��ȡ����ļ���
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

    //����Ҫִ��load data local�������ļ������������ӵ��б���
    int AddLoadFile(const std::string& strTableName, const std::string& strFileName);

private:
    /* ������ָ�루��CObjectManager�������ͻ��գ� */
    oss::COssObjectManager* m_pManager;             // OSS�������ָ��
    loss::CLogBase* m_pLog;                         // д��־����ָ��
    loss::CDbi* m_pDbi;                             // ���ݿ�ӿڲ���

    /* ͳ������ ��Ա*/
    oss::UINT m_uiWorldId;                          //  ��Ϸ�������
    static oss::UINT m_uiClusterId;                 //  �����world id����ʾ����world

    loss::T_vecResultSet m_vecResultSet;            //  ��ѯ�����

    static std::string m_strStatDay;                //  ͳ�����ڣ� YYYY-MM-DD
    static std::string m_strStatDayBegin;           //  ͳ��Ŀ���տ�ʼʱ�� YYYY-MM-DD 00:00:00
    static std::string m_strStatDayEnd;             //  ͳ��Ŀ���ս���ʱ�� YYYY-MM-DD 23:59:59
    static std::string m_strStatTime;               //  ͳ��ʱ�䣬 YYYY-MM-DD HH:MI:SS

    /* RUN�������ݳ�Ա */
    oss::UINT m_uiIsRun;                            //  ��ǰʵ���Ƿ������У�0 δ���У�1 ������
    std::string m_strCurrentConn;                   //  ��ǰDBIָ������ݿ�
    static oss::UINT m_uiExistInstanceNum;          //  ���ڵ�ʵ������
    static oss::UINT m_uiActiveInstanceNum;         //  ��ǰ���ڼ���״̬��ͳ��ʵ������
    static oss::UINT m_uiRunningInstanceNum;        //  ���������е�ͳ��ʵ������
    static oss::UINT m_uiIsClusterInitAffect;       //  ClusterInit()�����Ƿ��ѷ�������
    static loss::CThreadMutex m_mutex;              //  ʵ��������

    //��load�����ݿ���ļ�ӳ�䣬��Ϊ������ֵΪ��·�����ļ���
    static std::map <std::string, std::string> m_mapTableFileName;
};

}

#endif /* OSSOBJECT_HPP_ */
