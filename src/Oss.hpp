/*******************************************************************************
* Project:  DataAnalysis
* File:     Oss.hpp
* Description: OSS����
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/

#ifndef OSS_HPP_
#define OSS_HPP_

namespace oss
{

// ����UIN����Ϊ�޷��ų�����
typedef unsigned long UIN;

typedef unsigned int UINT;

class COss
{
public:
    COss(){};
    virtual ~COss(){};

    virtual int Run() = 0;
};

}

#endif /* OSS_HPP_ */
