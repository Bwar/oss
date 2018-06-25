/*******************************************************************************
* Project:  DataAnalysis
* File:     Oss.hpp
* Description: OSS基类
* Author:        bwarliao
* Created date:  2011-3-1
* Modify history:
*******************************************************************************/

#ifndef OSS_HPP_
#define OSS_HPP_

namespace oss
{

// 定义UIN类型为无符号长整型
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
