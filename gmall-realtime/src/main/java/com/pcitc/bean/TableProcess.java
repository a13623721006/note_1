package com.pcitc.bean;

import lombok.Data;

import java.io.Serializable;
@Data
public class TableProcess implements Serializable {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;


}
