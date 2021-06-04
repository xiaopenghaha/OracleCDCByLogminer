# 场景：现场库到前置库。

## 思考：使用触发器？

1、侵入性解决方案

2、需要时各种配置，不需要时又是各种配置

**Change Data Capture：**捕捉变化的数据，通过日志监测并捕获数据库的变动（包括数据或数据表的插入，更新，删除等），将这些变更按发生的顺序完整记录下来，写入到消息中间件中或者通过其他途径分发出去。

与触发器相比，通过日志监控的好处：

- 1、对原库基本无侵入性，不需要像触发器一样，对原库进行操作。
- 2、可以针对整库或者库中的表进行监控，比触发器更加灵活高效，避免监控表比较多的情况的频繁的建触发器

# 前置步骤

Oracle：通过开源logminer进行日志分析，支持大字符串，二进制。

目前只针对在线日志分析，离线的由于需要考虑log大小等，留给以后处理。

开启归档日志，这边步骤主要是为了能够使用logminer

```
以数据库系统管理员sys as sysdba登录
SQL> shutdown immediate; 关闭数据库
SQL> startup mount; 启动数据库到mount状态
SQL> alter database archivelog; 启动归档模式
SQL> alter database open;启动数据库
SQL> alter system switch logfile;切换日志文件
```

```
查看数据字典表或视图权限
  GRANT SELECT_CATALOG_ROLE TO [用户名];
执行系统所有包权限
  GRANT EXECUTE_CATALOG_ROLE TO [用户名];
创建会话权限
  GRANT CREATE SESSION TO [用户名];
选择任何事务的权限
  GRANT SELECT ANY TRANSACTION TO [用户名];
```

对于12c及以上，还需要对pdb用户进行一些设置，具体遇到再百度下。

# 说明

**关于logminer：**

所有对用户数据和数据字典的改变都记录在Oracle的Redo Log中，因此，Redo Log包含了所有进行恢复操作所需要的信息。但是，原始的Redo Log文件无法看懂，所以，Oracle从8i以后提供了一个非常有用的分析工具，称为LogMiner。使用该工具可以轻松获得Redo Log文件（包含归档日志文件）中的具体内容。

关于参考：主要参考了debezium和一些国外开源的实现，但是这些都没有支持clob大字段和blob二进制字段，同时也没考虑一些特殊情况，我在平时的测试积累中解决了此块的内容，故一起开源分享出来。

# 分析过程

## 1、拿到当前最大位点

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/17AF1223AED34FD782FD4C5EC3B05479/18206)

## 2、开启logminer分析

```
begin
	DBMS_LOGMNR.START_LOGMNR(
		STARTSCN => 586613478,
		ENDSCN => 586613490,
		OPTIONS => 
			DBMS_LOGMNR.SKIP_CORRUPTION
			+DBMS_LOGMNR.NO_SQL_DELIMITER
			+DBMS_LOGMNR.NO_ROWID_IN_STMT
			+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
			+DBMS_LOGMNR.CONTINUOUS_MINE
			+DBMS_LOGMNR.COMMITTED_DATA_ONLY
			+DBMS_LOGMNR.STRING_LITERALS_IN_STMT
	);
end;
```

## 3、通过数据字典得到表的数据类型

代码中的dictionary.sql

## 4、根据配置的白名单进行信息删选

```
SELECT
*
FROM
    V$LOGMNR_CONTENTS
WHERE(SEG_OWNER = 'EPOINT' AND TABLE_NAME = 'BASEINFO' AND COMMIT_SCN >=0)
```

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/1D3B56A7598E4979801FE21DA6AA503C/19778)

## 5、分析

### 1、insert语句（带大字段和图片）

大字段和图片都通过存储过程得到。

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/36DC1A7E70A34616909AA3692BA75E35/19869)

我们根据csf看是否超过4000个字节，如果超过csf为0，我们把所有的sql先组装到一起，然后通过信息组装出一个guid，存入map，这样后面就能找到对应的EMPTY_CLOB()对应的值了。

```
//用xid、ownerName、tableName、columnString唯一标识当前二进制字段的guid
//#!>-<!#为分隔符
//例如：99001800E16A0000#!>-<!#EPOINT#!>-<!#BASEINFO1#!>-<!#IMAGE
```

### 2、update语句（带大字段和图片）

```
update "EPOINT"."BASEINFO" set "NAME" = '苏爱毓', "BIRTHDAY" = TIMESTAMP ' 1977-03-17 17:22:59', "AGE" = 3, "ADDRESS" = UNISTR('\6FB3\95E8\516B\885718\53F7-8-6') where "ROWGUID" = 'b73af60a-cdda-4702-8f28-0d707c0245a1' and "NAME" = '法贞凤' and "BIRTHDAY" = TIMESTAMP ' 1978-04-16 09:21:46' and "AGE" = 61 and "ADDRESS" = UNISTR('\8BF8\57CE\5927\53A674\53F7-6-8')
```

下面紧接着是他要处理的大字段和二进制。

程序中通过下面的guid判断是否是一批数据，然后把检测到的大字段或者二进制与之关联。

```
//用xid、ownerName、tableName、columnString唯一标识当前二进制字段的guid
//#!>-<!#为分隔符
//例如：99001800E16A0000#!>-<!#EPOINT#!>-<!#BASEINFO1#!>-<!#IMAGE
```

## 注意：

我们再测试下，让startscn加1

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/945FA3B6425C425081360F198B02D5DF/19788)

可以看到无法分析出数据了，所以我们程序需要处理掉这种情况，遇到这种情况最简单的办法就是startscn要往回退一点，也注意不能形成死循环

其实主要是通过这些进行流程的分析处理，里面会遇到很多的坑。

# 测试实现

配置：由于测试整个过程，暂时单表，多表只要改造下。

## 1、配置

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/2E238B2317BD4FCA83F958719599CB1A/19900)

## 2、启动

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/EEEFAC9DDEC44A9C92ED5CFB38B8FAAA/19912)



## 3、插入一条数据

主要在11g在测试，12c做了兼容，而且要使用cdb账户

### 11g

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/E3F9463AA6D444BAAF9A80833B4DB299/19753)

### 插入mysql中

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/2ED54D7EBB5A4C2DB3F159DB5D851E47/19755)

## 5、更新

### oracle

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/A1A0FB85609E4C9AA775BF5235003763/19770)

### 更新完mysql

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/18E7957574DA4EE9A235194D75948DD8/19773)

## 6、插入2000条测试

### oracle

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/00539F69909346ACBDF6B6019EF48D78/19904)

### mysql目标数据库

![img](https://note.youdao.com/yws/public/resource/20f3a8c448f6bc55a6cf5818ffece8f9/xmlnote/D89BACB0DED24C79BBF6CBB61E4E5C85/19906)
