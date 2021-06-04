package oracle.logminer.model;


import org.apache.kafka.connect.data.Struct;

/**
 * @program:$(PROJECT_NAME)
 * @description 从日志中分析出来的除了本身数据以前的所有信息
 * @author:miaoneng
 * @create:2021-04-30 11:23
 **/
public class DataContent {
    Integer scn;
    Integer commit_scn;
    String rowid;
    String xid;
    String SEG_OWNER;
    String tableName;
    String timeStamp;
    String operation;
    Struct before;
    Struct after;
    String redoSQL;

    public DataContent(Integer scn, Integer commit_scn, String rowid, String xid,String SEG_OWNER, String tableName, String timeStamp, String operation,String redoSQL, Struct before, Struct after) {
        this.scn = scn;
        this.commit_scn = commit_scn;
        this.rowid = rowid;
        this.xid = xid;
        this.SEG_OWNER = SEG_OWNER;
        this.tableName = tableName;
        this.timeStamp = timeStamp;
        this.operation = operation;
        this.redoSQL = redoSQL;
        this.before = before;
        this.after = after;
    }
    public Integer getScn() {
        return scn;
    }

    public void setScn(Integer scn) {
        this.scn = scn;
    }

    public Integer getCommit_scn() {
        return commit_scn;
    }

    public void setCommit_scn(Integer commit_scn) {
        this.commit_scn = commit_scn;
    }

    public String getRowid() {
        return rowid;
    }

    public void setRowid(String rowid) {
        this.rowid = rowid;
    }

    public String getSEG_OWNER() {
        return SEG_OWNER;
    }

    public void setSEG_OWNER(String SEG_OWNER) {
        this.SEG_OWNER = SEG_OWNER;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Struct getBefore() {
        return before;
    }

    public void setBefore(Struct before) {
        this.before = before;
    }

    public Struct getAfter() {
        return after;
    }

    public void setAfter(Struct after) {
        this.after = after;
    }
    public String getRedoSQL() {
        return redoSQL;
    }

    public void setRedoSQL(String redoSQL) {
        this.redoSQL = redoSQL;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }
}
