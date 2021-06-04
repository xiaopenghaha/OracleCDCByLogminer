package oracle.logminer.sqldialect;

import oracle.logminer.model.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @program:$(PROJECT_NAME)
 * @description sql基类
 * @author:miaoneng
 * @create:2021-04-28 11:06
 **/
public interface LogMinerDialect {
    public List<Table> getTables(Connection connection) throws SQLException;
    //申明可以用哪些sql进行操作
    public enum Statement {

        //START_LOGMINER:开启logminer
        //STOP_LOGMINER：结束logminer
        //CONTENTS：根据拿到V$LOGMNR_CONTENTS中的增、删、改内容

        START_LOGMINER("start_logminer"), STOP_LOGMINER("stop_logminer"), CONTENTS("contents"), DICTIONARY("dictionary"), CURRENT_SCN(
                "current.scn"), LATEST_SCN("latest.scn"), EARLIEST_SCN("earliest.scn"), TABLES("tables");

        private final String property;

        private Statement(String property) {
            this.property = property;
        }

        public String getProperty() {
            return this.property;
        }

        public static Statement get(String property) {
            switch (property) {
                case "start":
                    return START_LOGMINER;
                case "stop":
                    return STOP_LOGMINER;
                case "contents":
                    return CONTENTS;
                case "dictionary":
                    return DICTIONARY;
                case "current.scn":
                    return CURRENT_SCN;
                case "latest.scn":
                    return LATEST_SCN;
                case "earliest.scn":
                    return EARLIEST_SCN;
                case "tables":
                    return TABLES;
                default:
                    throw new IllegalArgumentException("Invalid SQL statement property name \"" + property + "\"");
            }
        }
    }
}
