package oracle.logminer;

/**
 * @program:$(PROJECT_NAME)
 * @description
 * @author:miaoneng
 * @create:2021-04-07 14:33
 **/
public class OracleSql {
    //获得当前SCN
    static String CURRENT_SCN="SELECT MAX(CURRENT_SCN) CURRENT_SCN FROM GV$DATABASE";

    //用于启动logmnr会话
    static String Strat_MIN="begin\n" +
            "\tDBMS_LOGMNR.START_LOGMNR(\n" +
            "\t\tSTARTSCN => ?,\n" +
            "\t\tENDSCN => ?,\n" +
            "\t\tOPTIONS => \n" +
            "\t\t\tDBMS_LOGMNR.SKIP_CORRUPTION\n" +
            "\t\t\t+DBMS_LOGMNR.NO_SQL_DELIMITER\n" +
            "\t\t\t+DBMS_LOGMNR.NO_ROWID_IN_STMT\n" +
            "\t\t\t+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG\n" +
            "\t\t\t+DBMS_LOGMNR.CONTINUOUS_MINE\n" +
            "\t\t\t+DBMS_LOGMNR.COMMITTED_DATA_ONLY\n" +
            "\t\t\t+DBMS_LOGMNR.STRING_LITERALS_IN_STMT\n" +
            "\t);\n" +
            "end;";
    static String logmnr_contents = "SELECT * FROM V$LOGMNR_CONTENTS WHERE";

    static String connects = "" +
            "SELECT\n" +
            "*\n" +
            "FROM\n" +
            "    V$LOGMNR_CONTENTS\n" +
            "WHERE\n" +
            "    OPERATION_CODE IN (\n" +
            "        1,\n" +
            "        2,\n" +
            "        3\n" +
            "    )\n" +
            "    AND ";
}
