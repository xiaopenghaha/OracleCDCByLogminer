package oracle.logminer;

import net.sf.jsqlparser.JSQLParserException;
import oracle.logminer.model.DataContent;
import oracle.logminer.model.Offset;
import oracle.logminer.model.Table;
import oracle.logminer.sqldialect.BaseLogMinerDialect;
import oracle.logminer.sqldialect.LogMinerDialect;
import oracle.logminer.sqldialect.LogMinerSQLFactory;
import org.apache.kafka.connect.data.Field;
import org.hibernate.SessionFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;


/**
 * @program:$(PROJECT_NAME)
 * @description
 * @author:miaoneng
 * @create:2021-04-07 11:17
 **/
public class LogminerAnalysis {

    private static Logger logger = LoggerFactory.getLogger(LogminerAnalysis.class);

    //是否是oracle 12c及以上
    Boolean is12c = false;
    //url连接
    String jdbcURL = "";

    //用户名
    String user = "";

    //密码
    String password = "";

    //白名单
    String whiteTable = "";

    //区分11g还是12c以上，false-11g，true-12c及以上
    private Boolean multitenant;

    //oracle连接
    private Connection connection;

    //oracle pdb连接
    private Connection pdbConnection;

    //当前resource下面的config.properties
    private Properties properties;

    //需要同步库的连接
    private Connection synConnection;

    //实例名
    String databaseName = "";

    //连接超时时间
    private static final int VALIDITY_CHECK_TIMEOUT = 5;

    //表状态
    Map<Table, Offset> stateMap;

    //是否开始logminer分析
    private Boolean started;

    //分析起始SessionSCN
    private Long sessionStartSCN = -1L;

    //分析结束sessionSCN
    Long sessionAnalysisSCN = -1L;

    //上一次分析到的位点
    Long lastAnalysisSCN = Long.valueOf(0);

    //当前SessionSCN
    private Long currentSeesionSCN = -1L;

    //分析步长，有时候开始分析scn和结束scn差距太大，oracle会分析很久而且有可能报错，通过步长来控制
    public int stepCount = 300;

    //上一次的开始分析SCN
    private Long lastSessionStartSCN = -1L;

    //上一次的CommitSCN位点
    Long lastSessionCommitSCN = Long.valueOf(-1);

    //上一次拿到数据的row_id
    String lastRow_ID;

    //每个表对应的元数据
    private Map<Table, Schema> schemas = new HashMap<>();

    //自定义分隔符
    public String seperatorWord = "#!>-<!#";

    private static SessionFactory factory;
    //初始化
    public void init() throws IOException, SQLException {
        Properties props = new Properties();
        InputStream in = LogminerAnalysis.class.getClassLoader().getResourceAsStream("config.properties");
        props.load(in);
        properties = props;

        jdbcURL = props.getProperty("url");
        user = props.getProperty("user");
        password = props.getProperty("password");
        whiteTable = props.getProperty("whitetables");
        is12c = isMultitenant();
        List<Table> tableList = getVisibleTables();
        List<Table> filteredTables = filterTables(tableList, Arrays.asList(whiteTable.split(",")));

        //初始化白名单中的表以及位置信息放到map中
        stateMap = filteredTables.stream().collect(Collectors.toMap(t -> t, t -> Offset.DEFAULT_OFFSET));


        Properties properties = new Properties();
        properties.setProperty("user", props.getProperty("insert_username"));
        properties.setProperty("password", props.getProperty("insert_password"));
        synConnection = DriverManager.getConnection(props.getProperty("insert_url"), properties);


    }

    /*
     * @Description:
     * @Param: currentSeesionSCN
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/4/12 17:42
     */
    public void start(Long currentSeesionSCN) throws IOException, SQLException, ParseException, InterruptedException {
        this.currentSeesionSCN = currentSeesionSCN;
        connection = getConnection();

        //拿到当前的scn
        if (currentSeesionSCN.equals(-1L)) {
            currentSeesionSCN = getCurrentSCN();
        }
        while (true) {
            //进行分析
            List<DataContent> dataContentsList = poll();
            if (dataContentsList == null) {
                logger.info("没检测到数据");
            } else {
                logger.info("解析了{}条数据", dataContentsList.size());
                for (int i = 0; i < dataContentsList.size(); i++) {
                    // 预处理变化的数据
                    HashMap<String, Object> before = new HashMap<String, Object>();
                    HashMap<String, Object> after = new HashMap<String, Object>();
                    DataContent d = dataContentsList.get(i);
                    Struct beforest = d.getBefore();
                    Struct afterst = d.getAfter();

                    //解析insert数据
                    if(d.getOperation().equals("INSERT")){
                        Schema schema = afterst.schema();
                        List<org.apache.kafka.connect.data.Field> fieldLists = schema.fields();

                        StringBuffer insertSt = new StringBuffer();
                        insertSt.append("insert into "+d.getTableName()+"(");
                        for(int index =0;index <fieldLists.size();index++){
                            Field field = fieldLists.get(index);
                            insertSt.append(field.name());
                            if(index != fieldLists.size()-1){
                                insertSt.append(",");
                            }
                        }
                        insertSt.append(") values (");

                        for(int index =0;index <fieldLists.size();index++){
                            insertSt.append("?");
                            if(index != fieldLists.size()-1){
                                insertSt.append(",");
                            }
                            else{
                                insertSt.append(")");
                            }
                        }
                        PreparedStatement pst=synConnection.prepareStatement(insertSt.toString());

                        for(int index =0;index <fieldLists.size();index++){
                            Field field = fieldLists.get(index);
                            logger.info("{}",field.schema());
                            if(field.schema().equals(Schema.STRING_SCHEMA)||field.schema().equals(Schema.OPTIONAL_STRING_SCHEMA)){
                                pst.setString(index+1,afterst.get(field).toString());
                            }
                            else if(field.schema().equals(Schema.BOOLEAN_SCHEMA)||field.schema().equals(Schema.OPTIONAL_BOOLEAN_SCHEMA)){
                                pst.setBoolean(index+1,afterst.getBoolean(field.schema().name()));
                            }
                            else if(field.schema().type().equals(Schema.Type.INT64)){
                                long ts =0;
                                if(field.schema().name() != null && field.schema().name().equals("org.apache.kafka.connect.data.Timestamp")){
                                    Timestamp t = (Timestamp) afterst.get(field.name());
                                    pst.setTimestamp(index+1,t);
                                }
                                else{
                                    ts = afterst.getInt64(field.name());
                                    pst.setLong(index+1,ts);
                                }

                            }
                            else if(field.schema().type().equals(Schema.Type.BYTES)){
                                pst.setBinaryStream(index+1,new ByteArrayInputStream(afterst.getBytes(field.name())),afterst.getBytes(field.name()).length);
                            }
                        }
                        pst.executeUpdate();
                        pst.close();
                    }else if(d.getOperation().equals("UPDATE")){
                        Schema schema_before = beforest.schema();
                        List<org.apache.kafka.connect.data.Field> fieldLists_before = schema_before.fields();

                        List<Field> beforeFields = new ArrayList<>();
                        for(int index =0;index <fieldLists_before.size();index++){
                            Field field = fieldLists_before.get(index);
                            if(beforest.get(field) != null)
                            {
                                beforeFields.add(field);
                            }
                        }
                        StringBuffer whereSt_before = new StringBuffer();
                        whereSt_before.append("WHERE ");
                        for(int index =0;index<beforeFields.size();index++){
                            whereSt_before.append(beforeFields.get(index).name()+ "=?");
                            if(index != beforeFields.size()-1){
                                whereSt_before.append(" and ");
                            }
                        }
                        Schema schema_after = afterst.schema();
                        List<org.apache.kafka.connect.data.Field> fieldLists_after = schema_before.fields();
                        List<Field> afterFields = new ArrayList<>();
                        for(int index =0;index <fieldLists_before.size();index++){
                            Field field = fieldLists_after.get(index);
                            if(afterst.get(field) != null)
                            {
                                afterFields.add(field);
                            }
                        }
                        StringBuffer setSt_after = new StringBuffer();
                        setSt_after.append("SET ");
                        for(int index =0;index<afterFields.size();index++){
                            setSt_after.append(afterFields.get(index).name() + "=?");
                            if(index != afterFields.size()-1){
                                setSt_after.append(" , ");
                            }
                        }


                        String updateStr = "UPDATE "+d.getTableName() +" "+ setSt_after+" "+whereSt_before;

                        logger.info(updateStr);

                        PreparedStatement pst=synConnection.prepareStatement(updateStr.toString());

                        for(int index =0;index <afterFields.size();index++){
                            Field field = afterFields.get(index);
                            if(field.schema().equals(Schema.STRING_SCHEMA)||field.schema().equals(Schema.OPTIONAL_STRING_SCHEMA)){
                                pst.setString(index+1,afterst.get(field).toString());
                            }
                            else if(field.schema().equals(Schema.BOOLEAN_SCHEMA)||field.schema().equals(Schema.OPTIONAL_BOOLEAN_SCHEMA)){
                                pst.setBoolean(index+1,afterst.getBoolean(field.schema().name()));
                            }
                            else if(field.schema().type().equals(Schema.Type.INT64)){
                                long ts =0;
                                if(field.schema().name() != null && field.schema().name().equals("org.apache.kafka.connect.data.Timestamp")){
                                    Timestamp t = (Timestamp) afterst.get(field.name());
                                    pst.setTimestamp(index+1,t);
                                }
                                else{
                                    ts = afterst.getInt64(field.name());
                                    pst.setLong(index+1,ts);
                                }

                            }
                            else if(field.schema().type().equals(Schema.Type.BYTES)){
                                pst.setBinaryStream(index+1,new ByteArrayInputStream(afterst.getBytes(field.name())),afterst.getBytes(field.name()).length);
                            }
                        }
                        for(int index =0;index <beforeFields.size();index++){
                            Field field = beforeFields.get(index);
                            if(field.schema().equals(Schema.STRING_SCHEMA)||field.schema().equals(Schema.OPTIONAL_STRING_SCHEMA)){
                                pst.setString(index+1+afterFields.size(),beforest.get(field).toString());
                            }
                            else if(field.schema().equals(Schema.BOOLEAN_SCHEMA)||field.schema().equals(Schema.OPTIONAL_BOOLEAN_SCHEMA)){
                                pst.setBoolean(index+1+afterFields.size(),beforest.getBoolean(field.schema().name()));
                            }
                            else if(field.schema().type().equals(Schema.Type.INT64)){
                                long ts =0;
                                if(field.schema().name() != null && field.schema().name().equals("org.apache.kafka.connect.data.Timestamp")){
                                    Timestamp t = (Timestamp) beforest.get(field.name());
                                    pst.setTimestamp(index+1+afterFields.size(),t);
                                }
                                else{
                                    ts = beforest.getInt64(field.name());
                                    pst.setLong(index+1+afterFields.size(),ts);
                                }

                            }
                            else if(field.schema().type().equals(Schema.Type.BYTES)){
                                pst.setBinaryStream(index+1+afterFields.size(),new ByteArrayInputStream(beforest.getBytes(field.name())),beforest.getBytes(field.name()).length);
                            }
                        }
                        pst.executeUpdate();
                        pst.close();
                    }
                    else if(d.getOperation().equals("DELETE")){
                        Schema schema = beforest.schema();
                        List<org.apache.kafka.connect.data.Field> fieldLists = schema.fields();
                        List<Field> beforeFields = new ArrayList<>();

                        for(int index =0;index <fieldLists.size();index++){
                            Field field = fieldLists.get(index);
                            if(beforest.get(field) != null)
                            {
                                beforeFields.add(field);
                            }
                        }

                        StringBuffer deleteSt = new StringBuffer();
                        deleteSt.append("delete from "+d.getTableName()+" where ");
                        for(int index =0;index<beforeFields.size();index++){
                            Field field = beforeFields.get(index);
                            deleteSt.append(field.name()+"=?");
                            if(index != beforeFields.size()-1){
                                deleteSt.append(" and ");
                            }
                        }
                        PreparedStatement pst=synConnection.prepareStatement(deleteSt.toString());
                        for(int index =0;index <beforeFields.size();index++){
                            Field field = beforeFields.get(index);
                            if(field.schema().equals(Schema.STRING_SCHEMA)||field.schema().equals(Schema.OPTIONAL_STRING_SCHEMA)){
                                pst.setString(index+1,beforest.get(field).toString());
                            }
                            else if(field.schema().equals(Schema.BOOLEAN_SCHEMA)||field.schema().equals(Schema.OPTIONAL_BOOLEAN_SCHEMA)){
                                pst.setBoolean(index+1,beforest.getBoolean(field.schema().name()));
                            }
                            else if(field.schema().type().equals(Schema.Type.INT64)){
                                long ts =0;
                                if(field.schema().name() != null && field.schema().name().equals("org.apache.kafka.connect.data.Timestamp")){
                                    Timestamp t = (Timestamp) beforest.get(field.name());
                                    pst.setTimestamp(index+1,t);
                                }
                                else{
                                    ts = beforest.getInt64(field.name());
                                    pst.setLong(index+1,ts);
                                }

                            }
                            else if(field.schema().type().equals(Schema.Type.BYTES)){
                                pst.setBinaryStream(index+1,new ByteArrayInputStream(beforest.getBytes(field.name())),beforest.getBytes(field.name()).length);
                            }
                        }
                        pst.executeUpdate();
                        pst.close();
                    }
                }
            }
        }

    }

    public List<DataContent> poll() {

        //开始启动logmner
        try (CallableStatement s = (connection).prepareCall(getDialect().getStatement(LogMinerDialect.Statement.START_LOGMINER))) {
            List<DataContent> dataContentsList = new ArrayList<>();

            if (sessionStartSCN.equals(-1L)) {
                sessionStartSCN = getCurrentSCN();
            }
            //测试数据
            //sessionStartSCN = 586813554L;
            s.setLong(1,sessionStartSCN);

            //拿当前最新的scn作为要分析的结尾SessionSCN
            sessionAnalysisSCN = getCurrentSCN();

            //如果开始SCN大于结束SCN，那暂时就不用执行
            if (sessionStartSCN > sessionAnalysisSCN) {
                return null;
            }

            //如果lastSessionStartSCN <= sessionStartSCN,按照原来的步骤执行下去
            //如果lastSessionStartSCN > sessionStartSCN,说明存在回退位点，需要特殊处理
            //为啥要回退，有的时候一个流程中有可能包涵commitscn，但是没有包涵startscn，会导致这条数据没有被分析出来，是个空的数据，识别到就要回退。
            if (lastSessionStartSCN <= sessionStartSCN) {
                if (sessionAnalysisSCN - sessionStartSCN > stepCount) {
                    sessionAnalysisSCN = sessionStartSCN + stepCount;
                }
            }
            else if(!lastAnalysisSCN.equals(Long.valueOf(0))){
                //如果是回退位点就不需要做判断，就用上次的分析位点做这次的位点
                logger.info("回退到lastSessionStartSCN位点:{}",lastSessionStartSCN);
                logger.info("回退到analysisSCN位点:{}",lastAnalysisSCN);
                sessionAnalysisSCN = lastAnalysisSCN;
            }
            logger.info(getDialect().getStatement(LogMinerDialect.Statement.START_LOGMINER));

            //测试数据
            //sessionAnalysisSCN = 586813854L;
            s.setLong(2,sessionAnalysisSCN);

            //当前解析位置
            logger.info("当前sessionStartSCN：{}", sessionStartSCN);
            logger.info("当前sessionAnalysisSCN：{}", sessionAnalysisSCN);
            //执行
            s.execute();

            //准备需要解析的白名单列表
            //
            String[] whiteTables = whiteTable.split(",");
            StringBuffer where = new StringBuffer("(");
            for (int i = 0; i < whiteTables.length; i++) {
                where.append("(SEG_OWNER = ? AND TABLE_NAME = ? AND COMMIT_SCN >= ?)");
                if (i != whiteTables.length - 1) {
                    where.append(" OR ");
                }
            }
            where.append(")");
            //completeQuery = OracleSql.logmnr_contents + where.toString();


            /////////////实现CLOB--------
            Map<String, String> clobMap = new HashMap<String, String>();

            /////////////实现BLOB--------
            Map<String, String> blobMap = new HashMap<String, String>();

            Map<String, String> updateMap = new HashMap<String, String>();

            Map<String, String> scnMap = new HashMap<String, String>();

            //得到V$LOGMNR_CONTENTS中所有内容，这边处理是因为考虑到11g与12c有些sql可能不同，所以用工厂模式处理下
            String contents_all_Statement = getDialect().getStatement(LogMinerDialect.Statement.CONTENTS);

            //从V$LOGMNR_CONTENTS中获取白名单中的内容
            String contents_all_logminerQueryString = contents_all_Statement + where.toString();

            try (PreparedStatement contents_all_query = connection.prepareCall(contents_all_logminerQueryString)) {
                int paramIdx = 0;
                Iterator<Table> it2 = stateMap.keySet().iterator();
                while (it2.hasNext()) {
                    Table table = it2.next();
                    Offset offset = stateMap.get(table);
                    contents_all_query.setString(++paramIdx, table.getOwnerName());
                    contents_all_query.setString(++paramIdx, table.getTableName());
                    contents_all_query.setLong(++paramIdx, offset.getCommitSystemChangeNumber());
                    logger.info("Set contents_all_query WHERE parameters for {} @ SCN {}", table.getQName(),
                            offset.getCommitSystemChangeNumber());
                }
                ResultSet rs = contents_all_query.executeQuery();



                while (rs != null && rs.next()) {
                    String startscnString = rs.getNString("START_SCN");

                    //通过commit_scn与row_id双重保证
                    Long cmtscn = rs.getLong("COMMIT_SCN");
                    String currrentRowID = rs.getNString("ROW_ID");

                    //由于有情况存在要回退位点找存在的值，所以有些值已经存在就不需要统计在里面
                    //lastSessionCommitSCN = 586799014L;
                    if (cmtscn < lastSessionCommitSCN) {
                        continue;
                    } else if (cmtscn.equals(lastSessionCommitSCN)) {
                        if (currrentRowID.compareTo(lastRow_ID) <= 0) {
                            continue;
                        }
                    }

                    String clobString = "";
                    String blobString = "";
                    String redoSQL = rs.getString("SQL_REDO");
                    String xid = rs.getNString("XID");
                    Boolean csf = rs.getBoolean("CSF");
                    Integer scn = rs.getInt("SCN");
                    Integer commit_scn = rs.getInt("COMMIT_SCN");
                    String row_id = rs.getNString("ROW_ID");
                    String ownerName = rs.getNString("SEG_OWNER");
                    String tableName = rs.getNString("TABLE_NAME");
                    String timestamp = rs.getNString("TIMESTAMP");
                    String opera = rs.getNString("OPERATION");
                    String start_scn = rs.getNString("START_SCN");
                    Integer operatype = rs.getInt("OPERATION_CODE");
                    //String operation = rs.getNString("OPERATION");
                    if (opera != null && start_scn != null && opera.equals("INSERT") && start_scn.equals("0")) {

                        //说明大字段没有被拿到，此条插入语句的相应大字段在上面，需要特殊处理
                        //强制往前退100个位点
                        lastSessionStartSCN = sessionStartSCN;
                        lastAnalysisSCN = sessionAnalysisSCN;
                        sessionStartSCN = sessionStartSCN - 100;

                        //重新开始
                        return null;
                    }

                    //没有redoSQL继续
                    if (redoSQL == null) {
                        continue;
                    }
                    //csf说明是否结束，超过4000个字节，没有结束继续继续下一条
                    while (csf) {
                        rs.next();
                        redoSQL += rs.getString("SQL_REDO");
                        csf = rs.getBoolean("CSF");
                        if (redoSQL.contains("temporary table"))
                            continue;
                    }
                    if (redoSQL.contains("DECLARE") && redoSQL.contains("END")) {
                        //支持二进制
                        int update_b_i = redoSQL.indexOf("into loc_b from");
                        int update_b_ii = redoSQL.indexOf("where");
                        int update_b_iii = redoSQL.indexOf(" for update;");

                        int update_c_i = redoSQL.indexOf("into loc_c from");
                        int update_c_ii = redoSQL.indexOf("where");
                        int update_c_iii = redoSQL.indexOf(" for update;");

                        int loc_i = redoSQL.indexOf("select \"");

                        int loc_c_ii = redoSQL.indexOf("\" into loc_c from");

                        int loc_b_ii = redoSQL.indexOf("\" into loc_b from");

                        if ((loc_i != -1) && (loc_c_ii != -1)) {
                            String columnString = redoSQL.substring(loc_i + 8, loc_c_ii);

                            String guid = xid + seperatorWord + ownerName + seperatorWord + tableName + seperatorWord + columnString;

                            if (!updateMap.containsKey(guid)) {
                                String sqlString = "update " + redoSQL.substring(update_c_i + 15, update_c_ii) + "set \"" + columnString + "\"='EMPTY_CLOB()' " + redoSQL.substring(update_c_ii, update_c_iii);
                                updateMap.put(guid, sqlString);
                                scnMap.put(guid, scn + "*" + commit_scn + "*" + row_id + "*" + ownerName + "*" + tableName + "*" + timestamp);
                            }

                            if (clobMap.containsKey(guid)) {
                                clobString += clobMap.get(guid).toString();
                            }

                            if (redoSQL.indexOf("buf_c := '") != -1) {
                                int clobi = redoSQL.indexOf("buf_c := '");
                                int clobii = redoSQL.indexOf("'; \n" +
                                        "  dbms_lob.write");
                                String nowclobString = redoSQL.substring(clobi + 10, clobii);

                                clobString += nowclobString;
                                clobMap.put(guid, clobString);
                            }
                        } else if ((loc_i != -1) && (loc_b_ii != -1)) {
                            String columnString = redoSQL.substring(loc_i + 8, loc_b_ii);
                            //用xid、ownerName、tableName、columnString唯一标识当前二进制字段的guid
                            //#!>-<!#为分隔符
                            //例如：99001800E16A0000#!>-<!#EPOINT#!>-<!#BASEINFO1#!>-<!#IMAGE
                            String guid = xid + seperatorWord + ownerName + seperatorWord + tableName + seperatorWord + columnString;

                            if (!updateMap.containsKey(guid)) {
                                String sqlString = "update " + redoSQL.substring(update_b_i + 15, update_b_ii) + "set \"" + columnString + "\"='EMPTY_BLOB()' " + redoSQL.substring(update_b_ii, update_b_iii);
                                updateMap.put(guid, sqlString);
                            }

                            if (blobMap.containsKey(guid)) {
                                blobString += blobMap.get(guid).toString();
                            }
                            if (redoSQL.indexOf("buf_b := HEXTORAW('") != -1) {
                                int blobi = redoSQL.indexOf("buf_b := HEXTORAW('");
                                int blobii = redoSQL.indexOf("'); \n" +
                                        "  dbms_lob.write");
                                String nowblobString = redoSQL.substring(blobi + 19, blobii);

                                blobString += nowblobString;
                                blobMap.put(guid, blobString);
                            }
                        }
                        continue;

                    }
                    if (operatype == 1 || operatype == 2 || operatype == 3) {
                        DataContent dataContent = new DataContent(scn, commit_scn, row_id, xid,ownerName, tableName, timestamp,
                                opera, redoSQL,null, null);
                        dataContentsList.add(dataContent);
                    }
                }
                for(int index = 0;index<dataContentsList.size();index++){
                    String redoSQL = dataContentsList.get(index).getRedoSQL();
                    String xid = dataContentsList.get(index).getXid();
                    Integer scn = dataContentsList.get(index).getScn();
                    Integer commit_scn = dataContentsList.get(index).getCommit_scn();
                    String row_id = dataContentsList.get(index).getRowid();
                    String ownerName = dataContentsList.get(index).getSEG_OWNER();
                    String tableName = dataContentsList.get(index).getTableName();
                    String timestamp = dataContentsList.get(index).getTimeStamp();
                    String opera = dataContentsList.get(index).getOperation();
                    Table table = new Table(databaseName, ownerName, tableName);
                    Schema rowSchema;
                    if (schemas.containsKey(table)) {
                        rowSchema = schemas.get(table);
                        logger.trace("{} retrieved from cache", rowSchema.toString());
                    } else {
                        rowSchema = createRowSchema(table);
                        schemas.put(table, rowSchema);
                        logger.info("{} created and cached", rowSchema.toString());
                    }
                    //交易标识符的原始表示+实例名称+表名
                    String guid = xid + seperatorWord + ownerName + seperatorWord + tableName + seperatorWord;

                    Struct eventStruct;
                    try {
                        Map<String, Map<String, String>> changes = LogMinerSQLParser.parseRedoSQL(redoSQL);
                        Set<Map.Entry<String, Map<String, String>>> sets = changes.entrySet();
                        for (Map.Entry<String, Map<String, String>> entry : sets) {
                            Map<String, String> chaMap = entry.getValue();
                            //////////CLOB---------Insert支持
                            List<String> unistr_list_clob = new ArrayList<String>();
                            List<String> columnList_clob = new ArrayList<String>();
                            //////////


                            //////////BLOB---------Insert支持
                            List<String> unistr_list_blob = new ArrayList<String>();
                            List<String> columnList_blob = new ArrayList<String>();
                            //////////

                            List<String> unistr_list = new ArrayList<String>();
                            List<String> columnList = new ArrayList<String>();
                            Set<Map.Entry<String, String>> changesset = chaMap.entrySet();
                            for (Map.Entry<String, String> c : changesset) {
                                if (isOracleFunc(c.getValue())) {
                                    unistr_list.add(c.getValue());
                                    columnList.add(c.getKey());
                                }

                                if (c.getValue().equals("EMPTY_CLOB()")) {
                                    unistr_list_clob.add(c.getValue());
                                    columnList_clob.add(c.getKey());
                                } else if (c.getValue().equals("EMPTY_BLOB()")) {
                                    unistr_list_blob.add(c.getValue());
                                    columnList_blob.add(c.getKey());
                                }
                            }


                            if (!entry.getKey().equals("BEFORE")) {

                                if (!columnList_clob.isEmpty()) {
                                    for (int i = 0; i < columnList_clob.size(); i++) {
                                        String uni_key = columnList_clob.get(i);
                                        String insertguid = guid + uni_key;

                                        if (clobMap.containsKey(insertguid)) {
                                            chaMap.put(uni_key, clobMap.get(insertguid));
                                        } else {
                                            chaMap.put(uni_key, " ");
                                        }
                                    }

                                    changes.put(entry.getKey(), chaMap);
                                } else if (!clobMap.isEmpty()) {
                                    for (String sguid : clobMap.keySet()) {

                                        //String guid = xid+seperatorWord+owner+seperatorWord+tablename+seperatorWord+columnString;
                                        String[] guids = sguid.split(seperatorWord);

                                        //如果xid、owner、tablename一致说明在一个事务中，需要组装到一起
                                        if (guids[0].compareTo(xid) == 0 && guids[1].compareTo(ownerName) == 0 && guids[2].compareTo(tableName) == 0) {

                                            //如果上面的if没识别到运行到这边说明是update
                                            chaMap.put(guids[3], clobMap.get(sguid));
                                        }
                                    }
                                    changes.put(entry.getKey(), chaMap);
                                }

                                if (!columnList_blob.isEmpty()) {
                                    for (int i = 0; i < columnList_blob.size(); i++) {
                                        String uni_key = columnList_blob.get(i);
                                        String insertguid = guid + uni_key;

                                        if (blobMap.containsKey(insertguid)) {
                                            String bString = blobMap.get(insertguid);
                                            chaMap.put(uni_key, bString);
                                        } else {
                                            chaMap.put(uni_key, " ");
                                        }

                                    }

                                    changes.put(entry.getKey(), chaMap);
                                } else if (!blobMap.isEmpty()) {
                                    for (String sguid : blobMap.keySet()) {

                                        //String guid = xid+seperatorWord+owner+seperatorWord+tablename+seperatorWord+columnString;
                                        String[] guids = sguid.split(seperatorWord);

                                        //如果xid、owner、tablename一致说明在一个事务中，需要组装到一起
                                        if (guids[0].compareTo(xid) == 0 && guids[1].compareTo(ownerName) == 0 && guids[2].compareTo(tableName) == 0) {

                                            //如果上面的if没识别到运行到这边说明是update
                                            chaMap.put(guids[3], blobMap.get(sguid));
                                        }
                                        ;
                                    }
                                    changes.put(entry.getKey(), chaMap);
                                }

                            }

                            //oracle引擎处理
                            if (!columnList.isEmpty()) {
                                // 拼接需要oracle引擎处理的字符串
                                String uniStr = "";
                                for (int i = 0; i < unistr_list.size(); i++) {
                                    if (i != unistr_list.size() - 1) {
                                        uniStr += unistr_list.get(i).toString() + ",";
                                    } else {
                                        uniStr += unistr_list.get(i).toString();
                                    }
                                }
                                // 拼接Sql
                                String sql_uniStr = "select " + uniStr + " from dual";
                                ResultSet resultSet_uniStr = null;
                                try (Statement st = getConnection().createStatement()) {
                                    resultSet_uniStr = st.executeQuery(sql_uniStr);
                                    // 解析oracle处理得到的值
                                    while (resultSet_uniStr.next()) {
                                        for (int i = 0; i < unistr_list.size(); i++) {
                                            String uni_key = columnList.get(i);
                                            String uni_value = resultSet_uniStr.getObject(i + 1) + "";

                                            if (chaMap.containsKey(uni_key)) {
                                                chaMap.put(uni_key, uni_value);
                                            }
                                        }
                                    }
                                    changes.put(entry.getKey(), chaMap);
                                }

                            }
                        }

                        Struct before = createDataStruct(rowSchema,
                                changes.get("BEFORE"));
                        Struct after = createDataStruct(rowSchema,
                                changes.get("AFTER"));
                        dataContentsList.get(index).setBefore(before);
                        dataContentsList.get(index).setAfter(after);
                    } catch (JSQLParserException e) {
                        throw new SQLException("Cannot parse log miner redo SQL", e);
                    }
                }
            }

            if(dataContentsList.size() != 0) {
                Long scn_Long = (long) 0;
                Long commit_scn_Long = (long)0;
                String commit_row_id = "-1";
                //最大的RS_ID所在的数据
                String cdc_commit_scn_string = "select START_SCN,COMMIT_SCN,ROW_ID from V$LOGMNR_CONTENTS WHERE RS_ID =(SELECT max(RS_ID) FROM V$LOGMNR_CONTENTS WHERE ROW_ID=(SELECT  max(ROW_ID) FROM V$LOGMNR_CONTENTS where COMMIT_SCN=(select max(COMMIT_SCN) from V$LOGMNR_CONTENTS where (("+where.toString()+")))))";
                try (PreparedStatement mining_cdc_commit_scn = connection.prepareCall(cdc_commit_scn_string)){
                    mining_cdc_commit_scn.setFetchSize(0);
                    int paramIdx = 0;
                    Iterator<Table> it2 = stateMap.keySet().iterator();
                    while (it2.hasNext()) {
                        Table table = it2.next();
                        Offset offset = stateMap.get(table);
                        String o = table.getOwnerName();
                        String t = table.getTableName();
                        Long com = offset.getCommitSystemChangeNumber();

                        logger.info("o {}", o);
                        logger.info("t {}", t);
                        logger.info("com {}", com);

                        mining_cdc_commit_scn.setString(++paramIdx, table.getOwnerName());
                        mining_cdc_commit_scn.setString(++paramIdx, table.getTableName());
                        mining_cdc_commit_scn.setLong(++paramIdx, offset.getCommitSystemChangeNumber());

                        logger.trace("Set mining query WHERE parameters for {} @ SCN {}", table.getQName(),
                                offset.getCommitSystemChangeNumber());
                    }
                    ResultSet rss = mining_cdc_commit_scn.executeQuery();
                    while (rss != null && rss.next()) {
                        scn_Long = rss.getLong(1);
                        commit_scn_Long = rss.getLong(2);
                        commit_row_id = rss.getString(3);
                    }
                    rss.close();
                }
                //上一轮的startscn
                lastSessionStartSCN = sessionStartSCN;

                //上一轮的分析结束的scn作为下一轮的起始sessionStartSCN
                sessionStartSCN = sessionAnalysisSCN;

                //上一轮的结束sessionAnalysisScn
                lastAnalysisSCN = sessionAnalysisSCN;

                if(scn_Long !=0) {
                    //如果查询到的startscn与当前的sessionStartSCN一致，并且分析已经到达步长，需要前进，避免循环
                    if((scn_Long.equals(sessionStartSCN))&&(sessionAnalysisSCN - lastSessionStartSCN) >= stepCount) {
                        scn_Long++;
                    }
                    sessionStartSCN = scn_Long;
                }
                if(commit_scn_Long != 0) {
                    lastSessionCommitSCN = commit_scn_Long;

                }

                //上一轮的commit_row_id
                lastRow_ID = commit_row_id;
            }
            else {
                //如果此轮没有采集到数据

                lastSessionStartSCN = sessionStartSCN;
                sessionStartSCN = sessionAnalysisSCN;
                lastAnalysisSCN = sessionAnalysisSCN;
            }


            return dataContentsList;

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return null;
    }

    /*
     * @Description:获取当前oracle的scn
     * @Param:
     * @return: java.lang.Long
     * @Author: miaoneng
     * @Date: 2021/4/7 17:58
     */
    Long getCurrentSCN() throws SQLException {
        Long scn = 0L;
        try (PreparedStatement preparedStatement = connection.prepareStatement(OracleSql.CURRENT_SCN)) {
            try (ResultSet r = preparedStatement.executeQuery()) {
                while (r.next()) {
                    scn = r.getLong(1);
                }
            }
        }
        return scn;
    }

    Long getSessionStartSCN() {

        return Long.valueOf(-1);
    }

    /**
     * oracle解析白名单中的需要日志分析表
     */
    public List<Table> filterTables(List<Table> visibleTables, List<String> whiteList) {
        Set<String> whiteListSet = whiteList.isEmpty() ? null : new HashSet<>(whiteList);
        final List<Table> filteredTables = new ArrayList<>(visibleTables.size());
        if (whiteList != null) {
            for (Table table : visibleTables) {
                if (table.matches(whiteListSet)) {
                    filteredTables.add(table);
                }
            }
        }
        return filteredTables;
    }

    /*
     * @Description:得到oracle中所有可见表
     * @Param:
     * @return: java.util.List<oracle.logminer.model.Table>
     * @Author: miaoneng
     * @Date: 2021/4/28 14:57
     */
    public List<Table> getVisibleTables() throws SQLException, ConnectException {
        logger.trace("Retrieving list of tables visible in log miner session");
        //只有cdb用户才能拿到用户表
        List<Table> list = getDialect().getTables(getConnection());

        //实例名，一个session只有唯一一个实例名
        if (list != null && list.size() != 0 ) {
            databaseName = list.get(0).getDatabaseName();
        }
        return list;
    }

    private synchronized Connection getConnection() throws SQLException {
        try {
            if (connection == null) {
                initializeConnection();
            } else if (!isValid(connection, VALIDITY_CHECK_TIMEOUT)) {
                logger.info("Database connection is invalid. Reconnecting...");
                close();
            }
        } catch (SQLException e) {
            logger.warn("Connection initialization failure", e);
            throw new SQLException(e);
        }
        return connection;
    }

    private synchronized Connection getPdbConnection() throws SQLException {
        try {
            if (pdbConnection == null) {
                initializePdbConnection();
            } else if (!isValid(pdbConnection, VALIDITY_CHECK_TIMEOUT)) {
                logger.info("Database connection is invalid. Reconnecting...");
                close();
            }
        } catch (SQLException e) {
            logger.warn("Connection initialization failure", e);
            throw new SQLException(e);
        }
        return pdbConnection;
    }

    private boolean isValid(Connection connection, int timeout) throws SQLException {
        if (connection.getMetaData().getJDBCMajorVersion() >= 4) {
            return connection.isValid(timeout);
        }
        return false;
    }

    private void initializeConnection() throws SQLException {
        String jdbcURL = this.jdbcURL;
        String user = this.user;
        String password = this.password;
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        connection = DriverManager.getConnection(jdbcURL, properties);
    }

    /*
     * @Description:12c初始化pdb账户
     * @Param:
     * @return: void
     * @Author: miaoneng
     * @Date: 2021/5/26 14:08
     */
    private void initializePdbConnection() throws SQLException {
        String _jdbcURL = properties.getProperty("pdb.url");
        String _user = properties.getProperty("pdb.user");
        String _password = properties.getProperty("pdb.password");

        Properties properties = new Properties();
        properties.setProperty("user", _user);
        properties.setProperty("password", _password);
        pdbConnection = DriverManager.getConnection(_jdbcURL, properties);
    }
    public synchronized void close() {
        try {
            if (started) {
                logger.info("Stopping log miner session");

                try (CallableStatement s = connection
                        .prepareCall(getDialect().getStatement(LogMinerDialect.Statement.STOP_LOGMINER))) {
                    s.execute();
                }

                logger.debug("Log miner session ended");
            }

            if (connection != null) {
                logger.info("Closing database connection");
                connection.close();
            }

        } catch (SQLException e) {
            logger.warn("Ignoring error closing session JDBC resources", e);
        } finally {
            connection = null;
        }
    }

    //构建元数据
    private Schema createRowSchema(Table table) throws SQLException {
        logger.debug("Creating schema for {}", table.getQName());
        SchemaBuilder structBuilder = SchemaBuilder.struct()
                .name(table.getQName() + ".ROW");
        // TODO: consider using dictionary LAST_DDL_TIME to Integer magic to set schema
        // version


        Connection connection;
        connection = getConnection();
        //通过sql得到表的数据类型
        try (PreparedStatement dictionaryQuery = connection.prepareStatement(getDialect().getStatement(LogMinerDialect.Statement.DICTIONARY))) {

            logger.info(getDialect().getStatement(LogMinerDialect.Statement.DICTIONARY));
            if (isMultitenant()) {
                dictionaryQuery.setString(1, table.getDatabaseName());
                dictionaryQuery.setString(2, table.getOwnerName());
                dictionaryQuery.setString(3, table.getTableName());
            }
            else {
                dictionaryQuery.setString(1, table.getOwnerName());
                dictionaryQuery.setString(2, table.getTableName());
            }

            try (ResultSet drs = dictionaryQuery.executeQuery()) {
                if (drs.isBeforeFirst()) {
                    // TODO: No result, charf
                }
                while (drs.next()) {
                    String columnName = drs.getString("COLUMN_NAME");

                    //不关系是否可以为空，只关心可以解析
                    Boolean nullable = true;
                    String dataType = drs.getString("DATA_TYPE");
                    if (dataType.contains("TIMESTAMP"))
                        dataType = "TIMESTAMP";
                    int dataLength = drs.getInt("DATA_LENGTH");
                    int dataScale = drs.getInt("DATA_SCALE");
                    int dataPrecision = drs.getInt("DATA_PRECISION");

                    //这个两个参数遗憾还没用
                    //主键
                    Boolean pkColumn = drs.getInt("PK_COLUMN") == 1 ? true : false;
                    //不能重复
                    Boolean uqColumn = drs.getInt("UQ_COLUMN") == 1 ? true : false;
                    Schema columnSchema = null;

                    switch (dataType) {
                        case "NUMBER": {
                            if (dataScale > 0 || dataPrecision == 0) {
                                columnSchema = nullable ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
                            } else {
                                switch (dataPrecision) {
                                    case 1:
                                    case 2:
                                        columnSchema = nullable ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                                        break;
                                    case 3:
                                    case 4:
                                        columnSchema = nullable ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
                                        break;
                                    case 5:
                                    case 6:
                                    case 7:
                                    case 8:
                                    case 9:
                                        columnSchema = nullable ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
                                        break;
                                    default:
                                        columnSchema = nullable ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
                                        break;
                                }
                            }
                            break;
                        }
                        case "CHAR":
                        case "VARCHAR":
                        case "VARCHAR2":
                        case "NCHAR":
                        case "NVARCHAR":
                        case "NVARCHAR2":
                        case "LONG":
                        case "CLOB": {
                            columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
                            break;
                        }
                        case "BLOB": {
                            columnSchema = nullable ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
                            break;
                        }
                        case "DATE":
                        case "TIMESTAMP": {
                            columnSchema = nullable ? org.apache.kafka.connect.data.Timestamp.builder().optional().build()
                                    : org.apache.kafka.connect.data.Timestamp.builder().build();
                            break;
                        }
                        default:
                            columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
                            break;
                    }
                    structBuilder.field(columnName, columnSchema);
                }
            }
        }


        return structBuilder.build();
    }


    private Struct createDataStruct(Schema schema, Map<String, String> data) {
        Struct dataStruct = new Struct(schema);
        for (String field : data.keySet()) {
            String value = data.get(field);
            Schema fieldSchema = schema.field(field).schema();
            if (value != null && !"NULL".equalsIgnoreCase(value)) {
                dataStruct.put(field, convertFieldValue(value, fieldSchema));
            } else {
                dataStruct.put(field, null);
            }
        }
        return dataStruct;
    }

    private Object convertFieldValue(String value, Schema fieldSchema) {
        if (fieldSchema == null || fieldSchema.equals(Schema.STRING_SCHEMA) || "null".equalsIgnoreCase(value) || null == value) {
            return value;
        }
        if (fieldSchema.equals(Schema.INT8_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_INT8_SCHEMA)) {
            return Byte.parseByte(value);
        }
        if (fieldSchema.equals(Schema.INT16_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_INT16_SCHEMA)) {
            return Short.parseShort(value);
        }
        if (fieldSchema.equals(Schema.INT32_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_INT32_SCHEMA)) {
            return Integer.parseInt(value);
        }
        if (fieldSchema.equals(Schema.INT64_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_INT64_SCHEMA)) {
            return Long.parseLong(value);
        }
        if (fieldSchema.equals(Schema.FLOAT64_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_FLOAT64_SCHEMA)) {
            return Double.parseDouble(value);
        }
        if (fieldSchema.equals(Schema.BYTES_SCHEMA) || fieldSchema.equals(Schema.OPTIONAL_BYTES_SCHEMA)) {
            return parseHexStr2Byte(value);
        }
        if (fieldSchema.equals(org.apache.kafka.connect.data.Timestamp.builder().optional().build())
                || fieldSchema.equals(org.apache.kafka.connect.data.Timestamp.builder().build())) {
            if (value.equals("NULL")) {
                value = null;
            } else {
                return Timestamp.valueOf(value);
            }

        }
        return value;
    }


    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1)
            return null;
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),
                    16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }

    /*
     * @Description:判断是否是oracle的函数
     * @Param: s
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/4/28 17:31
     */
    private boolean isOracleFunc(String s) {
        if (s.startsWith("UNISTR") || s.startsWith("TO_DATE") || s.startsWith("TRANSLATE")
                || s.startsWith("TO_TIMESTAMP_TZ") || s.startsWith("TO_TIMESTAMP") || s.startsWith("TO_NUMBER")
                || s.startsWith("TO_NCLOB") || s.startsWith("TO_NCHAR") || s.startsWith("TO_MULTI_BYTE")
                || s.startsWith("TO_LOB") || s.startsWith("TO_CLOB") | s.startsWith("TO_CHAR")
                || s.startsWith("TO_BINARY_FLOAT") || s.startsWith("TO_BINARY_DOUBLE") || s.startsWith("ROWIDTOCHAR")
                || s.startsWith("ASCIISTR") || s.startsWith("CONVERT") || s.startsWith("NUMTODSINTERVAL")
                || s.startsWith("NUMTOYMINTERVAL")) {
            return true;
        }
        return false;

    }


    private BaseLogMinerDialect getDialect() throws SQLException {
        if (isMultitenant()) {
            return LogMinerSQLFactory.getInstance(LogMinerSQLFactory.Strategy.MULTITENANT);
        }
        return LogMinerSQLFactory.getInstance(LogMinerSQLFactory.Strategy.SINGLE_INSTANCE);
    }
    public boolean isMultitenant() throws SQLException {
        if (multitenant != null) {
            return multitenant;
        }

        try (PreparedStatement p = getConnection()
                .prepareStatement("SELECT COUNT(*) FROM DBA_VIEWS WHERE VIEW_NAME = 'CDB_TAB_COLS'")) {
            ResultSet rs = p.executeQuery();
            while (rs.next()) {
                if (rs.getInt(1) == 1) {
                    /*
                     * We would hope that the user specifies the CDB in the JDBC URL, but does not
                     * hurt to check. This needs to succeed for logminer to work in multitenant env.
                     */
                    try (Statement s = getConnection().createStatement()) {
                        s.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
                        logger.debug("Set session multitenant container = CDB$ROOT");
                        multitenant = Boolean.TRUE;
                    }
                }
            }
        }
        if (multitenant == null) {
            multitenant = Boolean.FALSE;
        }
        return multitenant;
    }
    @Test
    public void Test() throws IOException, SQLException, ParseException, InterruptedException {
        init();
        start(-1L);
    }
}
