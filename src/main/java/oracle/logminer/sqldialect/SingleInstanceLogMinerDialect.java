package oracle.logminer.sqldialect;

import oracle.logminer.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @program:$(PROJECT_NAME)
 * @description
 * @author:miaoneng
 * @create:2021-04-28 14:08
 **/
public class SingleInstanceLogMinerDialect extends BaseLogMinerDialect {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleInstanceLogMinerDialect.class);
    private static final String PROPERTIES_FILE = "/oracle/sql-single.properties";

    private static Map<Statement, String> STATEMENTS;

    static {
        STATEMENTS = new HashMap<Statement, String>();
        try {
            initializeStatements(STATEMENTS, PROPERTIES_FILE);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public String getStatement(Statement statement) {
        if (STATEMENTS.containsKey(statement)) {
            return STATEMENTS.get(statement);
        }
        return super.getStatement(statement);
    }

    @Override
    public List<Table> getTables(Connection connection) throws SQLException {
        List<Table> tables = new ArrayList<Table>();
        String query = getStatement(Statement.TABLES);
        String databaseName = "";
        try (PreparedStatement p = connection.prepareStatement("select name from v$database")) {
            ResultSet rs = p.executeQuery();
            while (rs.next()) {
                databaseName = rs.getString("NAME");
                break;
            }
        }
        //如果databasename，为空，有问题
        //throw一个错误
        if(databaseName.equals("")) throw new RuntimeException("databasename，为空，有问题");

        try (PreparedStatement p = connection.prepareStatement(query)) {
            ResultSet rs = p.executeQuery();
            while (rs.next()) {
                tables.add(new Table(databaseName,rs.getString(1), rs.getString(2)));
            }
        }
        return tables;
    }
}
