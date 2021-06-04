package oracle.logminer.sqldialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @program:$(PROJECT_NAME)
 * @description 11g和12c有些差别，故见一个抽象基类
 * @author:miaoneng
 * @create:2021-04-28 11:25
 **/
public abstract class BaseLogMinerDialect  implements LogMinerDialect{

    private static final Logger logger = LoggerFactory.getLogger(BaseLogMinerDialect.class);
    protected static final Charset SQL_FILE_ENCODING = Charset.forName("UTF8");
    private static final String PROPERTIES_FILE =  "/oracle/sql-base.properties";

    private static Map<Statement, String> STATEMENTS;

    static {
        STATEMENTS = new HashMap<Statement,String>();

    }

    protected static void initializeStatements(Map<Statement,String> statements,String pointersFile) throws IOException, URISyntaxException {
        Properties pointers = new Properties();
        InputStream is = BaseLogMinerDialect.class.getResourceAsStream(pointersFile);
        if(is == null){
            throw new IOException("Cannot find statement SQL file " + pointersFile);
        }
        pointers.load(is);

        for (String pointer : pointers.stringPropertyNames()) {
            String pointerLocation = pointers.getProperty(pointer);
            URL resourceURL = BaseLogMinerDialect.class.getResource(pointerLocation);
            if (resourceURL == null) {
                throw new IllegalStateException(String.format("Cannot initialize SQL resource %s from file %s", pointer, pointerLocation));
            }
            URI resourceURI = resourceURL.toURI();
            logger.info("{}: {}={}", pointersFile, pointer, resourceURI.toString());

            Statement s = Statement.get(pointer);
            statements.put(s, getStatementSQL(resourceURI));
        }
    }
    protected static String getStatementSQL(URI resourceURI) throws IOException {
        String sql = null;
        if (resourceURI.getScheme().equals("file")) {
            final Path path = Paths.get(resourceURI);
            byte[] sqlBytes = Files.readAllBytes(path);
            sql = new String(sqlBytes, SQL_FILE_ENCODING);
        } else if (resourceURI.getScheme().equals("jar")) {
            FileSystem fs = null;
            try {
                final String[] array = resourceURI.toString().split("!");
                fs = FileSystems.newFileSystem(URI.create(array[0]), Collections.<String, Object>emptyMap());
                final Path path = fs.getPath(array[1]);
                byte[] sqlBytes = Files.readAllBytes(path);
                sql = new String(sqlBytes, SQL_FILE_ENCODING);
            } finally {
                if (fs != null) {
                    fs.close();
                }
            }
        }
        return sql;
    }

    public String getStatement(Statement statement) {
        return STATEMENTS.get(statement);
    }
}
