package oracle.logminer.sqldialect;

import java.util.HashMap;
import java.util.Map;

public class LogMinerSQLFactory {
	private static Map<Strategy, BaseLogMinerDialect> INSTANCES;

	static {
		INSTANCES = new HashMap<Strategy, BaseLogMinerDialect>();
		INSTANCES.put(Strategy.SINGLE_INSTANCE, new SingleInstanceLogMinerDialect());
		INSTANCES.put(Strategy.MULTITENANT, new MultitenantLogMinerDialect());
	}

	private LogMinerSQLFactory() {
	}

	public static BaseLogMinerDialect getInstance(Strategy strategy) {
		return INSTANCES.get(strategy);
	}

	public enum Strategy {
		SINGLE_INSTANCE, MULTITENANT
	};
}
