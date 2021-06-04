package oracle.logminer.model;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class Offset {
	private Long systemChangeNumber;
	private Long commitSystemChangeNumber;
	private String rowId;
	private Date timestamp;

	public static final Offset DEFAULT_OFFSET = new Offset(0L, 0L, null, null);
	
	public Offset(Long systemChangeNumber, Long commitSystemChangeNumber, String rowId, Date timestamp) {
		super();
		this.systemChangeNumber = systemChangeNumber;
		this.commitSystemChangeNumber = commitSystemChangeNumber;
		this.rowId = rowId;
		this.timestamp = timestamp;
	}

	public Long getSystemChangeNumber() {
		return systemChangeNumber;
	}

	public void setSystemChangeNumber(Long systemChangeNumber) {
		this.systemChangeNumber = systemChangeNumber;
	}

	public Long getCommitSystemChangeNumber() {
		return commitSystemChangeNumber;
	}

	public void setCommitSystemChangeNumber(Long commitSystemChangeNumber) {
		this.commitSystemChangeNumber = commitSystemChangeNumber;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.rowId = rowId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();

		map.put("SCN", this.systemChangeNumber);
		map.put("COMMIT_SCN", this.commitSystemChangeNumber);
		map.put("ROW_ID", this.rowId);
		map.put("TIMESTAMP", this.timestamp);

		return map;
	}

	public static Offset fromMap(Map<String, Object> map) {
		if (map == null || map.keySet().size() == 0) {
			return DEFAULT_OFFSET;
		}

		Long mapSystemChangeNumber = map.get("SCN") == null ? null
				: (Long) map.get("SCN");
		Long mapCommitSystemChangeNumber = map.get("COMMIT_SCN") == null ? null
				: (Long) map.get("COMMIT_SCN");
		String mapRowId = map.get("ROW_ID") == null ? null
				: (String) map.get("ROW_ID");
		Date mapTimestamp = map.get("TIMESTAMP") == null ? null
				: (Date) map.get("TIMESTAMP");

		return new Offset(mapSystemChangeNumber, mapCommitSystemChangeNumber, mapRowId, mapTimestamp);
	}
}
