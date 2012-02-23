package net.opentsdb;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Created by IntelliJ IDEA.
 * User: mbreitung
 * Date: 12/23/11
 * Time: 10:59 AM
 */
public class BuildData {

	public static final String BUNDLE_NAME = "opentsdb_build";

	private static ResourceBundle resourceBundle;
	private static final String REVISION_STRING;
	private static final String BUILD_STRING;

	/**
	 * Short revision at which this package was built.
	 */
	public static final String short_revision;

	/**
	 * Full revision at which this package was built.
	 */
	public static final String full_revision;

	/**
	 * UTC date at which this package was built.
	 */
	public static final String date;

	/**
	 * UNIX timestamp of the time of the build.
	 */
	public static final long timestamp;

	/**
	 * Represents the status of the repository at the time of the build.
	 */
	public static enum RepoStatus {
		/**
		 * The status of the repository was unknown at the time of the build.
		 */
		UNKNOWN,
		/**
		 * There was no local modification during the build.
		 */
		MINT,
		/**
		 * There were some local modifications during the build.
		 */
		MODIFIED
	}

	/**
	 * Status of the repository at the time of the build.
	 */
	public static final RepoStatus repo_status;

	/**
	 * Username of the user who built this package.
	 */
	public static final String user;

	/**
	 * Host on which this package was built.
	 */
	public static final String host;

	/**
	 * Path to the repository in which this package was built.
	 */
	public static final String repo;

	static {
		resourceBundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.ENGLISH);
		short_revision = getValueFromBundle("short_revision", "000001");
		full_revision = getValueFromBundle("full_revision", "0000000000000001");
		date = getValueFromBundle("date", "1970/01/01 00:00:00 +0000");
		timestamp = Long.parseLong(getValueFromBundle("timestamp", "000000001"));
		repo_status = RepoStatus.valueOf(getValueFromBundle("repo_status", RepoStatus.UNKNOWN.name()));
		user = getValueFromBundle("user", "defaultUser");
		host = getValueFromBundle("host", "default-user.opentsdb.net");
		repo = getValueFromBundle("repo", "/Users/defaultUser/development/opentsdb");
		REVISION_STRING = createRevisionString();
		BUILD_STRING = createBuildString();
	}

	private static String createRevisionString() {
		return new StringBuilder().append("net.opentsdb built at revision ").append(short_revision).append(" ").append(repo_status).toString();
	}

	private static String createBuildString() {
		return new StringBuilder().append("Built on + ").append(date).append("by ").append(user).append("@").append(host).append(":").append(repo).toString();
	}

	private static String getValueFromBundle(String key, String defaultValue) {
		String value = resourceBundle.getString(key);
		return (null != value && value.length() > 0) ? value : defaultValue;
	}

	/**
	 * Human readable string describing the revision of this package.
	 *
	 * @return Revision String
	 */
	public static String revisionString() {
		return REVISION_STRING;
	}

	/**
	 * Human readable string describing the build information of this package.
	 *
	 * @return Build String
	 */
	public static String buildString() {
		return BUILD_STRING;
	}

	// Can't instantiate.
	private BuildData() {
	}
}
