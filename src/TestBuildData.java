package net.opentsdb;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNotSame;

/**
 * Created by IntelliJ IDEA.
 * User: mbreitung
 * Date: 12/28/11
 * Time: 10:23 AM
 */
public class TestBuildData {

	@Before
	public void init() throws IOException {
		runProcess("./build-aux/gen_build_data.sh opentsdb_build.properties");
	}

	private String runProcess(String executeThis) throws IOException {
		Process process = Runtime.getRuntime().exec(executeThis);
		try {
			process.waitFor();
			BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String result = "";
			String line = null;
			while ((line = in.readLine()) != null) {
				result += line;
			}
			return result;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Test
	public void getPropertiesFromFile() throws Exception {
		System.out.println("checking known properties");

		assertEquals(runProcess("hostname"), BuildData.host);
		assertEquals(runProcess("whoami"), BuildData.user);
		assertEquals(runProcess("pwd"), BuildData.repo);

		System.out.println("checking other properties");

		assertNotNull(BuildData.repo_status);
		assertNotSame(BuildData.RepoStatus.UNKNOWN, BuildData.repo_status);

		assertNotNull(BuildData.short_revision);
		assertNotSame("000001", BuildData.short_revision);

		assertNotNull(BuildData.timestamp);
		assertNotSame("000000001", BuildData.timestamp);
	}

}
