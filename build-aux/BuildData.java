// Decompiled by DJ v3.11.11.95 Copyright 2009 Atanas Neshkov  Date: 8/19/2014 1:02:11 PM
// Home Page: http://members.fortunecity.com/neshkov/dj.html  http://www.neshkov.com/dj.html - Check often for new version!
// Decompiler options: packimports(3) 
// Source File Name:   BuildData.java

package net.opentsdb;


public final class BuildData
{
  /** Represents the status of the repository at the time of the build. */
  public static enum RepoStatus {
    /** The status of the repository was unknown at the time of the build. */
    UNKNOWN,
    /** There was no local modification during the build. */
    MINT,
    /** There were some local modifications during the build. */
    MODIFIED;
  }


    public static final String revisionString()
    {
        return "net.opentsdb BuildData built at revision 756c62f (MINT)";
    }

    public static final String buildString()
    {
        return "Built on 2014/08/18 14:25:03 +0000 by nwhitehe@njwmintx:/home/nwhitehe/projects/opentsdb/mavenview";
    }

    public static String version()
    {
        return "BuildData";
    }

    public static String shortRevision()
    {
        return "756c62f";
    }

    public static String fullRevision()
    {
        return "756c62f68b6a97f3a8588f95f7d689008230f32d";
    }

    public static String date()
    {
        return "2014/08/18 14:25:03 +0000";
    }

    public static long timestamp()
    {
        return 0x53f20cbfL;
    }

    public static RepoStatus repoStatus()
    {
        return repo_status;
    }

    public static String user()
    {
        return "nwhitehe";
    }

    public static String host()
    {
        return "njwmintx";
    }

    public static String repo()
    {
        return "/home/nwhitehe/projects/opentsdb/mavenview";
    }

    private BuildData()
    {
    }

    public static final String version = "BuildData";
    public static final String short_revision = "756c62f";
    public static final String full_revision = "756c62f68b6a97f3a8588f95f7d689008230f32d";
    public static final String date = "2014/08/18 14:25:03 +0000";
    public static final long timestamp = 0x53f20cbfL;
    public static final RepoStatus repo_status;
    public static final String user = "nwhitehe";
    public static final String host = "njwmintx";
    public static final String repo = "/home/nwhitehe/projects/opentsdb/mavenview";

    static 
    {
        repo_status = RepoStatus.MINT;
    }
}

