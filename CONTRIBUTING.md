# Contribution Guidelines

There are a number of talented developers creating tools for OpenTSDB or contributing code directly to the project.
If you are interested in helping, by adding new features, fixing bugs, adding tools or simply updating documentation, please read the guidelines below. Then sign the contributors agreement and send us a pull request!

## Guidelines


- Please file [issues on GitHub](https://github.com/OpenTSDB/opentsdb/issues) after checking to see if anyone has posted a bug already. Make sure your bug reports contain enough details so they can be easily understood by others and quickly fixed.
- Read the Development page for tips
- The best way to contribute code is to fork the main repo and [send a pull request](https://help.github.com/articles/using-pull-requests) on GitHub.
    - Bug fixes should be done in the `master` branch
    - New features or major changes should be done in the `next` branch
- Alternatively, you can send a plain-text patch to the [mailing list](https://groups.google.com/forum/#!forum/opentsdb).
- Before your code changes can be included, please file the [Contribution License Agreement](https://docs.google.com/spreadsheet/embeddedform?formkey=dFNiOFROLXJBbFBmMkQtb1hNMWhUUnc6MQ).
- Unlike, say, the Apache Software Foundation, we do not require every single code change to be attached to an issue. Feel free to send as many small fixes as you want.
- Please break down your changes into as many small commits as possible.
- Please respect the coding style of the code you're changing.
    - Indent code with 2 spaces, no tabs
    - Keep code to 80 columns
    - Curly brace on the same line as if, for, while, etc
    - Variables need descriptive names `like_this` (instead of the typical Java style of `likeThis`)
    - Methods named `likeThis()` starting with lower case letters
    - Classes named `LikeThis`, starting with upper case letters
    - Use the `final` keyword as much as you can, particularly in method parameters and returns statements
    - Avoid checked exceptions as much as possible
    - Always provide the most restrictive visibility to classes and members
    - Javadoc all of your classes and methods. Some folks make use the Java API directly and we'll build docs for the site, so the more the merrier
    - Don't add dependencies to the core OpenTSDB library unless absolutely necessary
    - Add unit tests for any classes/methods you create and verify that your change doesn't break existing unit tests. We know UTs aren't fun, but they are useful

## Git Repository

OpenTSDB is maintained in [GitHub](https://github.com/OpenTSDB/opentsdb/). There are a limited number of branches and the purpose of each branch is as follows:

- `maintenance` - This was the previously released version of OpenTSDB, usually the last minor version. E.g. if 2.3.0 or 2.3.1 is the current release, `maintenance` will have the last 2.2.x version. This branch
should rarely have PRs pointed at it. Rather patches against `master` that would apply to previous releases can be cherry-picked.
- `master` - The current release version of OpenTSDB. Only pull requests with bug fixes should be given against `master`. When enough PRs have been merged, we'll cut another PATCH version, e.g. 2.2.0 to 2.2.1. Patches with new features or behavior modifications should point to `next`. Patches against `master` should be cherry-picked to the downstream branches.
- `next` - This is the next minor release version of OpenTSDB and contains code in development or, when the version is marked as RC, then release candidate code. If the version is marked as a SNAPSHOT then new features can be applied to `next`. Once it moves to RC, then new features should be issued against the `put` branch. Otherwise only bug fixes should be given for RC code.
- `put` - When the next branch is in an RC state, new features should be applied against `put` which will be the next minor version.
- `X.0` - The next major version of OpenTSDB that may include breaking API changes. When the code is in a fairly stable state, it will be promoted up to `next` as an RC, then `master` for an official `release`.

Any other branches are likely to be pruned at some point as they may contain stale code or temporary hacks.

## Details

- [General Development](http://opentsdb.net/docs/build/html/development/development.html)
- [Plugins](http://opentsdb.net/docs/build/html/development/plugins.html)
- [HTTP API](http://opentsdb.net/docs/build/html/development/http_api.html)

Thank you for your contributions!
