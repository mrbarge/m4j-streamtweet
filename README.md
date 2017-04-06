**m4j-streamtweet**
===================

A plugin for Max (cycling74.com) which provides a real-time stream of Twitter status 
updates based upon the provided search filters.
 
Currently supported search filters:
 * Location-based bounding boxes
 
In progress:
 * Tags
 * Usernames

Build
-----

m4j-streamtweet requires the installation of Max and its associated mxj libraries. The location of 
your Max installation can be supplied as the property `max.installdir`.

m4j-streamtweet provides a Maven build file and can be compiled and packaged using:

`mvn -Dmax.installdir=/path/to/Max/basedir package`

Installation
------------

1. Update Max's `max.java.config.txt` configuration (usually located in 
`$MAX_INSTALLDIR/resources/package/max-mxj/java-classes`) to set the following custom paths:
 * `max.dynamic.class.dir`
 * `max.dynamic.jar.dir`

2. Place the compiled `StreamTweet*class` files in the `max.dynamic.class.dir` location.
3. Place the dependent `lib/` jars in the `max.dynamic.jar.dir` location.

Once installed, the plugin can be used by creating an object of name `mxj StreamTweet`

Example usage
-------------

The included `StreamTweet.maxpat` patch illustrates how to use the plugin.

Note that OAuth credentials are not provided and must be registered via https://dev.twitter.com, and then set as the plugin arguments.
Dummy values have been inserted into the patch in their place.








