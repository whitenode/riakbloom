Overview
========

The **riakbloom** server is a collection of components that allows Bloom filters to be created and accessed through mapreduce jobs. As filters are stored in a designated bucket, it is possible to create and serialize filters outside Riak and then upload them for later use in mapreduce jobs. In order to improve the efficiency of map phase mapreduce filtering, the solution also consists of a simple caching server process, and it must therefore be deployed onto every Riak instance.

An example of how riakbloom is used can be found in the EXAMPLE.markdown file.

Installation
============

The riakbloom server must be deployed together with Riak (on the same VM) in order for the mapreduce functions, hooks and caching process that form part of the solution to be available. In order to simplify this, I have created a riak repository where I have added the required dependency on the riakbloom server. Using this, a version of Riak which has the riakbloom components included can be compiled from source.

The solution can be compiled as follows:

    $> git clone git@github.com:whitenode/riak.git
    $> cd riak
    $> git checkout riakbloom 
    $> make all

The riakbloom server will be initiated when riak starts up.

The dependencies were added in 2 places. A line a shown below was added to *rebar.config* for **riakbloom** to ensure the application is included in the build.

    {riakbloom, ".*", {git, "git://github.com/whitenode/riakbloom", {branch, "master"}}}

The other dependency added was to the *rel/teltool.config* file. In this **riakbloom** was added to the list of applications and the following application config was added:

    {app, riakbloom, [{incl_cond, include}]}

Configuration
=============

riakbloom takes 2 optional configuration parameters that can be specified in the Riak app.config file under a *riakbloom* section - *bucket* and *expiry_duration*.

**bucket** specifies the name of the bucket in which all riakbloom filters are to be stored. This defaults to *"riakbloom"* if the parameter is not specified. The value of this parameter must be the same across the entire cluster.

The **expiry_duration** determines how long filters must be cached in the local caching process. Once the filter have been cached for this amount of time the process may remove the locally cached copy. This parameter defaults to *10* (seconds).

Components
==========

This section describes the various components that collectively make up riakbloom. Examples showing how these can be used are available further down.

MapReduce function map_key
--------------------------

The **map_key** function takes one optional argument, the name of the bucket to print keys from. If specified, records belonging to a bucket different from the one specified will be filtered out. If the parameter is not specified, the keys of all records that are passed to the function will be output.

This function can be used to feed record keys into the **reduce_riakbloom** function.

MapReduce function map_riakbloom
--------------------------------

The **map_riakbloom** function allows filtering to be performed based on an existing Bloom filter during the map phase.

It can be configured to either filter on the record key (default) or a secondary index or user metadata value.

By default any record matching the filter will be included in the result set, but it is also possible to configure the function to instead *exclude* based on filter match. As Bloom filters always have a false positive probability (meaning keys may match even though they were not added to the filter), using the map function in *exclude* mode means that the false positive probability is changed into a fasle negative instead, and there is a probability that records may be excluded from the result incorrectly.

If an error occurs, e.g. the index or meta field to filter on can not be found on the record, the record will not be filtered and always be part of the result set.

This function takes an argument that has to be a correctly formatted JSON document containing the following fields:

**filter_id** - Name of the filter to use. If the filter does not exist, all records will be passed through as part of the result set.

**key** - Optional parameter indicating the field to get the key to be compared to the filter. Defaults to **"key"**, which uses the record key for the filter lookup.

It can also be specified as **"index:<index_name>"** or **"meta:<user metadata field name>"**, in order to based the lookup on the value stored in a secondary index or user metadata field.

**exclude** - Optional parameter indicating whether to include or exclude records that match the filter. It defaults to *"false"*.

Below are a few configuration examples:

**Minimal configuration just specifying the filter to use** 

    "{
        "filter_id":"filter1"
    }"

**Configuration for filtering based on secondary index idx_bin in exclude mode** 

    "{
        "filter_id":"filter1",
        "key":"index:idx_bin",
        "exclude":"true"
    }"

**Configuration for filtering based on user metadata field Name** 

    "{
        "filter_id":"filter1",
        "key":"meta:X-Riak-Meta-Name"
    }"

MapReduce function reduce_riakbloom
-----------------------------------

The **reduce_riakbloom** reduce phase function expects a list of keys, and will add these to the specified filter. If the filter does not already exist, it will instead create a new filter according to the supplied specification and add the list of keys to this.

The argument string must be a correctly formatted JSON document and must contain the following 4 fields:

**filter_id** - Name of the filter to update/create.

The remaining 3 parameters specify how the filter is to be created if this is necessary. See [ebloom](https://github.com/basho/ebloom) for further details.

**elements** - The number of keys the filter is expected to hold. This must be a positive integer.

**probability** - The expected false positive probability rate when the filter holds the expected number of elements.This must be expressed as a float string in the interval 0 < probability < 1.

**seed** - Random seed expressed as a positive integer. 

Below is an example of a valid configuration. 

    "{
        "filter_id":"filter1",
        "elements":"10000",
        "probability":"0.001",
        "seed":"0"
    }"

riakbloom caching mechanism
---------------------------

When filtering in the map phase of a mapreduce job, the map phase function is called once for every record. In order to not have to load and deserialize the appropriate filter once per record, a local caching server process has been introduced. This process will store the retrieved and deserialized filter in a local ETS table the first time it is accessed so that it can be reused by other map phase functions. Once the filter has been cached for *expiry_duration* seconds it may be removed, which will cause a reload if a mapreduce job using it is still running.

If multiple siblings are identified, the cacching process will merge these using the *ebloom:union* operation before caching the filter.

Post-commit hook
----------------
 
The post-commit hook **post_commit_hook** in the **riakbloom_hooks** module is associated with the bucket used to store riakbloom filters when the server is started. Once a filter is created, updated or deleted it will send an asynchronous message to all local caching processes to remove the specific filter from the local cache.

As filters are stored in a normal Riak bucket, they can be deleted the same way as any other data, e.g. through the HTTP interface using *curl*.

Creating Bloom filters based on external data
=============================================

Filters can be created externally based on the [ebloom](https://github.com/basho/ebloom) module and then uploaded to the bucket in serialized form. That is all that is required in order to make them available to be used in Riak mapreduce jobs.

To simplify this process, I have created the [riakbloomutil](https://github.com/whitenode/riakbloomutil) utility. This makes it possible to create and update filters based on keys in external files.

