Example of riakbloom usage
==========================

Here is a small example of how to use riakbloom. For simplicity it is based on the test data using in Basho's MapReduce tutorial. Although this does not illustrate a very realistic use of riakbloom, it does show the mechanics.

When using riakbloom to filter our result set, we will be counting the number of records returned. Let us therefore first run a query to count the total number of records we have in the *'goog'* bucket. This can be done using the standard **reduce_count_inputs** function as shown below:

    $> curl -XPOST http://localhost:8091/mapred -H 'Content-Type: application/json' -d '{"inputs":"goog",
        "query":[{"reduce":{"language":"erlang","module":"riak_kv_mapreduce","function":"reduce_count_inputs"}}]}'
    [1438]
    $>

We can then use the example query used in the tutorial to extract a sub-set of the records. We send the resulting keys to the **reduce_count_inputs** function used in the previous example to get the record count. The full mapreduce query is shown below:

    $> curl -XPOST http://localhost:8091/mapred -H 'Content-Type: application/json' -d '{"inputs":"goog",
        "query":[{"map":{"language":"javascript","source":"function(value, keyData, arg) {var data = 
        Riak.mapValuesJson(value)[0]; if(data.Close < data.Open) return [value.key]; else return [];}"}},
        {"reduce":{"language":"erlang","module":"riak_kv_mapreduce","function":"reduce_count_inputs"}}]}'
    [742]
    $>

This filtering operation, implemented in JavaScript, returns 742 records. We will now store these in a bloom filter and then run another mapreduce job that filters out the records based on the bloom filter instead of using the JavaScript function. If there are no fasle positives we should get exactly the same result.


In order to create the bloom filter, we will need to again perform the filtering in order to get they keys. We will then feed these into the **reduce_riakbloom** function in order to generate the filter. How this can be done is shown below:

    $> curl -XPOST http://localhost:8091/mapred -H 'Content-Type: application/json' -d '{"inputs":"goog",
        "query":[{"map":{"language":"javascript","source":"function(value, keyData, arg) {var data = 
        Riak.mapValuesJson(value)[0]; if(data.Close < data.Open) return [value.key]; else return [];}"}},
        {"reduce": {"language":"erlang", "module":"riakbloom_mapreduce", "function":"reduce_riakbloom","arg":"{\"filter_id\":\"goog1\",\"elements\":\"1000\",\"probability\":\"0.001\",\"seed\":\"0\"}"}}]}'
    []
    $>

We specified that the filter should be able to hold 1000 keys while ensuring a false positive probability less than 0.1%. As we will only be inserting 742 keys, the effective false positive probability will be a bit lower than that. Notice that the reduce function does not return anything.

We can now verify that a filter has indeed been created. In our environment we store filters in the default *'riakbloom'* bucket, and we can therefore get the filter information through a HEAD request as illustrated below:

    $> curl -v -X HEAD http://localhost:8091/riak/riakbloom/goog1
    * About to connect() to localhost port 8091 (#0)
    *   Trying 127.0.0.1... connected
    * Connected to localhost (127.0.0.1) port 8091 (#0)
    > HEAD /riak/riakbloom/goog1 HTTP/1.1
    > User-Agent: curl/7.21.4 (universal-apple-darwin11.0) libcurl/7.21.4 OpenSSL/0.9.8r zlib/1.2.5
    > Host: localhost:8091
    > Accept: */*
    > 
    < HTTP/1.1 200 OK
    < X-Riak-Vclock: a85hYGBgzGDKBVIcW1hlPQPybmlmMCWq5bEyLOt4fJIvCwA=
    < X-Riak-Meta-Bloom-Seed: 0
    < X-Riak-Meta-Bloom-Probability: 1.00000000000000002082e-03
    < X-Riak-Meta-Bloom-Elements: 1000
    < Vary: Accept-Encoding
    < Server: MochiWeb/1.1 WebMachine/1.9.2 (someone had painted it blue)
    < Link: ; rel="up"
    < Last-Modified: Fri, 05 Oct 2012 16:36:54 GMT
    < ETag: "2PysgJPOfHM1PxsQGkiwHV"
    < Date: Fri, 05 Oct 2012 17:30:45 GMT
    < Content-Type: application/octet-stream
    < Content-Length: 1866
    $>

We can see that the serialized form of the filter is 1866 bytes and some metadata headers have been added that describe the parameters used to create the filter. These are for information only.

If we now were to replace the JavaScript map phase function we previously used with a function comparing keys to the filter, **map_riakbloom**, we would expect to still get the same (although it could differ slightly due to the false positive probability) number of records returned.

    $> curl -XPOST http://localhost:8091/mapred -H 'Content-Type: application/json' -d '{"inputs":"goog",
        "query":[{"map": {"language":"erlang", "module":"riakbloom_mapreduce",
        "function":"map_riakbloom","arg":"{\"filter_id\":\"goog1\"}"}},
        {"reduce":{"language":"erlang","module":"riak_kv_mapreduce","function":"reduce_count_inputs"}}]}'
    [742]
    $>

In this example we did get exactly the same number of records returned.

When using the **map_riakbloom** function, it is also possible to exclude matches instead of including them. An example of this is shown below:

    $> curl -XPOST http://localhost:8091/mapred -H 'Content-Type: application/json' -d '{"inputs":"goog","query":
        [{"map": {"language":"erlang", "module":"riakbloom_mapreduce", "function":"map_riakbloom",
        "arg":"{\"filter_id\":\"goog1\",\"exclude\":\"true\"}"}},
        {"reduce":{"language":"erlang","module":"riak_kv_mapreduce","function":"reduce_count_inputs"}}]}'
    [696]
    $>

As expected, this returned 696 records.

