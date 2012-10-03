%% -------------------------------------------------------------------
%%
%% riakbloom: Tests for riakbloom server
%%
%% Copyright (c) 2012 WhiteNode Software Ltd.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riakbloom_tests).

-export([create_test_data/0,
         remove_test_data/0,
         verify_test_data_exists/0,
         run_test_1/0,
         run_test_2/0,
         run_test_3/0,
         run_test_4/0,
         run_test_5/0,
         run_test_6/0,
         run_test_7/0,
         run_test_8/0
        ]).

-define(MD_INDEX, <<"index">>).
-define(MD_USERMETA, <<"X-Riak-Meta">>).

%% Create test data to be used for all tests
create_test_data() ->
    {ok, C} = riak:local_client(),
    %% Create key1 record
    M1 = dict:from_list([{?MD_INDEX, [{<<"key_bin">>, <<"key1">>}]}, {?MD_USERMETA, [{"X-Riak-Meta-Key", "key1"}]}]),
    RO1 = riak_object:new(<<"testbucket">>, <<"key1">>, <<"key1_data">>, M1),
    C:put(RO1),
    %% Create key2 record
    M2 = dict:from_list([{?MD_INDEX, [{<<"key_bin">>, <<"key2">>}]}, {?MD_USERMETA, [{"X-Riak-Meta-Key", "key2"}]}]),
    RO2 = riak_object:new(<<"testbucket">>, <<"key2">>, <<"key2_data">>, M2),
    C:put(RO2),
    %% Create key3 record
    M3 = dict:from_list([{?MD_INDEX, [{<<"key_bin">>, <<"key3">>}]}, {?MD_USERMETA, [{"X-Riak-Meta-Key", "key3"}]}]),
    RO3 = riak_object:new(<<"testbucket">>, <<"key3">>, <<"key3_data">>, M3),
    C:put(RO3),
    %% Create key4 record
    M4 = dict:from_list([{?MD_INDEX, [{<<"key_bin">>, <<"key4">>}]}, {?MD_USERMETA, [{"X-Riak-Meta-Key", "key4"}]}]),
    RO4 = riak_object:new(<<"testbucket">>, <<"key4">>, <<"key4_data">>, M4),
    C:put(RO4),
    ok.

%% Remove test data used by all tests as well as possible test filters
remove_test_data() ->
    {ok, C} = riak:local_client(),
    C:delete(<<"testbucket">>, <<"key1">>),
    C:delete(<<"testbucket">>, <<"key2">>),
    C:delete(<<"testbucket">>, <<"key3">>),
    C:delete(<<"testbucket">>, <<"key4">>),
    ok.

%%
verify_test_data_exists() ->
    {ok, C} = riak:local_client(),
    case C:get(<<"testbucket">>, <<"key1">>) of
        {ok, _} ->
            ok;
        _ ->
            create_test_data(),
            ok
    end.
    
%% Test creating filter through reduce phase function
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_1() ->
    io:format("TEST: run_test_1 - Test creating filter through reduce phase function.~n"),
    verify_test_data_exists(),
    {ok, C} = riak:local_client(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    C:mapred([{<<"testbucket">>, <<"key1">>}], [{map, {modfun, riakbloom_mapreduce, map_key}, none, false}, {reduce, {modfun, riakbloom_mapreduce, reduce_riakbloom}, "{\"filter_id\":\"testfilter\",\"elements\":\"10\",\"probability\":\"0.001\",\"seed\":\"0\"}", true}]),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key1">>),
    io:format("TEST COMPLETED: run_test_1.~n"),
    ok.

%% Test updating filter through reduce phase function
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_2() ->
    io:format("TEST: run_test_2 - Test updating filter through reduce phase function.~n"),
    verify_test_data_exists(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"aaaa">>], 10, 0.001, 0),
    timer:sleep(2000),
    false = riakbloom:check_key(<<"testfilter">>, <<"key2">>),
    {ok, C} = riak:local_client(),
    C:mapred([{<<"testbucket">>, <<"key2">>}], [{map, {modfun, riakbloom_mapreduce, map_key}, none, false}, {reduce, {modfun, riakbloom_mapreduce, reduce_riakbloom}, "{\"filter_id\":\"testfilter\",\"elements\":\"10\",\"probability\":\"0.001\",\"seed\":\"0\"}", true}]),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key2">>),
    false = riakbloom:check_key(<<"testfilter">>, <<"key1">>),
    true = riakbloom:check_key(<<"testfilter">>, <<"aaaa">>),
    io:format("TEST COMPLETED: run_test_2.~n"),
    ok.

%% Test map phase function based on key
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_3() ->
    io:format("TEST: run_test_3 - Test map phase function based on key.~n"),
    verify_test_data_exists(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key3">>], 10, 0.001, 0),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key3">>),
    {ok, C} = riak:local_client(),
    {ok, [<<"key3">>]} = C:mapred(<<"testbucket">>, [{map, {modfun, riakbloom_mapreduce, map_riakbloom}, "{\"filter_id\":\"testfilter\"}", false}, {map, {modfun, riakbloom_mapreduce, map_key}, none, true}]),
    io:format("TEST COMPLETED: run_test_3.~n"),
    ok.

%% Test map phase function based on key in exclude mode
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_4() ->
    io:format("TEST: run_test_4 - Test map phase function based on key in exclude mode.~n"),
    verify_test_data_exists(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key1">>,<<"key2">>,<<"key3">>], 10, 0.001, 0),
    timer:sleep(2000),
    false = riakbloom:check_key(<<"testfilter">>, <<"key4">>),
    {ok, C} = riak:local_client(),
    {ok, [<<"key4">>]} = C:mapred(<<"testbucket">>, [{map, {modfun, riakbloom_mapreduce, map_riakbloom}, "{\"filter_id\":\"testfilter\",\"operation\":\"exclude\"}", false}, {map, {modfun, riakbloom_mapreduce, map_key}, none, true}]),
    io:format("TEST COMPLETED: run_test_4.~n"),
    ok.

%% Test map phase function based on secondary index
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_5() ->
    io:format("TEST: run_test_5 - Test map phase function based on secondary index.~n"),
    verify_test_data_exists(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key2">>], 10, 0.001, 0),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key2">>),
    {ok, C} = riak:local_client(),
    {ok, [<<"key2">>]} = C:mapred(<<"testbucket">>, [{map, {modfun, riakbloom_mapreduce, map_riakbloom}, "{\"filter_id\":\"testfilter\",\"key\":\"index:key_bin\"}", false}, {map, {modfun, riakbloom_mapreduce, map_key}, none, true}]),
    io:format("TEST COMPLETED: run_test_5.~n"),
    ok.


%% Test map phase function based on user metadata
%% 2 second delay was inserted at key stages to ensure that the post commit hook
%% has been processed (due to slow test environment)
run_test_6() ->
    io:format("TEST: run_test_6 - Test map phase function based on user metadata.~n"),
    verify_test_data_exists(),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key4">>], 10, 0.001, 0),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key4">>),
    {ok, C} = riak:local_client(),
    {ok, [<<"key4">>]} = C:mapred(<<"testbucket">>, [{map, {modfun, riakbloom_mapreduce, map_riakbloom}, "{\"filter_id\":\"testfilter\",\"key\":\"meta:X-Riak-Meta-Key\"}", false}, {map, {modfun, riakbloom_mapreduce, map_key}, none, true}]),
    io:format("TEST COMPLETED: run_test_6.~n"),
    ok.

%% Test filter cache expiry
run_test_7() ->
    io:format("TEST: run_test_7 - Test filter cache expiry.~n"),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key4">>], 10, 0.001, 0),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key4">>),
    1 = riakbloom:locally_cached_filter_count(),
    {ok, Delay} = application:get_env(riakbloom, expiry_duration),
    Dur = 1000 * (Delay + 2),
    timer:sleep(Dur),
    0 = riakbloom:locally_cached_filter_count(),
    io:format("TEST COMPLETED: run_test_7.~n"),
    ok.

%% Test cache refresh on update
run_test_8() ->
    io:format("TEST: run_test_8 - Test cache refresh on update.~n"),
    riakbloom:delete_filter(<<"testfilter">>),
    timer:sleep(2000),
    ok = riakbloom:create_filter(<<"testfilter">>, [<<"key4">>], 10, 0.001, 0),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key4">>),
    false = riakbloom:check_key(<<"testfilter">>, <<"key3">>),
    false = riakbloom:check_key(<<"testfilter">>, <<"key2">>),
    ok = riakbloom:add_keys_to_filter(<<"testfilter">>, [<<"key3">>]),
    timer:sleep(2000),
    true = riakbloom:check_key(<<"testfilter">>, <<"key4">>),
    true = riakbloom:check_key(<<"testfilter">>, <<"key3">>),
    false = riakbloom:check_key(<<"testfilter">>, <<"key2">>),
    io:format("TEST COMPLETED: run_test_8.~n"),
    ok.
