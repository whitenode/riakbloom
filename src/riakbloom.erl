%% -------------------------------------------------------------------
%%
%% riakbloom: Interface definition for riakbloom server application
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

-module(riakbloom).

-export([create_filter/5,
         add_keys_to_filter/2,
         add_keys_to_filter/5,
         clear_filter/1,
         delete_filter/1,
         check_key/2,
         force_cache_refresh/1,
         locally_cached_filter_count/0,
         locally_cached_filter_list/0
        ]).

-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_BLOOM_ELEMENTS, "X-Riak-Meta-Bloom-Elements").
-define(MD_BLOOM_PROBABILITY, "X-Riak-Meta-Bloom-Probability").
-define(MD_BLOOM_SEED, "X-Riak-Meta-Bloom-Seed").

%% @spec create_filter(binary(), [binary()], integer(), float(), integer()) -> ok | {error, term()}
%% @doc Creates a new filter and replaces any existing filter with the same ID
create_filter(FilterID, KeyList, PredictedElementCount, FalsePositiveProbability, RandomSeed)
        when is_binary(FilterID)
        andalso is_integer(PredictedElementCount)
        andalso is_float(FalsePositiveProbability)
        andalso is_integer(RandomSeed) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    case C:get(Bucket, FilterID) of
        {ok, RO} ->
            create_and_upload_filter(C, Bucket, RO, PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList); 
        {error, _} ->
            create_and_upload_filter(C, Bucket, FilterID, PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList)
    end.

%% @spec add_keys_to_filter(binary(), [binary()], integer(), float(), integer()) -> ok | {error, term()}
%% @doc Adds listed keys to the filter with the specified ID if it exists.
%%      Otherwise it creates a new filter with the specified ID
add_keys_to_filter(FilterID, KeyList) when is_binary(FilterID) andalso is_list(KeyList) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    case C:get(Bucket, FilterID) of
        {ok, RO} ->
            {ok, Filter} = deserialize_and_reconcile_filters(RO),
            add_keys(Filter, KeyList),
            SerializedFilter = ebloom:serialize(Filter),
            RO2 = riak_object:update_value(RO, SerializedFilter),
            C:put(RO2);
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec add_keys_to_filter(binary(), [binary()]) -> ok | {error, term()}
%% @doc Adds listed keys to the filter with the specified ID. If no filter with
%%      this ID exists, and error is returned
add_keys_to_filter(FilterID, KeyList, PredictedElementCount, FalsePositiveProbability, RandomSeed) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    case C:get(Bucket, FilterID) of
        {ok, RO} ->
            {ok, Filter} = deserialize_and_reconcile_filters(RO),
            add_keys(Filter, KeyList),
            SerializedFilter = ebloom:serialize(Filter),
            RO2 = riak_object:update_value(RO, SerializedFilter),
            C:put(RO2);
        {error, _} ->
            create_filter(FilterID, KeyList, PredictedElementCount, FalsePositiveProbability, RandomSeed)
    end.

%% @spec clear_filter(binary()) -> ok | {error, term()}
%% @doc Clears the filter with the specified ID. If no filter with
%%      this ID exists, and error is returned
clear_filter(FilterID) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    case C:get(Bucket, FilterID) of
        {ok, RO} ->
            [F | _] = riak_object:get_values(RO),
            {ok, Filter} = ebloom:deserialize(F),
            ebloom:clear(Filter),
            SerializedFilter = ebloom:serialize(Filter),
            RO2 = riak_object:update_value(RO, SerializedFilter),
            C:put(RO2);
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec delete_filter(binary()) -> ok
%% @doc Deletes the filter with the specified ID.
delete_filter(FilterID) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    C:delete(Bucket, FilterID),
    ok.

%% @spec locally_cached_filter_count() -> integer()
%% @doc Returns the number of filters currently cached at the local node
locally_cached_filter_count() ->
    riakbloom_cache:number_of_cached_filters().

%% @spec locally_cached_filter_list() -> [binary()]
%% @doc Returns a list of filters currently cached at the local node
locally_cached_filter_list() ->
    riakbloom_cache:list_of_cached_filters().

%% @spec check_key(binary(), binary()) -> true | false | error
%% @doc Checks the specified key against the specified filter.
%%      Returns error of filter does not exist.
check_key(FilterID, Key) ->
    riakbloom_cache:check_key(FilterID, Key).

%% @spec check_key(binary()) -> ok
%% @doc Forces the caches on all nodes to refresh the specified filter. 
force_cache_refresh(FilterID) ->
    riakbloom_cache:remove_cached_filter(FilterID),
    ok.
    
%% Internal functions
%% hidden
add_keys(Filter, KeyList) ->
    lists:foreach(fun(K) ->
                      add_key(Filter, K)
                  end, KeyList),
    ok.

add_key(Filter, Key) when is_list(Key) ->
    add_key(Filter, list_to_binary(Key));
add_key(Filter, Key) when is_binary(Key) ->
    ebloom:insert(Filter, Key).

%% hidden
deserialize_and_reconcile_filters(RO) ->
    [V | VL] = riak_object:get_values(RO),
    {ok, Filter} = ebloom:deserialize(V),
    lists:foreach(fun(F) ->
                      {ok, DF} = ebloom:deserialize(F),
                      ebloom:union(Filter, DF)
                  end, VL),
    {ok, Filter}.

%% hidden
create_and_upload_filter(Connection, Bucket, FilterID, PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList) when is_list(FilterID) ->
    create_and_upload_filter(Connection, Bucket, list_to_binary(FilterID), PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList);
create_and_upload_filter(Connection, Bucket, FilterID, PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList) when is_binary(FilterID) ->
    SF = create_and_serialize_new_filter(PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList),
    Dict = dict:new(),
    MetaData = set_filter_metadata(Dict, PredictedElementCount, FalsePositiveProbability, RandomSeed),
    RO = riak_object:new(Bucket, FilterID, SF, MetaData),
    Connection:put(RO);
create_and_upload_filter(Connection, _Bucket, RiakObject, PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList) ->
    SF = create_and_serialize_new_filter(PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList),
    [M | _] = riak_object:get_metadatas(RiakObject),
    MetaData = set_filter_metadata(M, PredictedElementCount, FalsePositiveProbability, RandomSeed),
    RO2 = riak_object:update_value(RiakObject, SF),
    RO3 = riak_object:update_metadata(RO2, MetaData),
    Connection:put(RO3).

%% hidden
create_and_serialize_new_filter(PredictedElementCount, FalsePositiveProbability, RandomSeed, KeyList) ->
    {ok, Filter} = ebloom:new(PredictedElementCount, FalsePositiveProbability, RandomSeed),
    add_keys(Filter, KeyList),
    ebloom:serialize(Filter).

%% hidden
set_filter_metadata(MetaData, PredictedElementCount, FalsePositiveProbability, RandomSeed) ->
    FilterMD = [{?MD_BLOOM_ELEMENTS, integer_to_list(PredictedElementCount)},
                {?MD_BLOOM_PROBABILITY, float_to_list(FalsePositiveProbability)},
                {?MD_BLOOM_SEED, integer_to_list(RandomSeed)}],
    dict:store(?MD_USERMETA, FilterMD, MetaData).
