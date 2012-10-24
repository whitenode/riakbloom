%% -------------------------------------------------------------------
%%
%% riakbloom_mapreduce: MapReduce functions for use with riakbloom server
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

-module(riakbloom_mapreduce).

%% Application callbacks
-export([map_key/3,
         map_riakbloom/3,
         reduce_riakbloom/2]).

-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX, <<"index">>).

%% @spec map_key(riak_object:riak_object(), term(), term()) ->
%%                   [Key :: binary()]
%% @doc map phase function returning object key in a readable format
map_key({error, notfound}, _, _) ->
    [];
map_key(RiakObject, Props, Arg) when is_list(Arg) ->
    map_key(RiakObject, Props, list_to_binary(Arg));
map_key(RiakObject, Props, Arg) when is_atom(Arg) ->
    map_key(RiakObject, Props, <<"">>);
map_key(RiakObject, _, Arg) when is_binary(Arg) ->
    Bucket = riak_object:bucket(RiakObject),
    Key = riak_object:key(RiakObject),
    case Arg of
        Bucket ->
            [Key];
        <<"">> ->
            [Key];
        _ ->
            []
    end;
map_key(_, _, _) ->
    [].   

%% @spec map_riakbloom(riak_object:riak_object(), term(), term()) ->
%%                   [{{Bucket :: binary(), Key :: binary()}, Props :: term()}]
%% @doc map phase function for filtering based on bloom filter using the riakbloom server
%% Passed object through by defult in case any error occurs.
map_riakbloom({error, notfound}, _, _) ->
    [];
map_riakbloom(RiakObject, Props, JsonArg) ->
    Bucket = riak_object:bucket(RiakObject),
    Key = riak_object:key(RiakObject),
    MetaDataList = riak_object:get_metadatas(RiakObject),
    {struct, Args} = mochijson2:decode(JsonArg),
    LookupKey = case proplists:get_value(<<"key">>, Args) of
        <<"meta:", Val/binary>> ->
            case get_metadata_value(MetaDataList, meta, Val) of
                undefined ->
                    error;
                Result ->
                    Result
            end;
        <<"index:", Val/binary>> ->
            case get_metadata_value(MetaDataList, index, Val) of
                undefined ->
                    error;
                Result ->
                    Result
            end;
        _ ->
            Key
    end,
    Include = case {LookupKey,
          proplists:get_value(<<"bucket">>, Args),
          proplists:get_value(<<"filter_id">>, Args),
          proplists:get_value(<<"exclude">>, Args)} of
        {error, _, _, _} ->
            true;
        {_, _, undefined, _} ->
            true;
        {_, undefined, FilterID, true} ->
            check_filter(FilterID, LookupKey, false);
        {_, Bucket, FilterID, true} ->
            check_filter(FilterID, LookupKey, false);
        {_, undefined, FilterID, <<"true">>} ->
            check_filter(FilterID, LookupKey, false);
        {_, Bucket, FilterID, <<"true">>} ->
            check_filter(FilterID, LookupKey, false);
        {_, undefined, FilterID, "true"} ->   
            check_filter(FilterID, LookupKey, false);
        {_, Bucket, FilterID, "true"} ->   
            check_filter(FilterID, LookupKey, false);
        {_, undefined, FilterID, _} ->
            check_filter(FilterID, LookupKey, true);    
        {_, Bucket, FilterID, _} ->
            check_filter(FilterID, LookupKey, true);
        _ ->
            true
    end,
    case Include of
        true ->
            [{{Bucket, Key}, Props}];
        _ ->
            []
    end.

%% @spec reduce_riakbloom([term()], term()) -> [] | [term()]
%% @doc reduce phase function for adding keys to a bloom filter using riakbloom server
%% Removes keys added to the filter from the result set, Returns full set of keys
%% in case there is an error.
reduce_riakbloom(List, JsonArg) ->
    {struct, Args} = mochijson2:decode(JsonArg),
    add_keylist_to_filter(proplists:get_value(<<"filter_id">>, Args),
                          proplists:get_value(<<"elements">>, Args),
                          proplists:get_value(<<"probability">>, Args),
                          proplists:get_value(<<"seed">>, Args),
                          List).

%% hidden
add_keylist_to_filter(undefined, _, _, _, _) ->
    [];
add_keylist_to_filter(_, undefined, _, _, _) ->
    [];
add_keylist_to_filter(_, _, undefined, _, _) ->
    [];
add_keylist_to_filter(_, _, _, undefined, _) ->
    [];
add_keylist_to_filter(FID, E, P, S, KeyList) when is_binary(E) ->
    add_keylist_to_filter(FID, binary_to_list(E), P, S, KeyList);
add_keylist_to_filter(FID, E, P, S, KeyList) when is_list(E) ->
    try list_to_integer(E) of
        EInt -> add_keylist_to_filter(FID, EInt, P, S, KeyList)
    catch
        _:_ ->
            []
    end;
add_keylist_to_filter(FID, E, P, S, KeyList) when is_binary(P) ->
    add_keylist_to_filter(FID, E, binary_to_list(P), S, KeyList);
add_keylist_to_filter(FID, E, P, S, KeyList) when is_list(P) ->
    try list_to_float(P) of
        PFloat -> add_keylist_to_filter(FID, E, PFloat, S, KeyList)
    catch
        _:_ ->
            []
    end;
add_keylist_to_filter(FID, E, P, S, KeyList) when is_binary(S) ->
    add_keylist_to_filter(FID, E, P, binary_to_list(S), KeyList);
add_keylist_to_filter(FID, E, P, S, KeyList) when is_list(S) ->
    try list_to_integer(S) of
        SInt -> add_keylist_to_filter(FID, E, P, SInt, KeyList)
    catch
        _:_ ->
            []
    end;
add_keylist_to_filter(FID, E, P, S, KeyList) when is_integer(E)
                                          andalso is_float(P)
                                          andalso is_integer(S) ->
    riakbloom:add_keys_to_filter(FID, KeyList, E, P, S),
    [].

%% hidden
check_filter(FilterID, Key, IncludeMode) when is_list(FilterID) ->
    check_filter(list_to_binary(FilterID), Key, IncludeMode);
check_filter(FilterID, Key, IncludeMode) when is_list(Key)->
    check_filter(FilterID, list_to_binary(Key), IncludeMode);
check_filter(FilterID, Key, IncludeMode) ->
    case {riakbloom:check_key(FilterID, Key), IncludeMode} of
        {true, true} ->
            true;
        {true, false} ->
            false;
        {false, true} ->
            false;
        {false, false} ->
            true;
        {error, true} ->
            true;
        {error, false} -> 
            true
    end.

%% hidden
get_metadata_value([], _Type, _MetaName) ->
    undefined;
get_metadata_value([MetaData | Rest], Type, MetaName) ->
    case get_metadata_value(MetaData, Type, MetaName) of
        undefined ->
            get_metadata_value(Rest, Type, MetaName);
        Value when is_list(Value) ->
            list_to_binary(Value);
        Value when is_binary(Value) ->
            Value
    end;
get_metadata_value(MetaData, Type, MetaName) ->
    case Type of
        meta ->
            MetaKey = ?MD_USERMETA,
            MN = binary_to_list(MetaName);
        index ->
            MetaKey = ?MD_INDEX,
            MN = MetaName
    end,
    case dict:find(MetaKey, MetaData) of
        {ok, Value} ->
            case [V || {K, V} <- Value, K == MN] of
                [] -> undefined;
                [V] when is_list(V) -> list_to_binary(V);
                [V] -> V
            end;
        error -> undefined
    end.
