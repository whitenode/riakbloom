%% -------------------------------------------------------------------
%%
%% riakbloom_app: Application file for riakbloom server 
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

-module(riakbloom_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(RIAKBLOOM_ETS, riakbloom_cached_filters).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    process_configuration(),
    ets:new(?RIAKBLOOM_ETS, [named_table, public]),
    ok = set_bucket_properties(),
    riakbloom_sup:start_link().

stop(_State) ->
    ok.

process_configuration() ->
    case application:get_env(riakbloom, bucket_name) of
        undefined ->
            application:set_env(riakbloom, bucket_name, <<"riakbloom">>);
        {ok, Name} when is_list(Name) ->
            application:set_env(riakbloom, bucket_name, list_to_binary(Name));
        {ok, Name} when is_binary(Name) ->
            application:set_env(riakbloom, bucket_name, Name);
        _ ->
            application:set_env(riakbloom, bucket_name, <<"riakbloom">>)
    end,
    case application:get_env(riakbloom, expiry_duration) of
        undefined ->
            application:set_env(riakbloom, expiry_duration, 10);
        {ok, Dur} when is_integer(Dur) andalso (Dur > 0) ->
            application:set_env(riakbloom, expiry_duration, Dur);
        _ ->
            application:set_env(riakbloom, expiry_duration, 10)
    end,
    ok.

set_bucket_properties() ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, C} = riak:local_client(),
    PHook = {struct, [{<<"mod">>, <<"riakbloom_hooks">>}, {<<"fun">>,<<"post_commit_hook">>}]},
    C:set_bucket(Bucket, [{allow_mult, true}, {postcommit, [PHook]}]).
