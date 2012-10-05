%% -------------------------------------------------------------------
%%
%% riakbloom_cache: riakbloom cache management server
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

-module(riakbloom_cache).

-behaviour(gen_server).

%% Application callbacks
-export([start_link/0,
         remove_cached_filter/1,
         load_filter/1,
         check_key/2,
         number_of_cached_filters/0,
         list_of_cached_filters/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {bucket, expiry_duration}).

-define(SERVER, ?MODULE).
-define(RIAKBLOOM_ETS, riakbloom_cached_filters).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

remove_cached_filter(FilterID) when is_list(FilterID) ->
    remove_cached_filter(list_to_binary(FilterID));
remove_cached_filter(FilterID) when is_binary(FilterID) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    lists:foreach(fun(P) ->
                      gen_server:cast(P, {remove_cached_filter, FilterID})
                  end, pg2:get_members(Bucket)),
    ok.

check_key(FilterID, Key) when is_list(FilterID) ->
    check_key(list_to_binary(FilterID), Key);
check_key(FilterID, Key) when is_list(Key) ->
    check_key(FilterID, list_to_binary(Key));
check_key(FilterID, Key) when is_binary(FilterID) andalso is_binary(Key) ->
    case ets:lookup(?RIAKBLOOM_ETS, FilterID) of
        [] ->
            case load_filter(FilterID) of
                {error, notfound} ->
                    error;
                Filter ->
                    ebloom:contains(Filter, Key)
            end;
        [{FilterID, {Filter, _}}] ->
            ebloom:contains(Filter, Key)
    end.

load_filter(FilterID) ->
    gen_server:call(?SERVER, {load_filter, FilterID}).

number_of_cached_filters() ->
    TabProps = ets:info(?RIAKBLOOM_ETS),
    proplists:get_value(size, TabProps).

list_of_cached_filters() ->
    [F || {F, {_, _}} <- ets:tab2list(?RIAKBLOOM_ETS)].

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------    
init([]) ->
    {ok, Bucket} = application:get_env(riakbloom, bucket_name),
    {ok, ExpDur} = application:get_env(riakbloom, expiry_duration),
    erlang:send_after(1000, self(), check_expiry),
    pg2:create(Bucket),
    pg2:join(Bucket, self()),
    {ok, #state{bucket = Bucket, expiry_duration = ExpDur}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call({load_filter, FilterID}, _From, #state{bucket = Bucket} = State) ->
    case ets:lookup(?RIAKBLOOM_ETS, FilterID) of
        [] ->
            {ok, C} = riak:local_client(),
            case C:get(Bucket, FilterID) of
                {ok, RO} ->
                    case deserialize_and_reconcile_filters(RO) of
                        none ->
                            {reply, {error, notfound}, State};
                        {ok, Filter} ->
                            TS = get_timestamp(),
                            ets:insert(?RIAKBLOOM_ETS, {FilterID, {Filter, TS}}),
                            {reply, Filter, State}
                    end;
                {error, _} ->
                    {reply, {error, notfound}, State}
            end;
        [{FilterID, {Filter, _}}] ->
            {reply, Filter, State}
    end.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({remove_cached_filter, FilterID}, State) ->
    ets:delete(?RIAKBLOOM_ETS, FilterID),
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(check_expiry, #state{expiry_duration = ED} = State) ->
    CutOff = (get_timestamp() - ED),
    FilterList = [FID || {FID, {_, TS}} <- ets:tab2list(?RIAKBLOOM_ETS), TS < CutOff],
    lists:foreach(fun(F) ->
                      ets:delete(?RIAKBLOOM_ETS, F)
                  end, FilterList),
    erlang:send_after(1000, self(), check_expiry),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% hidden
get_timestamp() ->
    calendar:datetime_to_gregorian_seconds(calendar:local_time()).
    
%% hidden
deserialize_and_reconcile_filters(RO) ->
    case [D || D <- riak_object:get_values(RO), D =/= <<>>] of
        [] ->
            none;
        [V | VL] ->
            {ok, Filter} = ebloom:deserialize(V),
            lists:foreach(fun(F) ->
                              {ok, DF} = ebloom:deserialize(F),
                              ebloom:union(Filter, DF)
                          end, VL),
            {ok, Filter}
    end.



