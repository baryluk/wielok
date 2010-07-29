%
% wielok - Simple locking system
%
% TODO: detect process crashes, and release their locks.
%         for not yet given locks, just remove then from queues,
%         for readers, remove them
%         for writers, log error, and crash whole locking manager for safety.
%
% TODO: rel, rel_excl, cancel can be turned into gen_server:cast to improve latency.
%
% TODO: rel, rel_excl calls should check if release is done be valid process Pid. just for sure.
%         Unfortunetly for rel it will increase memory consumption, and we will need to use dict or rb_trees
%         to store information about processes which helds locks. But this can be also handy
%         to prevent same process obtaining two locks.
%
% What more? We can define for how long we want a lock (for example we want to use disk, and know
% how much time we will use it, in the same scale to the other holders. We can also have deadlines
% (that we need lock after time X, and before time Y, and for time Z). We then can implement
% some kind of scheduler, and cancelation system if lock holder do not released it.
% It will be probably only usefull for readers.
%
% We can also limit number of concurrant readers (to not overload system to much by resource usage).
%
% We can also for example, if there are more than N writers in the queue, and
% there are still some readers, and no reader release lock in last M miliseconds,
% then start killing them...
%
% Copyright: Witold Baryluk, 2010
% License: BSD
%

-module(wielok).

-behaviour(gen_server).

-export([start/0, start/1, start_global/1, stop/0, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).
-export([acq/2, acq_excl/2, cancel_wait/2]).
-export([acq/1, acq_excl/1, rel/1, rel_excl/1, cancel/1, cancel_wait/1, uncancel/1, stat/1]).
-export([acq/0, acq_excl/0, rel/0, rel_excl/0, cancel/0, cancel_wait/0, uncancel/0, stat/0]).
-export([read/2, write/2]).

% HELPER MACROS

-record(state, {waiting_readers=[], waiting_readers_len=0, waiting_writers=queue:new(), who=undefined, canceled=false, waiting_cancelers=[]}).
-define(W_EMPTY, {[],[]}).
-define(TIMEOUT, (60*1000)).
                    % timeout for blocking acq, acq_excl, cancel_wait
                    % for rest timeout is default 5 seconds in case of overload.
-define(NAME0, ?MODULE).
-define(NAME, {global, ?NAME0}).

%-define(debug, true).   % enable debuging, and additional internal checks
-define(debug, false).

% INIT

start() ->
	start(?NAME0).

start(Name) when is_atom(Name) ->
	gen_server:start({local, Name}, ?MODULE, init, []).

start_global(Name) when is_atom(Name) ->
	gen_server:start({global, Name}, ?MODULE, init, []).

init(_Args) ->
	{ok, #state{}}.

terminate(Reason, State) ->
	{reply, ok, _State2} = c(cancel, self(), State),
	% link to active readers/writer and fake own death
	% for readers we can just ignore them (just make calling wielok:rel() not to crash due to the missing gen_server)
	% for writers, notifiy them somethow
	Reason.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

stop() ->
	stop(?NAME).

stop(Name) ->
	gen_server:cast(Name, stop).

% testing ( can be removed later )

check_invariants(_OldS, NewS) ->
	check_invariants(NewS).

check_invariants(_S = #state{waiting_readers=R,waiting_writers=W,waiting_cancelers=C,canceled=CC,who=Who,waiting_readers_len=RL})
                           when is_list(R), is_list(C), is_boolean(CC) ->
	All = {
	% if canceled, then waiting_readers and waiting_writers must be empty
	if
		CC -> case {W,R} of {?W_EMPTY,[]} -> ok; _ -> error end;
		true -> ok
	end,
	% if who=undefined, then both waiting_readers, waiting_writers, waiting_cancelers must be empty
	case Who of
		undefined -> case {W,R,C} of {?W_EMPTY,[],[]} -> ok; _ -> error end;
		_ -> ok
	end,
	% check also transitions:
	% once canceled=true, we cannot make it false
	ok,
	% if canceled=true, then who cannot change from undefined to anything
	ok,
	% sanity of who
	case Who of
		{readers, N} when is_integer(N), N >= 1 -> ok;
		{readers, _} -> {error1,Who};
		{writer, _} -> ok;
		undefined -> ok;
		_ -> {error2,Who}
	end,
	% if there is a list of cancelers, then we for sure canceled
	case C of
		[] -> ok;
		C1 when is_list(C1) -> if CC -> ok; true -> {error,C,C1} end
	end,
	% if noreply, then one of the waiting_* incresaed its size by one, and who do not changed!
	%    we also know that for acq it will grow readers, acq_excl writers, and cancel_wait
	case {R,RL} of
		{[],L} when L =/= 0 -> {error1,R,RL};
		{[_],L} when L =/= 1 -> {error2,R,RL};
		{[_,_],L} when L =/= 2 -> {error3,R,RL};
		_ -> ok
	end
	},
	case All of {ok, ok, ok, ok, ok, ok, ok} -> ok; _ -> All end.

% LOGIC
-ifdef(debug).
handle_call(Call, From, S1) ->
	ok = check_invariants(S1),
	R = c(Call, From, S1),
	case R of
		{reply, _, S2} ->
			ok = check_invariants(S1, S2),
			R;
		{noreply, S2} ->
			ok = check_invariants(S1, S2),
			R
	end.
-else.
handle_call(Call, From, S1) ->
	c(Call, From, S1).
-endif.

c(acq_excl, _, S = #state{canceled=true}) ->
	{reply, canceled, S};
c(acq, _, S = #state{canceled=true}) ->
	{reply, canceled, S};


c(acq_excl, From, S = #state{waiting_writers=_W,who=undefined}) ->
	{reply, ok, S#state{who={writer,From}}};

c(acq_excl, From, S = #state{waiting_writers=W}) ->
	W2 = queue:in(From, W),
	{noreply, S#state{waiting_writers=W2}};

c(rel_excl, From1, S = #state{waiting_readers=R,waiting_writers=W,who={writer,_From2},waiting_readers_len=RL}) ->
	gen:reply(From1, ok), % quick reply, to not block this process, and make it O(1).
	%{Pid1,_} = From1,  % hack - internal structure of From can change.
	%{Pid2,_} = From2,
	%Pid1 = Pid2,
	S2 = case S of
		#state{canceled=true,waiting_cancelers=C} when C =/= [] ->
			spawn_notify_cancelers(C, cancelation_done),
			S#state{who=undefined,waiting_cancelers=[]};
		#state{waiting_readers=[],waiting_writers=?W_EMPTY} ->
			S#state{who=undefined};
		#state{waiting_readers=[]} ->
			{{value,Next}, W2} = queue:out(W),
			gen:reply(Next, ok),
			S#state{who={writer,Next},waiting_writers=W2};
		_ ->
			F1 = fun(Reader,N) -> gen:reply(Reader, ok), N+1 end,
			%FF = fun() -> Total2 = lists:foldl(F1, 0, R) end,
			% if needed (R list is big), we can send messages in parallel in bg, and wait asynchronously
			% (we can precompute Total on acq, as we add it elem by elem)
			% this will allow us to receive rel or cancel quicker
			if
				(RL < 10) -> lists:foldl(F1, 0, R);
				%(RL < 10) -> FF();
				true -> spawn_link(lists, foldl, [F1, 0, R])
				%true -> spawn_link(FF)
			end,
			Total = RL,
			S#state{who={readers,Total},waiting_readers=[],waiting_readers_len=0}
	end,
	{noreply, S2};

c(acq, _From, S = #state{who=undefined}) ->
	{reply, ok, S#state{who={readers, 1}}};
c(acq, From, S = #state{who={readers,N},waiting_readers=R,waiting_readers_len=RL}) ->
	case S of
		#state{waiting_writers=?W_EMPTY} ->
			{reply, ok, S#state{who={readers,N+1}}};
		_ ->
			{noreply, S#state{waiting_readers=[From|R],waiting_readers_len=RL+1}}
	end;
c(acq, From, S = #state{who={writer,_},waiting_readers=R,waiting_readers_len=RL}) ->
	{noreply, S#state{waiting_readers=[From|R],waiting_readers_len=RL+1}};

		
c(rel, From, S = #state{waiting_readers=_R,waiting_writers=W,who={readers,N0}}) when N0 >= 1 ->
	gen:reply(From, ok),  % quick reply, to not block this process, and make it O(1).
	S2 = case S of
		#state{who={readers,1},canceled=true,waiting_cancelers=C} when C =/= [] ->  % actually we do not need when C =/= [], here as for each will just skip empty list
			spawn_notify_cancelers(C, cancelation_done),
			S#state{who=undefined,waiting_cancelers=[]};
		#state{who={readers,1},waiting_writers=?W_EMPTY} ->
			S#state{who=undefined};
		#state{who={readers,1}} ->
			{{value,Next}, W2} = queue:out(W),
			gen:reply(Next, ok),
			S#state{who={writer,Next},waiting_writers=W2};
		#state{who={readers,N}} when N > 1 ->
			S#state{who={readers, N-1}}
	end,
	{noreply, S2};


c(cancel, _From, S = #state{canceled=true}) ->
	{reply, ok, S};
c(cancel, _From, S = #state{waiting_readers=R,waiting_writers=W}) ->
	spawn_cancel_both(R, W),
	{reply, ok, S#state{canceled=true,waiting_readers=[],waiting_readers_len=0,waiting_writers=?W_EMPTY}};

c(cancel_wait, _From, S = #state{canceled=true,who=undefined}) ->
	{reply, ok, S};
c(cancel_wait, From, S = #state{waiting_readers=R,waiting_writers=W,who=Who,waiting_cancelers=C}) ->
	spawn_cancel_both(R, W),
	case Who of
		undefined ->
			[] = C, % internal invariants
			[] = R,
			?W_EMPTY = W,
			0 = S#state.waiting_readers_len,
			{reply, ok, S#state{canceled=true}};
		_ ->
			% we will notify cancelers in rel or rel_excl
			{noreply, S#state{canceled=true,waiting_cancelers=[From|C],waiting_readers=[],waiting_readers_len=0,waiting_writers=?W_EMPTY}}
	end;

c(uncancel, _From, S = #state{canceled=true,waiting_cancelers=C}) ->
	notify_cancelers(C, cancelation_canceled),
	{reply, uncanceled, S#state{canceled=false,waiting_cancelers=[]}};

c(uncancel, _From, S = #state{canceled=false}) ->
	{reply, already_uncanceled, S};

c(stat, _, S) ->
	{reply, S, S};

c(Other, From, S) ->
	error_logger:error_msg("Not handled call ~p from ~p when in state~n ~p received!~n",[Other,From,S]),
	{noreply, S}.


% INTERNAL AUXILARY FUNCTIONS

spawn_notify_cancelers(C, Msg) when is_list(C) ->
	spawn_link(fun() -> notify_cancelers(C, Msg) end).

notify_cancelers(C, Msg) when is_list(C) ->
	lists:foreach(fun(Canceler) -> gen:reply(Canceler, Msg) end, C).


spawn_cancel_both(R, W) when is_list(R) ->
	spawn_link(fun() -> cancel_both(R, W) end).

cancel_both(R, W) ->
	lists:foreach(fun(Reader) -> gen:reply(Reader, canceled) end, R),
	ok = cancel_writers(W).

cancel_writers(?W_EMPTY) ->
	ok;
cancel_writers(W) ->
	{{value,Next}, W2} = queue:out(W),
	gen:reply(Next, canceled),
	cancel_writers(W2).


handle_cast(stop, S) ->
	{stop, normal, S};
handle_cast(Other, S) ->
	error_logger:error_msg("Not handled cast ~p when in state~n ~p received!~n",[Other,S]),
	{noreply, S}.

handle_info(Other, S) ->
	error_logger:error_msg("Not handled msg ~p when in state~n ~p received!~n",[Other,S]),
	{noreply, S}.



% EXTERNAL API

acq_excl(P, T) ->
	gen_server:call(P, acq_excl, T).
acq_excl(P) ->
	acq_excl(P, ?TIMEOUT).
rel_excl(P) ->
	gen_server:call(P, rel_excl).
acq(P, T) ->
	gen_server:call(P, acq, T).
acq(P) ->
	acq(P, ?TIMEOUT).
rel(P) ->
	gen_server:call(P, rel).
cancel(P) ->
	gen_server:call(P, cancel).
cancel_wait(P,T) ->
	gen_server:call(P, cancel_wait, T).
cancel_wait(P) ->
	cancel_wait(P, ?TIMEOUT).

uncancel(P) ->
	gen_server:call(P, uncancel).

stat(P) ->
	gen_server:call(P, stat).

% default shortcut, to global locker

acq_excl() -> acq_excl(?NAME0).
rel_excl() -> rel_excl(?NAME0).
acq() -> acq(?NAME0).
rel() -> rel(?NAME0).
cancel() -> cancel(?NAME0).
cancel_wait() -> cancel_wait(?NAME0).
uncancel() -> uncancel(?NAME0).
stat() -> stat(?NAME0).

%
% transactional API
%
% TODO: pre-detect some deadlocks on client side, by storing acq lock info in process dictionary

read(N, F) when is_function(F) ->
	try wielok:acq(N) of
		ok ->
			V = try F() of
				X -> {ok, X}
			catch
				E:EV -> {error, eval, {E, EV}}
			end,
			wielok:rel(N),
			V;
		What ->
			{error, acq, What}
	catch
		E:EV -> {error, acq, {E,EV}}
	end.

write(N, F) when is_function(F) ->
	try wielok:acq_excl(N) of
		ok ->
			V = try F() of
				X -> {ok, X}
			catch
				E:EV -> {error, eval, {E, EV}}
			end,
			wielok:rel_excl(N),
			V;
		What ->
			{error, acq, What}
	catch
		E:EV -> {error, acq, {E,EV}}
	end.
