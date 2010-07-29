-module(wielok_tests).

-export([go/0]).
-export([gen_test/2]).

%-define(debug(A,B), void).
-define(debug(A,B), io:format(A,B)).

-define(NAME, {global,wielok}).


repeat(N, F) when is_function(F, 1), is_integer(N), N >= 0 ->
	repeat(F, N, 0).

repeat(_F,N,N) ->
	ok;
repeat(F,N,I) ->
	F(I+1),
	repeat(F,N,I+1).


repeat_collect(N, F) when is_function(F, 1), is_integer(N), N >= 0 ->
	repeat_collect(F, N, 0, []).

repeat_collect(_F,N,N,Acc) ->
	lists:reverse(Acc);
repeat_collect(F,N,I,Acc) ->
	X = F(I+1),
	repeat_collect(F,N,I+1,[X|Acc]).

go() ->
	repeat(2000, fun(_) -> ok = gen_test(gen_size()) end).

gen_size() ->
	case random:uniform(7) of
		1 -> 1;
		2 -> 2;
		3 -> 3;
		4 -> 4;
		5 -> random:uniform(10);
		6 -> random:uniform(100);
		7 -> random:uniform(1000)
	end.

gen_test(N) ->
	MainSeed = erlang:md5(term_to_binary({self(),now(),make_ref()})),
	gen_test(N, MainSeed).

gen_test(N, MainSeed) ->
	<<A:32,B:32,C:32,_/binary>> = erlang:md5(term_to_binary(MainSeed)),
	random:seed(A, B, C),

	wielok:start_global(wielok),
	io:format("main seed ~p~n", [MainSeed]),
	io:format("wielok started:~n", []),
	Self = self(),
	Count = gen_size(), % maximal number of command to execute by processes

	io:format("Starting ~p processes, each for maximal ~p commands.~n", [N, Count]),
	Processes = repeat_collect(N, fun(I) -> spawn_link(fun() -> test_start(I, Self, {seed, MainSeed, I}, Count) end) end),

	done = receive_all(Processes),

	LastState = wielok:stat(?NAME),

	{state,[],0,{[],[]},_,_,[]} = LastState,

	io:format("wielok stoping: ~p~n", [LastState]),
	wielok:stop(?NAME),
	sleep(10),
	ok.


receive_all([]) ->
	done;
receive_all(Processes) ->
	receive
		{Pid, done} ->
			receive_all(Processes -- [Pid])
		after 60000 ->
			timeout
	end.


test_start(I, Parent, Seed, Count) ->
	<<A:32,B:32,C:32,_/binary>> = erlang:md5(term_to_binary(Seed)),
	random:seed(A, B, C),
	X = random:uniform(Count),
	io:format("starter process #~p, doing ~p commands~n", [I, X]),
	Tests = [
		{0.05, stat, fun subtest_stat/0},
		{0.05, sleep, fun subtest_sleep/0},
		{0.02, cancel, fun subtest_cancel/0},
		{0.02, cancel_wait, fun subtest_cancel_wait/0},
		{0.08, uncancel, fun subtest_uncancel/0},
		{0.1, acq_excl, fun subtest_acq_excl/0}, % and rel_excl
		{0.9, acq, fun subtest_acq/0}, % and rel
		{0.1, acq_excl_trans, fun subtest_acq_excl_trans/0},
		{0.9, acq_trans, fun subtest_acq_trans/0}
	],
	ProbSum = lists:foldl(fun({Prob,_,_},Sum) when Prob > 0.0 -> Sum+Prob end, 0.0, Tests),
	repeat(X, fun(_) -> test_go_sub(Tests, ProbSum) end),
	Parent ! {self(), done}.

test_go_sub(Tests, ProbSum) ->
	X = random:uniform(),
	ok = choise_until(ProbSum*X, Tests).

choise_until(_, [{_,N,F}]) ->
	?debug("~p ~p~n",[self(), N]),
	F();
choise_until(X, [{P,N,F}|T]) ->
	case X-P of
		Z when Z =< 0.0 ->
			?debug("~p ~p~n",[self(), N]),
			F();
		_ -> choise_until(X-P, T)
	end.

subtest_stat() ->
	X = wielok:stat(?NAME),
	?debug("~p stat done ~p~n",[self(), X]),
	ok = case X of
		{state, R, RL, W, _Who, C, CC} when is_list(R), is_integer(RL), RL >= 0, is_boolean(C), is_list(CC) ->
			case queue:is_queue(W) of
				true -> ok;
				_ -> errorW
			end;
		_ -> errorG
	end,
	ok.

subtest_cancel() ->
	X = wielok:cancel(?NAME),
	?debug("~p cancel done ~p~n",[self(), X]),
	ok = X,
	ok.

subtest_cancel_wait() ->
	X = wielok:cancel_wait(?NAME),
	?debug("~p cancel_wait done ~p~n",[self(), X]),
	ok.

subtest_uncancel() ->
	X = wielok:uncancel(?NAME),
	?debug("~p uncancel done ~p~n",[self(), X]),
	ok = case X of
		uncanceled -> ok;
		already_uncanceled -> ok;
		_ -> error
	end,
	ok.


subtest_acq() ->
	X = wielok:acq(?NAME),
	?debug("~p acq done ~p~n",[self(), X]),
	case X of
		ok ->
			Timeout = random:uniform(100),
			sleep(Timeout),
			?debug("~p rel ~p~n",[self(), X]),
			ok = wielok:rel(?NAME);
		canceled ->
			ok
	end.

subtest_acq_excl() ->
	X = wielok:acq_excl(?NAME),
	?debug("~p acq_excl done ~p~n",[self(), X]),
	case X of
		ok ->
			Timeout = random:uniform(5),
			sleep(Timeout),
			?debug("~p rel_excl ~p~n",[self(), X]),
			ok = wielok:rel_excl(?NAME);
		canceled ->
			ok
	end.


subtest_acq_trans() ->
	X = wielok:read(?NAME, fun() ->
			case random:uniform(20) of
				1 ->
					throw(bad);
				_ ->
					ok
			end,
			Timeout = random:uniform(40),
			sleep(Timeout),
			case random:uniform(20) of
				1 ->
					throw(bad);
				_ ->
					ok
			end
		end),
	?debug("~p read done ~p~n",[self(), X]).

subtest_acq_excl_trans() ->
	X = wielok:write(?NAME, fun() ->
			case random:uniform(20) of
				1 ->
					throw(bad);
				_ ->
					ok
			end,
			Timeout = random:uniform(40),
			sleep(Timeout),
			case random:uniform(20) of
				1 ->
					throw(bad);
				_ ->
					ok
			end
		end),
	?debug("~p write done ~p~n",[self(), X]).


sleep(T) ->
	?debug("~p sleeping for ~pms~n",[self(), T]),
	receive
		after T ->
			?debug("~p sleeped ~pms~n",[self(), T]),
			ok
	end.

subtest_sleep() ->
	Timeout = random:uniform(100),
	sleep(Timeout),
	ok.
