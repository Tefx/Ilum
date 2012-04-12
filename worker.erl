-module (worker).
-export ([start/1]).
-import (wman, [require/1, release/2]).

start(Manager) ->
	spawn(fun() -> worker_run(Manager) end).

worker_run(Manager) ->
	wman:release(Manager, self()),
	receive
		{Pid, eval, E} ->
			Pid ! eval(Manager, E),
			worker_run(Manager);
		_ -> 
			worker_run(Manager)
	end.

eval(Manager, [Func|Args]) -> erlang:apply(Func, pmap_eval(Manager, Args)).

pmap_eval(M, L) ->
	S = self(),
	Pids = lists:map(
		fun(X) -> 
			spawn(fun() -> remote_eval(M, S, X) end) 
		end, L),
	gather(Pids).

gather([P|T]) -> 
	receive
		{P, Result} -> [Result|gather(T)]
	end; 
gather([]) -> 
	[].

remote_eval(_, Parent, E) when not is_list(E) -> 
	Parent ! E;
remote_eval(Manager, Parent, E) ->
	Worker = wman:require(Manager),
	Worker ! {self(), eval, E},
	receive
		Result ->
			Parent ! {self(), Result}
	end.



