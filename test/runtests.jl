using Test
using Distributed
using ClusterManagers
using DistributedQuery

if Base.current_project() != nothing
    proj_path = joinpath(["/", split(Base.current_project(), "/")[1:end-1]...])
    p = addprocs(SlurmManager(3),
                 time="00:30:00",
                 exeflags="--project=$(proj_path)", ntasks_per_node=1)
else
    p = addprocs(SlurmManager(3),
                 time="00:30:00",
                 ntasks_per_node=1, partition=gpu)
end

@everywhere using DistributedQuery
@everywhere using DataFrames
@everywhere using CSV
#proc_chan, data_chan = make_query_channels(p, [1], chan_depth::Int=5);

_shard_file_list = ["../mockData/iris_df_1.jlb",
                    "../mockData/iris_df_2.jlb",
                    "../mockData/iris_df_3.jlb"]

shard_file_list = [joinpath(dirname(pathof(DistributedQuery)), sf) for sf in _shard_file_list]
serialized_file_list = DistributedQuery.Utilities.partition(shard_file_list, p) 
data_worker_pool = p
proc_worker_pool = [myid()]

fut = DistributedQuery.deployDataStore(data_worker_pool, DistributedQuery.Utilities.loadSerializedFiles, [serialized_file_list])

test_res = [fetch(fut[p]) == @fetchfrom p DistributedQuery.DataContainer for p in data_worker_pool]

@test all(test_res)

if all(test_res)
    print("DistributedQuery.deployDataStore passed\n")
else
    print("DistributedQuery.deployDataStore Failed\n test_res: $(test_res)")
end


proc_chan, data_chan = DistributedQuery.make_query_channels(data_worker_pool, proc_worker_pool)

status_chan = RemoteChannel(()->Channel{Any}(10000), myid())

query_f = (data, column) -> data[:, column]

query_args = ["sepal_l"]

agrigate_f = (x...) -> sum(vcat(x...))

sentinal_fut =
    [@spawnat p DistributedQuery.sentinal(DistributedQuery.DataContainer,
                                          data_chan[myid()] ,proc_chan,
                                          status_chan)
     for p in data_worker_pool]




query_task = @async DistributedQuery.query_client(data_chan, proc_chan[1], agrigate_f, query_f, query_args...)

[take!(status_chan) for i in 1:1000 if isready(status_chan)]
[put!(v, "Done") for (k,v) in data_chan]
[wait(f) for f in sentinal_fut]
[take!(status_chan) for i in 1:1000 if isready(status_chan)]

sentinal_fut =
    [@spawnat p DistributedQuery.sentinal(DistributedQuery.DataContainer,
                                          data_chan[myid()] ,proc_chan,
                                          status_chan)
     for p in data_worker_pool]

query_task = @async DistributedQuery.query_client(data_chan, proc_chan[1], agrigate_f, query_f, query_args...)
query_timeout_in_s = 10
sleep(query_timeout_in_s)


if istaskdone(query_task)
    local_result = sum([sum(fetch(f[2])[:, query_args[1]]) for f in fut])
    query_result = fetch(query_task)
    @test local_result == query_result
else
    print("test query was not done in query_timeout_in_s: $query_timeout_in_s")
    @test false
end

#rmprocs(p);
