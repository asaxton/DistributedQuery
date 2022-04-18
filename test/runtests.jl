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
                 ntasks_per_node=1)
end

@everywhere using DistributedQuery
@everywhere using DataFrames
@everywhere using CSV

_shard_file_list = ["../mockData/iris_df_1.jlb",
                    "../mockData/iris_df_2.jlb",
                    "../mockData/iris_df_3.jlb"]

shard_file_list = [joinpath(dirname(pathof(DistributedQuery)), sf) for sf in _shard_file_list]
serialized_file_list = DistributedQuery.Utilities.partition(shard_file_list, p) 
data_worker_pool = p
proc_worker_pool = [myid()]

@testset begin
    fut = DistributedQuery.deployDataStore(data_worker_pool, DistributedQuery.Utilities.loadSerializedFiles, [serialized_file_list])

    @test all([fetch(fut[p]) == @fetchfrom p DistributedQuery.DataContainer for p in data_worker_pool])

    proc_chan, data_chan = DistributedQuery.make_query_channels(data_worker_pool, proc_worker_pool)

    status_chan = RemoteChannel(()->Channel{Any}(10000), myid())

    @everywhere query_f = (_data, _column) -> sum(_data[:, _column])

    query_args = ["sepal_l"]

    agrigate_f = (x...) ->  sum([x...])

    sentinal_fut =
        [@spawnat p DistributedQuery.sentinal(DistributedQuery.DataContainer,
                                              data_chan[myid()] ,proc_chan,
                                              status_chan)
         for p in data_worker_pool]



    query_task = @async DistributedQuery.query_client(data_chan, proc_chan[1], agrigate_f, query_f, query_args...)
    query_timeout_in_s = 5
    if !istaskdone(query_task)
        sleep(query_timeout_in_s)
    end

    if istaskdone(query_task)
        local_result = sum([sum(fetch(f[2])[:, query_args[1]]) for f in fut])
        query_result = sum(fetch(query_task))
        #test_to_digit = 4
        @test local_result ≈ query_result atol=0.00001
        #@test round(local_result;  digits=test_to_digit) == round(query_result;  digits=test_to_digit)
        #@test local_result == query_result
    else
        print("test query was not done in query_timeout_in_s: $query_timeout_in_s")
        @test false
    end


    [put!(v, "Done") for (k,v) in data_chan]
    sentinal_shutdown_timeout = 4
    sleep(4)
    @test all([isready(f) for f in sentinal_fut])
end
rmprocs(p);
