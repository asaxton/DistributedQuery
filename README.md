# jlDistributedQuery

**DistributedQuery** Is meant to host large datasets in partitioned across multiple workers with queryable access. Idealy, worker will be remote worker with their own memory. has 3 major part, `deployDataStore()`, `sentinal()`, `make_query_channels()`, `query_client()`. The basic exicution is the following,

```
using Test
using Distributed
using ClusterManagers
using DistributedQuery

proj_path = joinpath(["/", split(Base.current_project(), "/")[1:end-1]...])
p = addprocs(SlurmManager(3),
             time="00:30:00",
             exeflags="--project=$(proj_path)", ntasks_per_node=1)

@everywhere using DistributedQuery
@everywhere using DataFrames
@everywhere using CSV
#proc_chan, data_chan = make_query_channels(p, [1], chan_depth::Int=5);

_shard_file_list = ["../mockData/iris_df_1.jlb",
                    "../mockData/iris_df_2.jlb",
                    "../mockData/iris_df_3.jlb"]

shard_file_list = [joinpath(dirname(pathof(DistributedQuery)), sf) for sf in _shard_file_list]
serialized_file_list = shard_file_list
data_worker_pool = p
proc_worker_pool = [myid()]
fut = DistributedQuery.deployDataStore(data_worker_pool, proc_worker_pool, serialized_file_list)

test_res = [fetch(fut[p]) == @fetchfrom p DistributedQuery.DataContainer for p in data_worker_pool]

@test all(test_res)

if all(test_res)
    print("DistributedQuery.deployDataStore passed\n")
else
    print("DistributedQuery.deployDataStore Failed\n test_res: $(test_res)")
end


proc_chan, data_chan = DistributedQuery.make_query_channels(data_worker_pool, proc_worker_pool)

status_chan = RemoteChannel(()->Channel{Any}(10000), myid())

sentinal_fut =
    [@spawnat p DistributedQuery.sentinal(DistributedQuery.DataContainer,
                                          data_chan[myid()] ,proc_chan,
                                          status_chan)
     for p in data_worker_pool]

rmprocs(p);

```