using CSV
using Test
using Distributed
using ClusterManagers
using DistributedQuery
using Serialization
using DataFrames

# Test data
_csv_file_list = ["../mockData/iris_df_1.csv",  "../mockData/iris_df_2.csv",
                 "../mockData/iris_df_3.csv", "../mockData/iris_df_4.csv",
                 "../mockData/iris_df_5.csv", "../mockData/iris_df_6.csv"]                 

csv_file_list = [joinpath(dirname(pathof(DistributedQuery)), sf) for sf in _csv_file_list]
serialized_file_list = []
@info "Creating serialized data files for testing"
for f in csv_file_list
    sfname = "$(join(split(f, ".")[1:end-1],"."))_test.jlb"
    serialize(sfname, DataFrame(CSV.File(f)))
    push!(serialized_file_list, sfname)
end

@testset begin
    ### Test CSV Files
    @info "Testing deploy datastore with CSV files"
    # Launch hosts
    if Base.current_project() != nothing
        proj_path = joinpath(["/", split(Base.active_project(), "/")[1:end-1]...])
        p = addprocs(SlurmManager(3),
                    time="00:30:00", ntasks_per_node=1,
                    exeflags="--project=$(proj_path)")
    else
        p = addprocs(SlurmManager(3),
                    time="00:30:00", ntasks_per_node=1,
                    exeflags="--project=$(proj_path)")
    end

    # Setup host environments
    ap_dir = joinpath(splitpath(Base.active_project())[1:end-1])
    if "tmp" == splitpath(ap_dir)[2]
        hostNames = [@fetchfrom w gethostname() for w in p]
        ap_dir_list = [joinpath(ap_dir, d) for d in readdir(ap_dir)]
        for (w, hn) in zip(p, hostNames)

            @fetchfrom w begin
                open(`mkdir $(ap_dir)`) do f
                    read(f, String)
                end
            end
        end

        for hn in hostNames
            for fpn in ap_dir_list
                open(`scp $(fpn) $(hn):$(fpn)`) do f
                    read(f, String)
                end
            end
        end
    end
    
    @everywhere using DistributedQuery
    @everywhere using DataFrames
    @everywhere using CSV

    data_worker_pool = p
    proc_worker_pool = [myid()]

    # Deploy the datastore
    host_dict = DistributedQuery.Utilities.partition(csv_file_list, p)
    fut = DistributedQuery.deployDataStore(p, DistributedQuery.Utilities.loadCSVFiles, [host_dict])
    @test all([fetch(fut[w]) == @fetchfrom w DistributedQuery.DataContainer for w in p])
    
    # Kill procs so can test again with serialized files
    rmprocs(p)

    ### Test serialized files
    @info "Testing deploy datastore with serialized files"
    # Launch hosts
    if Base.active_project() != nothing
        proj_path = joinpath(["/", split(Base.current_project(), "/")[1:end-1]...])
        p = addprocs(SlurmManager(3),
                    time="00:30:00", ntasks_per_node=1,
                    exeflags="--project=$(proj_path)")
    else
        p = addprocs(SlurmManager(3),
                    time="00:30:00", ntasks_per_node=1,
                    exeflags="--project=$(proj_path)")
    end

    # Setup host environments
    ap_dir = joinpath(splitpath(Base.active_project())[1:end-1])
    if "tmp" == splitpath(ap_dir)[2]
        hostNames = [@fetchfrom w gethostname() for w in p]
        ap_dir_list = [joinpath(ap_dir, d) for d in readdir(ap_dir)]
        for (w, hn) in zip(p, hostNames)

            @fetchfrom w begin
                open(`mkdir $(ap_dir)`) do f
                    read(f, String)
                end
            end
        end

        for hn in hostNames
            for fpn in ap_dir_list
                open(`scp $(fpn) $(hn):$(fpn)`) do f
                    read(f, String)
                end
            end
        end
    end

    @everywhere using DistributedQuery
    @everywhere using DataFrames
    @everywhere using CSV

    data_worker_pool = p
    proc_worker_pool = [myid()]

    # Deploy the datastore
    host_dict = DistributedQuery.Utilities.partition(serialized_file_list, p)
    fut = DistributedQuery.deployDataStore(p, DistributedQuery.Utilities.loadSerializedFiles, [host_dict])
    @test all([fetch(fut[w]) == @fetchfrom w DistributedQuery.DataContainer for w in p])

    @info "Testing Query Channels"
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
    rmprocs(p);
    @info "Cleaning up serialized files"
    for f in serialized_file_list
        rm(f)
    end
end

