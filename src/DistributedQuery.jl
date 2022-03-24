module DistributedQuery
using Distributed
using Serialization
using DataFrames
using CSV

"""
"""
function deployDataStore(data_worker_pool, proc_worker_pool, serialized_file_list)
    if length(data_worker_pool) != length(serialized_file_list)
        throw(error("""data_worker_pool and serialized_file_list must be the same length: """*
                    """length(data_worker_pool):$(length(data_worker_pool)), """*
                    """length(serialized_file_list):$(length(serialized_file_list))"""))
    end
    fut = Dict()
    for (dw, sf) in zip(data_worker_pool, serialized_file_list)
        _fut = @spawnat dw global DataContainer = deserialize(sf)
        push!(fut, dw => _fut)
    end
    fut
    #[wait(f) for f in fut]
end

"""
Sentinal waits for a item to be put on the q_channel.

The item put in q_channel should either be a String with the message "Done". This will cause sentinal() to exit it's  while loop and truen.


"""
function sentinal(DataContainer, q_channel,res_channel_dict, status_chan)
    stat_dict = Dict("message" => "In sentinal",
                     "t_idx" => 1,
                     "myid()" => myid())
    put!(status_chan, stat_dict)
    while true
        stat_dict["message"] = """In sentinal, waiting for take """*
            """on channel $(q_channel)"""
        put!(status_chan, stat_dict)
        query_req = take!(q_channel)
        stat_dict["message"] = "In sentinal, got take"
        put!(status_chan, stat_dict)
        if query_req == "Done"
            stat_dict["message"] = "In sentinal, break-ing"
            put!(status_chan, stat_dict)
            break
        end
        stat_dict["message"] = "In sentinal, query_f-ing"
        put!(status_chan, stat_dict)
        try
            global q_res = query_req["query_f"](DataContainer, query_req["query_args"]...)
        catch e
            stat_dict["message"] = "In sentinal, catching error. e: $(e)"
            put!(status_chan, stat_dict)
            put!(res_channel_dict[query_req["client"]], e)
        finally
            stat_dict["message"] = "In sentinal, putting-ing"
            put!(status_chan, stat_dict)
            put!(res_channel_dict[query_req["client"]], q_res)
        end
    end
end

"""
"""
function query_client(q_channels, res_channel_dict, agrigate_f, query_f, query_args...)
    query_req = Dict("client" => myid(),
                     "query_f" => query_f,
                     "query_args" => query_args)
    for chan ∈ values(q_channels)
        put!(chan, query_req)
    end
    res_list = []
    Threads.@threads for i ∈ 1:length(q_channels)
        push!(res_list, take!(res_channel_dict))
    end
    res = agrigate_f(res_list...)
    return res
end

function make_query_channels(data_worker_pool, proc_worker_pool, chan_depth::Int=5)
    proc_chan = Dict(p => RemoteChannel(()->Channel{Any}(chan_depth), p) for p ∈ proc_worker_pool)
    data_chan = Dict(p => RemoteChannel(()->Channel{Any}(chan_depth), p) for p ∈ data_worker_pool)
    return proc_chan, data_chan
end

end
