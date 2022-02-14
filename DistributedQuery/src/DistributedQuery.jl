module DistributedQuery
using Distributed

function sentinal(df, q_channel,res_channel_dict, status_chan)
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
            global q_res = query_req["query_f"](df, query_req["query_args"]...)
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
