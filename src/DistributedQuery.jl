module DistributedQuery
using Distributed
using Serialization
using DataFrames
using CSV

"""

    deployDataStore(data_worker_pool, serialized_file_list)

deployDataStore assumes your have already a number of serialized files that contain partitions of your dataset. deserialize() will be run on the files that you pass to serialize_file_list and the result will assigned to DistributedQuery.DataContainer. As a result the hosting model demands that data_worker_pool and serialized_file_list must be the same length

# Arguments
- `data_worker_pool::Any`: Array of worker ids that each of the data partitions will be placed on
- `serialized_file_list::Any`: Array of strings that contain the path and filename to the serialized file

# Returns
- `Dict{Any, Future}`: Returns a Ditionary with worker id as the key, and Future of the spawend deserialize task

# Throws
- `ERROR`: Thrown if length(data_worker_pool) != length(serialized_file_list)

"""
function deployDataStore(data_worker_pool, serialized_file_list)
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

    sentinal(DataContainer, q_channel, res_channel_dict, status_chan)

`sentinal()` waits for a item to be put on the q_channel.

The item put in `q_channel` should either be a `String` with the message `"Done"`, which will cause `sentinal()` to exit it's  while loop and return or the following **Query Dict**.

- **Query Dict**: this Dict will contain 3 keys: `"query_f"`, `"query_args"`, and `"client"`. The primary function of `sentinal()` is to call, `q_res = query_req["query_f"](DataContainer, query_req["query_args"]...)` and place the result `q_res` in `res_channel_dict[query_req["client"]]`

# Arguments
- `DataContainer::Any`: This should be a container. In typical usage it'll be a DataFrame that was loaded from deployDataStore()
- `q_channel::Channel`: A single channel that will take!() wait for a query dict to be put!() on it.
- `res_channel_dict::Dict{Any, Channel}`: Indexed by worker id. Will be used to look up which channel to place query result on
- `status_chan::Channel`: Status Messages are put!() on this channel

# Returns
- `Nothing`

# Throws
- `Nothing`
"""
function sentinal(DataContainer, q_channel, res_channel_dict, status_chan)
    stat_dict = Dict("message" => "In sentinal",
                     "t_idx" => 1,
                     "myid()" => myid())
    put!(status_chan, stat_dict)

    query_failed = false
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
            query_f = query_req["query_f"]
            global q_res = query_f(DataContainer, query_req["query_args"]...)
        catch e
            query_failed = true
            stat_dict["message"] = "In sentinal, catching error. e: $(e)"
            put!(status_chan, stat_dict)
            put!(res_channel_dict[query_req["client"]], e)
        end
        if !query_failed
            stat_dict["message"] = "In sentinal, putting-ing"
            put!(status_chan, stat_dict)
            put!(res_channel_dict[query_req["client"]], q_res)
            query_failed = false
        end
    end
    stat_dict["message"] = "Final Exit"
    put!(status_chan, stat_dict)
end


"""

    function query_client(q_channels, res_channel_dict, agrigate_f, query_f, query_args...)

# Throws
- `Array{Excpetion}`
"""
function query_client(q_channels, res_channel_dict, agrigate_f, query_f, query_args...)
    query_req = Dict("client" => myid(),
                     "query_f" => query_f,
                     "query_args" => query_args)
    for chan ∈ values(q_channels)
        put!(chan, query_req)
    end
    res_list = []

    for i ∈ 1:length(q_channels)
        res = take!(res_channel_dict)
        push!(res_list, res)
    end
    except_mask = [typeof(r) <: Exception for r in res_list]
    if any(except_mask)
        zipped_id_errors = collect(zip(["Error on worker $(c.where)" for c in q_channels], res_list))
        throw([zipped_id_errors[except_mask]])
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
