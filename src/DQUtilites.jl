#DQUtilities.jl
module Utilities

using DataFrames
using Distributed
using Serialization

"""
loadSerializedFiles(partitioned_file_list)

Example/Helper function for loading serialized files using the deploydatastore

# Arguments
- `partitioned_file_list::Any`: Dictionary of arrays where eacch key is a host id and the array is a list of files to be loaded.

# Returns
- `Dict{Any, Any}`: Returns a .
"""
function loadSerializedFiles(partitioned_file_list)
	df = DataFrame()
	for file in partitioned_file_list[myid()]
		data = deserialize(file)
		append!(df, data)
	end
	return df
end

"""
partition(data, hosts)

Splits the data into a dictionary of non-overlapping equal sized chunks with the host id as the key of each partition

# Arguments
- `data::Any`: Array of worker ids that each of the data partitions will be placed on
- `hosts::Any`: Array of host ids

# Returns
- `Dict{Any, Any}`: Returns a Ditionary with worker id as the key, and partition of the data.
"""
function partition(data, hosts)
	hosts_dict = Dict()
	chunk_size = Int(ceil(size(data,1)/size(hosts,1)))
	chunk = 0
	for host_id in hosts
		if (chunk+1)*chunk_size < size(data,1)
			hosts_dict[host_id] = data[(chunk*chunk_size+1):((chunk+1)*chunk_size)]
		else
			hosts_dict[host_id] = data[(chunk*chunk_size+1):end]
		end
		chunk += 1
	end
	return hosts_dict
end

end # module Helpers