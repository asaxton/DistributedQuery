#DQUtilities.jl
module Utilities

using DataFrames
using Distributed
using Serialization

"""

	loadSerializedFiles(partitioned_file_list)

Example/Utility function for loading serialized files using the deploydatastore

# Arguments
- `partitioned_file_list::Any`: Dictionary of arrays where each key is a host id and the array is a list of files to be loaded.

# Returns
- `Dict{Any, Any}`: Returns a DataFrame of the contents of all the serialized files.
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
	loadCSVFiles(partitioned_file_list)

Example/Utility function for loading CSV files using the deploydatastore

# Arguments
- `partitioned_file_list::Any`: Dictionary of arrays where each key is a host id and the array is a list of files to be loaded.

# Returns
- `Dict{Any, Any}`: Returns a DataFrame of the contents of all the serialized files.
"""
function loadCSVFiles(partitioned_file_list)
	df = DataFrame()
	for file in partitioned_file_list[myid()]
		data = DataFrame(CSV.File(file))
		append!(df, data)
	end
	return df
end

"""

	partition(data, hosts)

Splits the data into a dictionary of non-overlapping equal sized chunks with the host id as the key of each partition

# Arguments
- `data::Any`: Array of data to be partitioned
- `hosts::Any`: Array of host ids that will be the key of each partition
# Returns
- `Dict{Any, Any}`: Returns a Ditionary with worker id as the key, and a partition of the data as a value.
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
