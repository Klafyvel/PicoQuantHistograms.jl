module PicoQuantHistograms

using OrderedCollections
using Tables


abstract type FileFormat end
struct PHUFile end
struct PHDFile end

abstract type TagType end
struct TypeEmpty8 <: TagType end
struct TypeBool8 <: TagType end
struct TypeInt8 <: TagType end
struct TypeBitSet64 <: TagType end
struct TypeColor8 <: TagType end
struct TypeFloat8 <: TagType end
struct TypeTDateTime <: TagType end
struct TypeFloat8Array <: TagType end
struct TypeAsciiString <: TagType end
struct TypeWideString <: TagType end
struct TypeBinaryBlob <: TagType end

struct Tag{T <: TagType}
    id::String
    idx::Int32
    value::Any
    enhancement::Any
end

function Base.show(io::IO, t::Tag)
    print(io, t.id)
    if t.idx != -1
        print(io, "[", t.idx, "]")
    end
    if !isnothing(t.enhancement)
        print(io, " : ", t.value, " ", t.enhancement)
    else
        print(io, " : ", t.value)
    end
end
function Base.show(io::IO, t::Tag{T}) where {T <: TypeEmpty8}
    print(io, t.id)
    if t.idx != -1
        print(io, "[", t.idx, "]")
    end
end
function Base.show(io::IO, t::Tag{T}) where {T <: Union{TypeAsciiString, TypeWideString}}
    print(io, t.id)
    if t.idx != -1
        print(io, "[", t.idx, "]")
    end
    print(io, " : \"", t.enhancement, "\"")
end

function dispatch_tag(f, tagtype::UInt32, args...; kwargs...)
    if tagtype == 0xFFFF0008
        f(TypeEmpty8(), args...; kwargs...)
    elseif tagtype == 0x00000008
        f(TypeBool8(), args...; kwargs...)
    elseif tagtype == 0x10000008
        f(TypeInt8(), args...; kwargs...)
    elseif tagtype == 0x11000008
        f(TypeBitSet64(), args...; kwargs...)
    elseif tagtype == 0x12000008
        f(TypeColor8(), args...; kwargs...)
    elseif tagtype == 0x20000008
        f(TypeFloat8(), args...; kwargs...)
    elseif tagtype == 0x21000008
        f(TypeTDateTime(), args...; kwargs...)
    elseif tagtype == 0x2001FFFF
        f(TypeFloat8Array(), args...; kwargs...)
    elseif tagtype == 0x4001FFFF
        f(TypeAsciiString(), args...; kwargs...)
    elseif tagtype == 0x4002FFFF
        f(TypeWideString(), args...; kwargs...)
    elseif tagtype == 0xFFFFFFFF
        f(TypeBinaryBlob(), args...; kwargs...)
    else
        error("Unknown tag type for dispatch_tag: $tagtype")
    end
end

function readtag(io)
    tagid = strip(String(read(io, 32)), '\0')
    tagidx = read(io, Int32)
    tagtypecode = read(io, UInt32)
    @debug "Tag" tagid tagidx tagtypecode
    dispatch_tag(readtag, tagtypecode, io, tagid, tagidx)
end

function readtag(::TypeEmpty8, io, tagid, tagidx)
    Tag{TypeEmpty8}(tagid, tagidx, read(io, 8), nothing)
end
function readtag(::TypeBool8, io, tagid, tagidx)
    Tag{TypeBool8}(tagid, tagidx, any(read(io, 8) .!= 0), nothing)
end
function readtag(::TypeInt8, io, tagid, tagidx)
    Tag{TypeInt8}(tagid, tagidx, read(io, Int64), nothing)
end
function readtag(::TypeBitSet64, io, tagid, tagidx)
    Tag{TypeBitSet64}(tagid, tagidx, read(io, UInt64), nothing)
end
function readtag(::TypeColor8, io, tagid, tagidx)
    Tag{TypeColor8}(tagid, tagidx, read(io, UInt64), nothing)
end
function readtag(::TypeFloat8, io, tagid, tagidx)
    Tag{TypeFloat8}(tagid, tagidx, read(io, Float64), nothing)
end
function readtag(::TypeTDateTime, io, tagid, tagidx)
    Tag{TypeTDateTime}(tagid, tagidx, read(io, Float64), nothing)
end
function readtag(::TypeFloat8Array, io, tagid, tagidx)
    length = read(io, UInt32)
    vals = Vector{Float64}(undef, length / sizeof(Float64))
    readbytes!(io, vals)
    Tag{TypeFloat8Array}(tagid, tagidx, length, vals)
end
function readtag(::TypeAsciiString, io, tagid, tagidx)
    length = read(io, UInt64)
    vals = read(io, length)
    Tag{TypeAsciiString}(tagid, tagidx, length, strip(String(vals), '\0'))
end
function readtag(::TypeWideString, io, tagid, tagidx)
    length = read(io, UInt64)
    vals = Vector{UInt16}(undef, length / sizeof(UInt16))
    readbytes!(io, vals)
    Tag{TypeWideString}(tagid, tagidx, length, strip(transcode(String, vals), '\0'))
end
function readtag(::TypeBinaryBlob, io, tagid, tagidx)
    length = read(io, UInt64)
    vals = read(io, length)
    Tag{TypeBinaryBlob}(tagid, tagidx, length, vals)
end

function readstringtags!(io, tags, string_tags)
    for (tag, len) in string_tags
        tags[tag] = Tag{TypeAsciiString}(tag, -1, len, rstrip(String(read(io, len)), '\0'))
    end
end

function readint32tags!(io, tags, int32_tags)
    for tag in int32_tags
        tags[tag] = Tag{TypeInt8}(tag, -1, read(io, Int32), nothing)
    end
end

function readfloat32tags!(io, tags, float32_tags)
    for tag in float32_tags
        tags[tag] = Tag{TypeFloat8}(tag, -1, read(io, Float32), nothing)
    end
end

function readph20curve(io)
    tags = OrderedDict{String, Tag}()
    int32_tags = ["CurveIndex", "TimeOfRecording"]
    readint32tags!(io, tags, int32_tags)
    string_tags = [("HardwareIdent", 16), ("HardwareVersion", 8)]
    readstringtags!(io, tags, string_tags)
    int32_tags = [
        "HardwareSerial", "SyncDivider", "CFDZeroCross0", "CFDLevel0", 
        "CFDZeroCross1", "CFDLevel1", "Offset", "RoutingChannel", "ExtDevices", 
        "MeasMode", "SubMode",
    ]
    readint32tags!(io, tags, int32_tags)
    float32_tags = ["P1", "P2", "P3"]
    readfloat32tags!(io, tags, float32_tags)
    readint32tags!(io, tags, ["RangeNo"])
    readfloat32tags!(io, tags, ["Resolution"])
    int32_tags = [
        "Channels", "AcquisitionTime", "StopAfter", "StopReason", 
        "InpRate0", "InpRate1", "HistCountRate", 
    ]
    readint32tags!(io, tags, int32_tags)
    tags["IntegralCount"] = Tag{TypeInt8}("IntegralCount", -1, read(io, Int64), nothing)
    int32_tags = [
        "Reserved", "DataOffset", "RouterModelCode", "RouterEnabled", "RtCh_InputType", 
        "RtCh_InputLevel", "RtCh_InputEdge", "RtCh_CFDPresent", "RtCh_CFDLevel", 
        "RtCh_CFDZeroCross", 
    ]
    readint32tags!(io, tags, int32_tags)
    return tags
end

function readheadertags!(::PHDFile, io, tags)
    string_tags = [
        ("CreatorName", 18)
        ("CreatorVersion", 12)
        ("FileTime", 18)
        ("CRLF", 2)
        ("Comment", 256)
    ]
    readstringtags!(io, tags, string_tags)
    int32_tags = [
        "NumberOfCurves", "BitsPerRecord", "RoutingChannels", "NumberOfBoards", 
        "ActiveCurve", "MeasurementMode", "SubMode", "RangeNo", "Offset", 
        "AcquisitionTime", "StopAt", "StopOnOvfl", "Restart", "DisplayLinLog", 
        "DisplayTimeAxisFrom", "DisplayTimeAxisTo", "DisplayCountAxisFrom", 
        "DisplayCountAxisTo",
    ]
    readint32tags!(io, tags, int32_tags)
    tags["RoutingChannels"] = Tag{TypeInt8}("RoutingChannels", -1, 4, nothing)
    tags["DisplayCurve"] = [
        OrderedDict([
            "MapTo"=>Tag{TypeInt8}("MapTo", -1, read(io, Int32), nothing), 
            "Show"=>Tag{TypeBool8}("Show", -1, read(io, Int32) ≠ 0, nothing)])
    for _ in 1:8]
    tags["Param"] = [
        OrderedDict([
            "Start"=>Tag{TypeFloat8}("Start", -1, read(io, Float32), nothing), 
            "Step"=>Tag{TypeFloat8}("Step", -1, read(io, Float32), nothing), 
            "Stop"=>Tag{TypeFloat8}("Stop", -1, read(io, Float32), nothing)
        ])
    for _ in 1:3]
    int32_tags = [
        "RepeatMode",
        "RepeatsPerCurve",
        "RepeatTime",
        "RepeatWaitTime",
    ]
    readint32tags!(io, tags, int32_tags)
    tags["ScriptName"] = Tag{TypeAsciiString}("ScriptName", 0, 20, rstrip(String(read(io, 20)), '\0'))
    boards = []
    for _ in 1:tags["NumberOfBoards"].value
        board = OrderedDict{String, Union{Tag, OrderedDict, Vector}}()
        board["HardwareIdent"] = Tag{TypeAsciiString}("HardwareIdent", -1, 16, rstrip(String(read(io, 16)), '\0'))
        board["HardwareVersion"] = Tag{TypeAsciiString}("HardwareIdent", -1, 8, rstrip(String(read(io, 8)), '\0'))
        int32_tags = [
            "HardwareSerial", "SyncDivider", "CFDZeroCross0",
            "CFDLevel0", "CFDZeroCross1", "CFDLevel1",
        ]
        readint32tags!(io, board, int32_tags)
        board["Resolution"] = Tag{TypeFloat8}("Resolution", -1, read(io, Float32), nothing)
        board["RouterModelCode"] = Tag{TypeInt8}("RouterModelCode", -1, read(io, Int32), nothing)
        board["RouterEnabled"] = Tag{TypeInt8}("RouterEnabled", -1, read(io, Int32), nothing)
        int32_tags = [
            "InputType", "InputLevel", "InputEdge",
            "CFDPresent", "CFDLevel", "CFDZCross",
        ]
        board["RouterChannel"] = [
            OrderedDict([
                tag => Tag{TypeInt8}(tag, -1, read(io, Int32), nothing)
                for tag in int32_tags]) 
            for _ in 1:tags["RoutingChannels"].value
        ]
        push!(boards, board)
    end
    tags["Board"] = boards
    if tags["MeasurementMode"].value ≠ 0 # InteractiveMode
        error("Unhandled measurement mode $(tags["MeasurementMode"]). TTTR files have not been implemented yet.")
    end
    @debug "Position before Curve" position(io)
    tags["Curve"] = [readph20curve(io) for _ in 1:tags["NumberOfCurves"].value]
    return nothing
end

function readheadertags!(::PHUFile, io, tags)
    tag = readtag(io)
    tags[tag.id] = tag
    while !eof(io) && tag.id != "Header_End"
        tag = readtag(io)
        tags[tag.id] = tag
    end
    return nothing
end

struct Header
    magic::String
    version::String
    tags::OrderedDict{String, Union{Tag, OrderedDict, Vector}}
end

function Base.show(io::IO, h::Header)
    print(io, "Header(", h.magic, ", ", h.version, ")\n\tTags:\n")
    for (_, t) in collect(h.tags)
        print(io, "\t\t", t, "\n")
    end
end

function readheader(io)
    seekstart(io)
    magic = rstrip(String(read(io, 8)), '\0')
    version = ""
    versionlength = 8
    isphd = magic == "PicoHarp"
    if isphd 
        magic = magic * rstrip(String(read(io, 8)), '\0')
        seek(io, 16)
        versionlength = 6
    end
    version = strip(String(read(io, versionlength)), '\0')
    tags = OrderedDict{String, Union{Tag, OrderedDict, Vector}}()
    if isphd
        readheadertags!(PHDFile(), io, tags)
    else
        readheadertags!(PHUFile(), io, tags)
    end
    Header(magic, version, tags)
end

Base.iterate(iter::Header) = iterate(iter.tags)
Base.iterate(iter::Header, state) = iterate(iter.tags, state)
Base.length(iter::Header) = length(iter.tags)
function Base.getindex(h::Header, i)
    elem = getindex(h.tags, i)
    if elem isa Tag
        return elem.value
    else
        return elem
    end
end

function readhisto(io, header)
    if startswith(header.magic, "PicoHarp")
        readhisto(PHDFile(), io, header)
    else
        readhisto(PHUFile(), io, header)
    end
end

function readhisto(::PHUFile, io, header)
    N = header["HistResDscr_HistogramBins"]
    start = header["HistResDscr_DataOffset"]
    data = zeros(Int32, N)
    seek(io, start)
    read!(io, data)
    data
end

function readhisto(::PHDFile, io, header)
    numberofrows = collect(map(x->x["Channels"].value, header["Curve"]))
    numberofdatarows = maximum(numberofrows)
    counts = Array{Int32, 2}(undef, numberofdatarows, header["NumberOfCurves"])
    for (i, rows) in enumerate(numberofrows)
        @debug "Reading data" i rows
        read!(io, @view(counts[1:rows, i]))
    end
    counts = Array{Union{Int32, Missing}, 2}(counts)
    for (i, rows) in enumerate(numberofrows)
        counts[rows+1:end, i] .= missing
    end
    return counts
end

struct PHisto
    header::Header
    histo::AbstractArray{Union{Int32, Missing}}
end

Base.length(histo::PHisto) = length(histo.histo)
dt(histo::PHisto) = histo.header["HistResDscr_MDescResolution"]    
time(histo::PHisto) = range(start=0, step=dt(histo), length=length(histo))

function PHisto(io)
    header = readheader(io)
    data = readhisto(io, header)
    PHisto(header, data)
end
PHisto(s::AbstractString) = open(s) do io PHisto(io) end

function Base.show(io::IO, h::PHisto)
    print(io, "PHisto with ", length(h), " bins")
end

# Tables.jl interface
function Tables.columnnames(t::PHisto)
    header = getfield(t, :header)
    if startswith(header.magic, "PicoHarp")
        names = [Symbol("time$i") for i in 1:header["NumberOfCurves"]]
        names = repeat(names, inner=2)
        names[2:2:end] .= [Symbol("count$i") for i in 1:header["NumberOfCurves"]]
        return names
    else
        return [:time, :count]
    end
end
Tables.istable(::Type{PHisto}) = true
function Tables.schema(t::PHisto) 
    names = Tables.columnnames(t)
    Tables.Schema(names, repeat([Union{Float64, Missing}, Union{Int32, Missing}], outer=length(name)÷2))
end
Tables.columnaccess(::Type{PHisto}) = true
Tables.columns(t::PHisto) = t
Tables.getcolumn(t::PHisto, i) = Tables.getColumn(t, Tables.columnnames(t)[i])
function Tables.getcolumn(t::PHisto, nm::Symbol) 
    if nm ∉ Tables.columnnames(t)
        throw(ArgumentError("Column $nm does not exist for this Histogram."))
    end
    if startswith(t.header.magic, "PicoHarp")
        name = string(nm)
        requestingtime = startswith(name, "time")
        if requestingtime
            num = parse(Int, name[5:end])
            nrows = t.header["Curve"][num]["Channels"].value
            resolution = t.header["Curve"][num]["Resolution"].value * 1e-9
            bins = Vector{Union{Float32, Missing}}(range(start=0, length=size(t.histo, 1), step=resolution))
            bins[nrows+1:end] .= missing
            return bins
        else
            num = parse(Int, name[6:end])
            return t.histo[:, num]
        end

    else
        if nm == :time
            time(t)
        else
            t.histo
        end
    end
end
function Base.getproperty(f::PHisto, sym::Symbol)
    try
        Tables.getcolumn(f, sym)
    catch e
        if e isa ArgumentError 
            getfield(f, sym)
        else
            rethrow(e)
        end
    end
end
function Base.propertynames(t::PHisto, private::Bool=false)
    if private 
        [Tables.columnnames(t); fieldnames(PHisto)...]
    else
        [Tables.columnnames(t); :header]
    end
end

export PHisto

end
