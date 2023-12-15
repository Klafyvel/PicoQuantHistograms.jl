module PicoQuantHistograms

using OrderedCollections
using Tables

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

struct Header
    magic::String
    version::String
    tags::OrderedDict{String, Tag}
end

function Base.show(io::IO, h::Header)
    print(io, "Header(", h.magic, ", ", h.version, ")\n\tTags:\n")
    for (_, t) in collect(h.tags)
        print(io, "\t\t", t, "\n")
    end
end

function readheader(io)
    seekstart(io)
    magic = strip(String(read(io, 8)), '\0')
    version = strip(String(read(io, 8)), '\0')
    tags = OrderedDict{String, Tag}()
    tag = readtag(io)
    tags[tag.id] = tag
    while !eof(io) && tag.id != "Header_End"
        tag = readtag(io)
        tags[tag.id] = tag
    end
    Header(magic, version, tags)
end

Base.iterate(iter::Header) = iterate(iter.tags)
Base.iterate(iter::Header, state) = iterate(iter.tags, state)
Base.length(iter::Header) = length(iter.tags)
Base.getindex(h::Header, i) = getindex(h.tags, i).value

function readhisto(io, header)
    N = header["HistResDscr_HistogramBins"]
    start = header["HistResDscr_DataOffset"]
    data = zeros(Int32, N)
    seek(io, start)
    read!(io, data)
    data
end

struct PHisto
    header::Header
    histo::Vector{Int32}
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
Tables.columnnames(::PHisto) = [:time, :count]
Tables.istable(::Type{PHisto}) = true
Tables.schema(t::PHisto) = Tables.Schema(Tables.columnnames(t), [Float64, Int32])
Tables.columnaccess(::Type{PHisto}) = true
Tables.columns(t::PHisto) = t
Tables.getcolumn(t::PHisto, i) = Tables.getColumn(t, Tables.columnnames(t)[i])
function Tables.getcolumn(t::PHisto, nm::Symbol) 
    if nm ∉ Tables.columnnames(t)
        throw(ArgumentError("Column $nm does not exist for this TTR."))
    end
    if nm == :time
        time(t)
    else
        t.histo
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
