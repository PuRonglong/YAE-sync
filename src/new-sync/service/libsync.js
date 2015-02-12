var ffi = require("ffi");

var lib = ffi.Library('./libsync', {
    'file_chunk': ['int', ['string', 'string', 'int']],
    'file_delta': ['int', ['string', 'string', 'string', 'int']],
    'file_sync': ['int', ['string', 'string']]
});

exports.file_chunk = file_chunk;
exports.file_delta = file_delta;
exports.file_sync = file_sync;

// callback(err, result)
function file_chunk(src, chunk, algo, callback){
    lib.file_chunk.async(src, chunk, algo, callback);
}

function file_delta(src, chunk, delta, algo, callback){
    lib.file_delta.async(src, chunk, delta, algo, callback);
}

function file_sync(src, delta, callback){
    lib.file_sync.async(src, delta, callback);
}