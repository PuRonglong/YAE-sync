var dbHelper = require(FRAMEWORKPATH + "/utils/dbHelper");
var oss = require(FRAMEWORKPATH + "/utils/ossClient");
var libsync = require("./libsync");
var fs = require("fs");
var async = require("async");

exports.checkDeviceId = checkDeviceId;
exports.checkChunk = checkChunk;
exports.downloadChunk = downloadChunk;

function checkDeviceId(req, res, next){

    var deviceId = req.headers["x-deviceid"];
    var enterpriseId = req.headers["x-enterpriseid"];

    var sql = "select count(1) as count" +
        " from planx_graph.tb_userlogincounts a, planx_graph.tb_enterprise b" +
        " where a.username = b.admin_account and b.id = :enterprise_id and a.deviceId = :device_id";

    dbHelper.execSql(sql, {enterprise_id: enterpriseId, device_id: deviceId}, function(err, result){

        if(err){
            console.log(err);
            next(err);
            return;
        }

        // 设备ID不一致，这可能是解锁设备造成的
        if(result[0].count === 0){
            console.log("备份接口异常调用，没有找到登陆设备，或登陆设备不一致");
            doResponse(req, res, {code:1, error:{errorCode: 400, errorMessage: "没有找到登陆设备，或登陆设备不一致"}});
            return;
        }

        doResponse(req, res, {message: "ok"});
    });
}

function checkChunk(req, res, next){

    var enterpriseId = req.headers["x-enterpriseid"];

    dbHelper.queryData("new_backup_history", {"enterprise_id": enterpriseId}, function(err, result){

        if(err){
            console.log(err);
            next(err);
            return;
        }

        if(result.length > 0){
            doResponse(req, res, {flag: 1});
        }else{
            doResponse(req, res, {flag: 0});
        }
    });
}

function downloadChunk(req, res, next){

    var ossPath;
    var localPath;
    var chunkPath;

    async.series([_resolveOssPath, _fetchRdbFromOss, _doChunk], function(err){

        if(err){
            console.log(err);
            next(err);
            return;
        }

        res.download(chunkPath, "test.png", function(err){

            if(err){
                console.log("下载chunk文件失败");
                console.log(err);
            }

            _cleanChunkFile();
        });
    });

    function _resolveOssPath(callback){

        var enterpriseId = req.headers["x-enterpriseid"];

        var sql = "select * from planx_graph.new_backup_history where enterprise_id = :enterprise_id order by id desc limit 0, 1";

        dbHelper.execSql(sql, {enterprise_id: enterpriseId}, function(err, result){

            if(err){
                console.log("查询oss路径失败");
                callback(err);
                return;
            }

            if(result.length === 0){
                callback({errorMessage: "找不到rdb文件"});
                return;
            }

            ossPath = result[0].oss_path;
            callback(null);
        });
    }

    function _fetchRdbFromOss(callback){

        localPath = global.appdir + "data/" + ossPath;

        oss.getObject("new-backup", ossPath, localPath, function(err) {

            if(err){
                console.log("从OSS获取文件失败");
                callback(err);
                return;
            }

            callback(null);
        });
    }

    function _doChunk(callback){

        chunkPath = localPath + ".chunk";

        libsync.file_chunk(localPath, chunkPath, 0, function (err, flag) {

            if(err){

                console.log("调用libsync库失败");
                callback(err);
                _cleanRdbFile();
                return;
            }

            if(flag === -1){

                callback({errorMessage: "调用file_chunk失败"});
                _cleanRdbFile();
                return;
            }

            _cleanRdbFile();
            callback(null);
        });
    }

    function _cleanRdbFile(){

        fs.unlink(localPath, function(err){

            if(err){
                console.log("删除rdb文件失败: " + localPath);
            }
        });
    }

    function _cleanChunkFile(){

        fs.unlink(chunkPath, function(err){

            if(err){
                console.log("删除chunk文件失败: " + chunkPath);
            }
        });
    }
}