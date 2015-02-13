var dbHelper = require(FRAMEWORKPATH + "/utils/dbHelper");
var oss = require(FRAMEWORKPATH + "/utils/ossClient");
var libsync = require("./libsync");
var fs = require("fs");
var async = require("async");

exports.checkDeviceId = checkDeviceId;
exports.checkChunk = checkChunk;
exports.downloadChunk = downloadChunk;
exports.uploadDeltaOrRdb = uploadDeltaOrRdb;
exports.downloadRdb = downloadRdb;

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

    var enterpriseId = req.headers["x-enterpriseid"];

    _fetchLatestRdbFromOss(enterpriseId, function(err, localRdbPath){

        if(err){
            console.log(err);
            next(err);
            return;
        }

        var chunkPath = localRdbPath + ".chunk";

        libsync.file_chunk(localRdbPath, chunkPath, 0, function (err, flag) {

            _cleanRdbFile(function(){

                if(err){
                    console.log("调用libsync库失败");
                    console.log(err);
                    next(err);
                    return;
                }

                if(flag === -1){
                    console.log({errorMessage: "调用file_chunk失败"});
                    next({errorMessage: "调用file_chunk失败"});
                    return;
                }

                res.download(chunkPath, "current.chunk", function(err){

                    if(err){
                        console.log("下载chunk文件失败");
                        console.log(err);
                    }

                    _cleanChunkFile();
                });
            });
        });

        function _cleanRdbFile(callback){

            fs.unlink(localRdbPath, function(err){

                if(err){
                    console.log("删除rdb文件失败: " + localRdbPath);
                }

                callback();
            });
        }

        function _cleanChunkFile(){

            fs.unlink(chunkPath, function(err){

                if(err){
                    console.log("删除chunk文件失败: " + chunkPath);
                }
            });
        }
    });
}

function uploadDeltaOrRdb(req, res, next){

    var deviceId = req.headers["x-deviceid"];
    var enterpriseId = req.headers["x-enterpriseid"];
    var backupType = req.headers["x-backuptype"];// full or chunk

    req.form.on("end", function(){

        var now = new Date().getTime();

        var tmp_path = req.files.file.path;// 上传文件缓存路径
        var uploadPath = global.appdir + "data/uploadTemp/" + req.files.file.name;// 上传的rdb或chunk

        fs.rename(tmp_path, uploadPath, function(err){

            if(err){
                console.log("移动文件失败，上传文件未保存成功: " + uploadPath);
                next(err);
                return;
            }

            if(backupType === "full"){
                _handleRdbFile();
            }else{
                _handleDeltaFile();
            }
        });

        function _handleRdbFile(){

            var ossFileName = now + "_" + req.files.file.name;

            _putOssAndRecord(uploadPath, ossFileName, function(err){

                if(err){
                    next(err);
                    return;
                }

                doResponse(req, res, {message: "ok"});
            });
        }

        function _handleDeltaFile(){

            _fetchLatestRdbFromOss(enterpriseId, function(err, localRdbPath){

                if(err){

                    console.log(err);

                    _cleanDeltaFile(function(){
                        next(err);
                    });

                    return;
                }

                libsync.file_sync(localRdbPath, uploadPath, function(err, flag){

                    if(err){
                        console.log("调用libsync库失败");
                        console.log(err);
                        _cleanDeltaFile(function(){
                            next(err);
                        });
                        return;
                    }

                    if(flag === -1){
                        console.log({errorMessage: "调用file_sync失败"});
                        _cleanDeltaFile(function(){
                            next({errorMessage: "调用file_sync失败"});
                        });
                        return;
                    }

                    // 上面的file_sync如果调用成功，delta会被自动删除
                    var nameExt = req.files.file.name.replace("delta", "rdb");
                    var ossFileName = now + "_" + nameExt;

                    _putOssAndRecord(localRdbPath, ossFileName, function(err){

                        if(err){
                            next(err);
                            return;
                        }

                        doResponse(req, res, {message: "ok"});
                    });
                });

                function _cleanDeltaFile(callback){

                    fs.unlink(uploadPath, function(err){

                        if(err) {
                            console.log("删除delta文件失败: " + uploadPath);
                        }

                        callback();
                    });
                }
            });
        }

        function _putOssAndRecord(localFilePath, ossFileName, callback){

            oss.putNewBackupObjectToOss(ossFileName, localFilePath, function(err){

                fs.unlink(localFilePath, function(err){

                    if(err) {
                        console.log("删除文件失败: " + localFilePath);
                    }

                    if(err){
                        console.log("上传文件到OSS失败");
                        callback(err);
                        return;
                    }

                    var model = {
                        enterprise_id: enterpriseId,
                        device_id: deviceId,
                        oss_path: ossFileName,
                        upload_date: now,
                        merge_done: 0
                    };

                    dbHelper.addData("new_backup_history", model, function(err){

                        if(err){
                            console.log("备份记录写入数据库失败");
                            callback(err);
                            return;
                        }

                        callback(null);
                    });
                });
            });
        }
    });
}

function downloadRdb(req, res, next){

    var enterpriseId = req.headers["x-enterpriseid"];

    _fetchLatestRdbFromOss(enterpriseId, function(err, localRdbPath){

        if(err){
            console.log(err);
            next(err);
            return;
        }

        res.download(localRdbPath, "latest.rdb", function(err){

            if(err){
                console.log("下载rdb文件失败");
                console.log(err);
            }

            _cleanRdbFile();
        });

        function _cleanRdbFile(){

            fs.unlink(localRdbPath, function(err){

                if(err){
                    console.log("删除rdb文件失败: " + localRdbPath);
                }
            });
        }
    });
}

function _fetchLatestRdbFromOss(enterpriseId, callback){

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

        var ossPath = result[0].oss_path;
        var localRdbPath = global.appdir + "data/" + ossPath;

        oss.getObject("new-backup", ossPath, localRdbPath, function(err) {

            if(err){
                console.log("从OSS获取文件失败");
                callback(err);
                return;
            }

            callback(null, localRdbPath);
        });
    });
}