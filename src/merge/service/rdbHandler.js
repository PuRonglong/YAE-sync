var _ = require("underscore");
var async = require("async");
var fs = require("fs");
var sqlite3 = require("sqlite3").verbose();
var dbHelper = require(FRAMEWORKPATH + "/utils/dbHelper");
var oss = require(FRAMEWORKPATH + "/utils/ossClient");
var logger = require(FRAMEWORKPATH + "/utils/logger").getLogger();

var branch = require("./handlers/branch.js");
var common = require("./handlers/common.js");
var festival = require("./handlers/festival.js");

exports.handleRdb2Mysql =  handleRdb2Mysql;

function handleRdb2Mysql(req, res, next){

    doResponse(req, res, {message: "ok"});

    var rdbDataList = req.body.enterprise_rdbData;
    var now = new Date().getTime();

    async.each(rdbDataList, function(item, next){

        var sql = "update planx_graph.new_backup_history set merge_done = 1, merge_date = :date where id = :id;";

        dbHelper.execSql(sql, {id: item.id, date: now}, function(err){

            if(err){
                next(err);
                return;
            }

            _process(item, next);
        });

    }, function(err){

        if(err) {
            logger.error(err);
        }else{
            logger.info("批量写入mysql成功");
        }
    });

    function _process(rdbRecord, callback){

        var ossPath = "";
        var localRdbPath = "";
        var rdbHelper = "";
        var rdbData = rdbRecord;

        if(_.isEmpty(rdbData)){
            callback({error: "rdb没有需要处理的数据", errorCode: '0001'});
            return;
        }

        var allSqlList = [];

        var metaDataListInsert = [];
        var metaDataListUpdate = [];
        var metaDataListDelete = [];

        var entityDataListInsert = [];
        var entityDataListUpdate= [];

        var entityData = [];
        var tempData = [];

        async.series([_queryOssPath, _queryMetaInsert, _queryInsertData, _queryMetaUpdate, _queryUpdateData, _queryMetaDelete, _buildAllSql, _batchExecSql, _changeMerge_done], function(err){

            if(err){

                var sql = "update planx_graph.new_backup_history set merge_done = 3 where id = :id;";

                dbHelper.execSql(sql, {id: rdbData.id}, function (error) {

                    if (error) {
                        callback({error1: err, error2: error, errorCode: '0002'});
                        return;
                    }

                    callback({error1: err, errorCode: '0003'});
                });
                return;
            }

            rdbHelper.run("delete from tb_modify_data;", []);
            rdbHelper.close();
            callback(null);
        });

        function _queryOssPath(callback){

            ossPath = rdbData.oss_path;

            localRdbPath = global.appdir + "data/oss_cache/" + ossPath;

            oss.getObject("new-backup", ossPath, localRdbPath, function(err) {

                if(err){
                    console.log("从OSS获取文件失败");
                    callback(err);
                    return;
                }

                rdbHelper = new sqlite3.Database(localRdbPath);
                callback(null, localRdbPath);
            });

        }

        function _queryMetaInsert(callback){
            var sql = "select entity_id, table_name " +
                "from tb_modify_data " +
                "where type = 'insert';";

            rdbHelper.all(sql, [], function(err, result){
                if(err){
                    callback(err);
                    return;
                }

                metaDataListInsert = result;
                callback(null);
            });
        }

        function _queryMetaUpdate(callback){
            var sql = "select entity_id, table_name " +
                "from tb_modify_data " +
                "where type = 'update' order by id asc;";

            rdbHelper.all(sql, [], function(err, result){
                if(err){
                    callback(err);
                    return;
                }

                metaDataListUpdate = result;
                callback(null);
            });
        }

        function _queryMetaDelete(callback){
            var sql = "select entity_id, table_name " +
                "from tb_modify_data " +
                "where type = 'delete';";

            rdbHelper.all(sql, [], function(err, result){
                if(err){
                    callback(err);
                    return;
                }

                metaDataListDelete = result;
                callback(null);
            });
        }

        function _queryInsertData(callback){
            async.eachLimit(metaDataListInsert, 10, _queryOne, callback);

            function _queryOne(item, callback){
                var sql = "select * from " + item.table_name +
                    " where id = ? ;";

                rdbHelper.all(sql, [item.entity_id], function(err, result){
                    if(err){
                        callback(err);
                        return;
                    }

                    if(!_.isEmpty(result)){
                        entityDataListInsert.push({table: item.table_name, data: result[0]});
                    }
                    callback(null);
                });
            }
        }

        function _queryUpdateData(callback){
            async.eachLimit(metaDataListUpdate, 10, _queryOne, callback);

            function _queryOne(item, callback){
                var sql = "select * " +
                    " from " + item.table_name +
                    " where id = ? ;";

                rdbHelper.all(sql, [item.entity_id], function(err, result){  //用通配符？来代替直接用item.entity_id，因为直接用会只识别'-'前面的数字，而不是识别整个字符串
                    if(err){
                        callback(err);
                        return;
                    }

                    if(!_.isEmpty(result)){
                        entityDataListUpdate.push({table: item.table_name, data: result[0]});
                    }
                    callback(null);
                });
            }
        }

        function _buildAllSql(callback){
            //[{insert:[{table:tb_serverBill, data:[{id:1, name: "Jashion"}]}]},{update: []}, {delete: [{table: tb_serverBill, data: [{id: 1}]}]}]
            //delete对应的数据只有表名和改条数据的id
            entityData.push({insert: entityDataListInsert}, {update: entityDataListUpdate}, {delete: metaDataListDelete});

            //处理数据逻辑
            //返回的数据格式[{allSqlList: [{statement: "delete from tb_serviceBill where id = :id", value: {}}], "data": [{"insert": [{table:"tb_serviceBill", data: [...]}, {}, {}]}, {}, {}]}]
            async.series([branchEntity, festivals, lastData], function(err){
                if(err){
                    callback(err);
                    return;
                }

                callback(null);
            });

            function branchEntity(callback){
                branch._buildMemberInfoSql(entityData, function(err, result){
                    if(err){
                        callback(err);
                        return;
                    }

                    tempData = result;
                    callback(null);
                });
            }

            function festivals(callback){
                festival.festival_present(tempData, function(err, result){
                    if(err){
                        callback(err);
                        return;
                    }

                    tempData = result;
                    callback(null);
                });
            }

            function lastData(callback){
                common._addData2Mysql(tempData, function(err, result){
                    if(err){
                        callback(err);
                        return;
                    }

                    allSqlList = result;
                    callback(null);
                });
            }
        }

        function _batchExecSql(callback) {
            dbHelper.bacthExecSql(allSqlList, callback);
        }

        function _changeMerge_done(callback){

            var sql = "update planx_graph.new_backup_history set merge_done = 2 where id = :id;";

            dbHelper.execSql(sql, {id: item.id}, callback);
        }
    }
}