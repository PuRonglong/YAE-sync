var async = require("async");
var _ = require("underscore");
var dbHelper = require(FRAMEWORKPATH + "/utils/dbHelper");
var logger = require(FRAMEWORKPATH + "/utils/logger").getLogger();

exports.sync = sync;

function sync(req, res, next){

    var enterpriseId = req.params["enterpriseId"];
    var lastSyncStr = req.query["lastSyncTime"] || "0";
    var lastSyncTime = parseInt(lastSyncStr);

    var consumeDatas = [];
    var rechargeDatas = [];
    var thisSyncTime = lastSyncTime;

    async.series([_queryConsume, _queryRecharge, _judgeSyncTime], function(err){

        if(err){
            console.log(err);
            logger.error(err);
            next(err);
            return;
        }

        var result = {
            syncTime: thisSyncTime,
            consumeDatas: consumeDatas,
            rechargeDatas: rechargeDatas
        };

        doResponse(req, res, result);
    });

    function _queryConsume(callback){

        var sql = "select * from planx_graph.tb_servicebill where status = 4 and enterprise_id = :enterprise_id and create_date > :sync_date;";

        dbHelper.execSql(sql, {enterprise_id: enterpriseId, sync_date: lastSyncTime}, function(err, results){

            if(err){
                callback(err);
                return;
            }

            async.each(results, function(serviceBill, next){

                var consumeData = {};
                consumeData.serviceBill = serviceBill;
                consumeData.projectList = [];
                consumeData.billAttrList = [];
                consumeData.paymentList = [];

                async.series([_queryProject, _queryBillAttrMap, _queryPaymentDetail], function(err){

                    if(err){
                        next(err);
                        return;
                    }

                    consumeDatas.push(consumeData);
                    next();
                });

                function _queryProject(callback){

                    var sql = "select * from planx_graph.tb_billproject where enterprise_id = :enterprise_id and serviceBill_id = :servicebill_id;";

                    dbHelper.execSql(sql, {enterprise_id: enterpriseId, servicebill_id: serviceBill.id}, function(err, results){

                        if(err){
                            callback(err);
                            return;
                        }

                        _.each(results, function(item){
                            consumeData.projectList.push(item);
                        });

                        callback(null);
                    });
                }

                function _queryBillAttrMap(callback){

                    var sql = "select * from planx_graph.tb_billAttrMap where enterprise_id = :enterprise_id and billId = :servicebill_id;";

                    dbHelper.execSql(sql, {enterprise_id: enterpriseId, servicebill_id: serviceBill.id}, function(err, results){

                        if(err){
                            callback(err);
                            return;
                        }

                        _.each(results, function(item){
                            consumeData.billAttrList.push(item);
                        });

                        callback(null);
                    });
                }

                function _queryPaymentDetail(callback){

                    var sql = "select * from planx_graph.tb_paymentDetail where enterprise_id = :enterprise_id and serviceBill_id = :servicebill_id;";

                    dbHelper.execSql(sql, {enterprise_id: enterpriseId, servicebill_id: serviceBill.id}, function(err, results){

                        if(err){
                            callback(err);
                            return;
                        }

                        _.each(results, function(item){
                            consumeData.paymentList.push(item);
                        });

                        callback(null);
                    });
                }

            }, callback);
        });
    }

    function _queryRecharge(callback){

        var sql = "select * from planx_graph.tb_rechargememberbill where status = 4 and enterprise_id = :enterprise_id and create_date > :sync_date;";

        dbHelper.execSql(sql, {enterprise_id: enterpriseId, sync_date: lastSyncTime}, function(err, results){

            if(err){
                callback(err);
                return;
            }

            _.each(results, function(item){
                rechargeDatas.push(item);
            });

            callback(null);
        });
    }

    function _judgeSyncTime(callback){

        // 没有充值和消费流水，则原样返回
        if(consumeDatas.length === 0 && rechargeDatas.length === 0){
            callback(null);
            return;
        }

        var bills = consumeDatas.concat(rechargeDatas);

        var dateArray = _.map(bills, function(bill){

            if(bill.modify_date){
                return bill.modify_date;
            }

            return bill.create_date;
        });

        var sorted = _.sortBy(dateArray, function(item){
            return item;
        });

        thisSyncTime = _.last(sorted);

        callback(null);
    }
}