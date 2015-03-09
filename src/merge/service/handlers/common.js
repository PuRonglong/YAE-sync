var async = require('async');
var fs = require('fs');
var _ = require('underscore');
var sqlHelper = require(FRAMEWORKPATH + "/db/sqlHelper");

exports._addData2Mysql = _addData2Mysql;

function _addData2Mysql(dataList, callback) {
    var sqlList = [];
    var allData = [];

    var insertData = [];
    var updateData = [];
    var deleteMetaList = [];

    //过滤"sqlList"和"data"
    _.each(dataList, function(item){
        _.each(item, function(value, key){
            if(key === "allSqlList"){
                sqlList = value;
            }else if(key == "data"){
                allData = value;
            }
        });
    });

    //在data上面过滤"insert","update"和"delete"
    _.each(allData, function(item){
        _.each(item, function(value, key){
            if(key === "insert"){
                insertData = value;
            }else if(key === "update"){
                updateData = value;
            }else if(key === "delete"){
                deleteMetaList = value;
            }
        });
    });

    //构建"insert","update"和"delete"的sql
    _handleInsertData(insertData);
    _handleUpdateData(updateData);
    _handleDeleteData(deleteMetaList);

    function _handleInsertData(insertData) {
        _.each(insertData, function (item) {
            if(!_.isEmpty(item)){
                sqlList.push(sqlHelper.getServerInsertForMysql("planx_graph", item.table, item.data, null, true));
            }
        });
    }

    function _handleUpdateData(updateData) {
        _.each(updateData, function(item){
            if(!_.isEmpty(item)){
                sqlList.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", item.table, item.data));
            }
        });
    }

    function _handleDeleteData(deleteMetaList) {
        _.each(deleteMetaList, function(item){
            if(!_.isEmpty(item)){
                sqlList.push(sqlHelper.getServerDelForMysql("planx_graph", item.table_name, item.entity_id));
            }
        });
    }

    callback(null, sqlList);
}
