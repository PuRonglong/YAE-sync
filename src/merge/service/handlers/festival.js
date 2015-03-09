var _ = require("underscore");
var sqlHelper = require(FRAMEWORKPATH + "/db/sqlHelper");

exports.festival_present = festival_present;

function festival_present(dataList, callback){
    var sqlList = [];
    var allData = [];
    var festivals_table = ["weixin_festivals", "weixin_present_received"];

    var sql = [];
    var festivals_data = [];

    var otherData = [];
    var otherInsertData = [];
    var otherDeleteData = [];
    var allOtherData = [];

    //过滤"allSqlList"和"data"
    _.each(dataList, function(item){
        _.each(item, function(value, key){
            if(key === "allSqlList"){
                sqlList = value;
            }else if(key === "data"){
                allData = value;
            }
        });
    });

    _.each(allData, function(item){
        _.each(item, function(value, key){
            if(key === "insert"){
                otherInsertData = otherInsertData.concat(value);
            }else if(key === "update"){
                festivals_data = festivals_data.concat(value);
            }else if(key === "delete"){
                otherDeleteData = otherDeleteData.concat(value);
            }
        });
    });

    //过滤优惠券所需的数据
    var weixin_festivals = _.filter(festivals_data, function(item){
        return _.contains(["weixin_festivals"], item.table);
    });
    var weixin_present_received = _.filter(festivals_data, function(item){
        return _.contains(["weixin_present_received"], item.table);
    });

    //需要返回的数据
    var otherUpdateData = _.reject(festivals_data, function(item){
        return _.contains(festivals_table, item.table);
    });

    if(!_.isEmpty(weixin_festivals)){
        _.each(weixin_festivals, function(item){
            var festivals_updateModel = {
                id: item.data.id,
                turnover_grow: item.data.turnover_grow
            };

            sql.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", item.table, festivals_updateModel));
        });
    }

    if(!_.isEmpty(weixin_present_received)){
        _.each(weixin_present_received, function(item){
            var presents_updateModel = {
                id: item.data.id,
                consume_state: item.data.consume_state,
                festival_consume: item.data.festival_consume
            };

            sql.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", item.table, presents_updateModel));
        });
    }

    //构建需要返回的数据
    sqlList = sqlList.concat(sql);
    otherData.push({insert: otherInsertData}, {update: otherUpdateData}, {delete: otherDeleteData});

    allOtherData.push({allSqlList: sqlList}, {data: otherData});
    callback(null, allOtherData);
}