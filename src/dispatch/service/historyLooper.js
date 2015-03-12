var dbHelper = require(FRAMEWORKPATH + "/utils/dbHelper");
var datas = new (require(FRAMEWORKPATH + "/bus/request"))();
var logger = require(FRAMEWORKPATH + "/utils/logger").getLogger();

var TIME_INTERVAL = 5 * 1000;

exports.start = start;

function start(){

    setTimeout(_loop, TIME_INTERVAL);

    //setInterval(_loop, TIME_INTERVAL);
}

function _loop(){

    var merge_data = [];

    logger.info("开始轮询new_backup_history表");

    async.series([_handleRdbData, _postRdbData], function(err){

        if(err){
            logger.error(err);
        }
    });

    function _handleRdbData(callback) {

        var enterpriseIdList = [];
        var enterpriseDataList = [];
        var deliver_data = [];

        async.series([_bulidEnterpriseId, _buildBackupRdb, _filterData], function(err, result){

            if(err){
                callback(err);
                return;
            }

            callback(err, result[2]);
        });

        function _bulidEnterpriseId(callback){

            var sqlFind = "select enterprise_id from planx_graph.new_backup_history group by enterprise_id;";

            dbHelper.execSql(sqlFind, [], function(err, result){

                if(err){
                    callback(err);
                    return;
                }

                enterpriseIdList = result;
                callback(null);
            });
        }

        function _buildBackupRdb(callback){

            var sqlGet = "select * from planx_graph.new_backup_history where merge_done = 0 or merge_done = 1 order by upload_date;";

            dbHelper.execSql(sqlGet, [], function(err, result){

                if(err){
                    callback(err);
                    return;
                }

                enterpriseDataList = result;
                _buildData();
                callback(null);
            });

            function _buildData(){

                if(_.isEmpty(enterpriseIdList) || _.isEmpty(enterpriseDataList)){
                    return;
                }

                var enterpriseData = [];

                _.each(enterpriseIdList, function(item){
                    _.each(enterpriseDataList, function(result){
                        if(result.enterprise_id === item.enterprise_id){
                            enterpriseData  = enterpriseData.concat(result);
                        }
                    });

                    deliver_data.push({enterprise_id: item.enterprise_id, data: enterpriseData});
                    enterpriseData = [];
                });
            }
        }

        //状态：merge_done为1 是比较merge_date和当前的时间如果大于1小时则重新发
        //merge_done:0 = 未处理, 1 = 正在处理， 2 = 处理成功， 3 = 处理失败
        function _filterData(callback){

            _.each(deliver_data, function(item){

                _.each(item.data, function(value, key){

                    if(key !== 0){
                        return;
                    }

                    if(value.merge_done === 0){
                        merge_data.push(value);
                        return;
                    }

                    if(value.merge_done === 1){

                        var now = new Date().getTime();

                        if((now - value.merge_date) >= (60 * 60 * 1000)){
                            merge_data.push(value);
                        }
                    }
                });
            });

            callback(null);
        }
    }

    function _postRdbData(callback){

        datas.postResource("newsynchandler/handleRdb", "", {enterprise_rdbData: merge_data}).then(function(){
            callback(null);
        }, function(err) {
            callback(err);
        });
    }
}

