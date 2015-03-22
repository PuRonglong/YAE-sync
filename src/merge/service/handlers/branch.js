var _ = require('underscore');
var sqlHelper = require(FRAMEWORKPATH + "/db/sqlHelper");

exports._buildMemberInfoSql = _buildMemberInfoSql;

function _buildMemberInfoSql(dataList, callback) {
    var sqlList = [];
    var branchInsertData = [];
    var branchTableList = ['tb_serviceBill', 'tb_billAttrMap', 'tb_billProject', 'tb_rechargeMemberBill'];   //跨店充值和跨店消费

    var otherInsertData = [];  //储存非跨店的数据
    var otherUpdateData = [];
    var otherDeleteData = [];
    var otherData = [];

    var allOtherData = [];  //返回的数据格式[{"allSqlList": [{statement: [], value: {}}], "data": [{"insert": [{table:"tb_serviceBill", data: [...]}, {}, {}]}, {}, {}]}]

    var updateBranchSql = "update planx_graph.tb_membercardattrmap " +
        "set value = :value" + ", modify_date = :modify_date " +
        "where memberCardId = :memberCardId;";

    if(_.isEmpty(dataList)){
        return;
    }

    //过滤"insert"的所有数据
    _.each(dataList, function(item){
        _.each(item, function(value, key){
            if(key === "insert"){
                branchInsertData = branchInsertData.concat(value);
            }else if(key === "update"){
                otherUpdateData = otherUpdateData.concat(value);
            }else if(key === "delete"){
                otherDeleteData = otherDeleteData.concat(value);
            }
        });
    });

    //过滤跨店需要的数据
    var branchList = _.filter(branchInsertData, function(item){
        return _.contains(branchTableList, item.table) && item.data.status === 9;
    });
    var billList = _.filter(branchList, function (item) {
        return item.table === 'tb_serviceBill';
    });
    var branchRecharge = _.filter(branchList, function(item){
        return item.table === 'tb_rechargeMemberBill';
    });

    //过滤非跨店消费的"insert"的数据
    var tempData = _.reject(branchInsertData, function(item){
        return _.contains(branchTableList, item.table) && item.data.status === 9;
    });
    otherInsertData = otherInsertData.concat(tempData);

    //跨店充值
    _.each(branchRecharge, function(item){
        sqlList = sqlList.concat(_buildRechargeMember(item));
        sqlList = sqlList.concat(_buildRechargeMemberCard(item));
    });

    function _buildRechargeMember(branch){
        var rechargeMember = [];

        if(!_.isEmpty(branch)){
            var updateRechargeMember = {
                id: branch.data.member_id,
                modify_date: branch.data.create_date,
                currentScore: branch.data.currentScore
            };

            rechargeMember.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", "tb_member", updateRechargeMember));
        }

        return rechargeMember;
    }

    function _buildRechargeMemberCard(branch){
        var rechargeMemberCard = [];

        if(!_.isEmpty(branch)){
            var updateRechargeMemberCard = {
                id: branch.data.memberCard_id,
                modify_date: branch.data.create_date,
                currentMoney: Number(branch.data.amount + branch.data.presentMoney)
            };

            var rechargeMemberCardSql = "update planx_graph.tb_membercard " +
                "set currentMoney = currentMoney + :currentMoney" + ", modify_date = :modify_date " +
                "where id = :id;";
            rechargeMemberCard.push({statement: rechargeMemberCardSql, value: updateRechargeMemberCard});
        }

        return rechargeMemberCard;
    }

    _.each(billList, function (item) {
        sqlList = sqlList.concat(_buildRechargeCard(item));    //更新充值卡
        sqlList = sqlList.concat(_buildRecordCard(item));    //更新计次卡
        sqlList = sqlList.concat(_buildQuarterCard(item));    //更新年卡/季卡
        sqlList = sqlList.concat(_buildPresent(item));    //更新赠送服务
        sqlList = sqlList.concat(_buildCoupon(item));    //更新现金卷
        sqlList = sqlList.concat(_buildMember(item));    //更新会员表信息
    });

    //将"status = 9"改成"status = 4"
    if(!_.isEmpty(branchList)){
        _.each(branchList, function(item){
            if(item.data.status === 9){
                item.data.status = 4;
            }

            sqlList.push(sqlHelper.getServerInsertForMysql("planx_graph", item.table, item.data, null, true));
        });
    }

    //返回数据构建
    otherData.push({insert: otherInsertData}, {update: otherUpdateData}, {delete: otherDeleteData});
    allOtherData.push({allSqlList: sqlList}, {data: otherData});
    callback(null, allOtherData);

    function _buildRechargeCard(bill) {
        var rechargeSql = [];

        var balanceSnapshot = _.find(branchList, function (item) {
            return item.table === "tb_billAttrMap" && item.data.groupName ===  "rechargeBalanceSnapshot" && item.data.billId === bill.data.id;
        });

        if (!_.isEmpty(balanceSnapshot)) {
            var updateModel = {
                id: balanceSnapshot.data.memberCardId,
                currentMoney: Number(balanceSnapshot.data.value),
                modify_date: balanceSnapshot.data.create_date
            };

            rechargeSql.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", "tb_membercard", updateModel))  //拼接sql，调用YAE-SERVICE的函数
        }

        return rechargeSql;
    }

    function _buildRecordCard(bill) {
        var recordCardBillSql = [];
        var recordCardPaySql = [];

        var billMemBalance = _.find(branchList, function(item){
            return item.table === "tb_billAttrMap" && item.data.groupName === "billMemBalance" && item.data.billId === bill.data.id;
        });

        var payment = _.find(branchList, function(item){
            return item.table === "tb_billAttrMap" && item.data.groupName === "payment" && item.data.keyName === "record" && item.data.billId === bill.data.id;
        });

        if(!_.isEmpty(billMemBalance)){
            var updateModelBill = {
                memberCardId:billMemBalance.data.memberCardId,
                value:Number(billMemBalance.data.value),
                modify_date:billMemBalance.data.create_date
            }

            recordCardBillSql.push({statement: updateBranchSql, value: updateModelBill});
        }

        //更新计次卡的tb_memberCard需要计算currentMoney = currentMoney - def_int1
        if(!_.isEmpty(payment)){
            var updateModelPay = {
                id: payment.data.memberCardId,
                def_int1:Number(payment.data.def_int1),
                modify_date:payment.data.create_date
            }

            var PaySql = "update planx_graph.tb_membercard " +
                "set currentMoney = currentMoney - :def_int1" + ", modify_date = :modify_date " +
                "where id = :id;";
            recordCardPaySql.push({statement: PaySql, value: updateModelPay});
        }

        return recordCardBillSql.concat(recordCardPaySql);
    }

    function _buildQuarterCard(bill) {
        var quarterCardSql = [];

        var paymentQuarter = _.filter(branchList, function(item){
            return item.table === "tb_billAttrMap" && item.data.groupName === "payment" && item.data.keyName === "quarter" && item.data.billId === bill.data.id;
        });

        if(!_.isEmpty(paymentQuarter)){
            var updateModel = {
                memberCardId: paymentQuarter.data.memberCard_id,
                value: Number(paymentQuarter.data.value),
                modify_date: paymentQuarter.data.create_date
            };

            quarterCardSql.push({statement: updateBranchSql, value: updateModel});
        }

        return quarterCardSql;
    }

    function _buildPresent(bill) {
        var presentSql = [];

        var presentBalanceSnapshot = _.filter(branchList, function(item){
            return item.table === "tb_billAttrMap" && item.data.groupName === "presentBalanceSnapshot" && item.data.billId === bill.data.id;
        });

        if(!_.isEmpty(presentBalanceSnapshot)){
            var updateModel = {
                memberCardId: presentBalanceSnapshot.data.memberCardId,
                value: Number(presentBalanceSnapshot.data.value),
                modify_date: presentBalanceSnapshot.data.modify_date
            };

            presentSql.push({statement: updateBranchSql, value: updateModel});
        }
        return presentSql;
    }

    function _buildCoupon(bill) {
        var couponSql = [];

        var paymentCoupon = _.filter(branchList, function(item){
            return item.table === "tb_billAttrMap" && item.data.groupName === "payment" && item.data.keyName === "coupon" && item.data.billId === bill.data.id;
        });

        if(!_.isEmpty(paymentCoupon)){
            var updateModel = {
                memberCardId: paymentCoupon.data.memberCardId,
                value: "used",
                modify_date: paymentCoupon.data.modify_date
            };

            couponSql.push({statement: updateBranchSql, value: updateModel});
        }

        return couponSql;
    }

    function _buildMember(bill){
        var member = [];

        if(!_.isEmpty(bill)){
            var updateModel = {
                id: bill.data.member_id,
                currentScore: bill.data.currentScore,
                modify_date: bill.data.create_date
            }

            member.push(sqlHelper.getServerUpdateSqlOfObjId("planx_graph", "tb_member", updateModel));
        }

        return member;
    }
}