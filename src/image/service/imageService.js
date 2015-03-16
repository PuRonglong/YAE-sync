var fs = require("fs");
var oss = require(FRAMEWORKPATH + "/utils/ossClient");

exports.uploadImageZip = uploadImageZip;
exports.downloadImageZip = downloadImageZip;

function uploadImageZip(req, res, next){

    req.form.on("end", function(){

        var fileName = req.files.file.name;// 上传文件名
        var tmp_path = req.files.file.path;// 上传文件缓存路径
        var uploadPath = global.appdir + "data/upload_cache/" + fileName;// 上传zip的本地存储路径

        fs.rename(tmp_path, uploadPath, function(err){

            if(err){
                console.log("移动文件失败，上传文件未保存成功: " + uploadPath);
                next(err);
                return;
            }

            oss.putImageObjectToOss(fileName, uploadPath, function(ossError){

                fs.unlink(uploadPath, function(unlinkError){

                    if(unlinkError){
                        console.log("删除文件失败: " + uploadPath);
                    }

                    if(ossError){
                        console.log("上传文件到OSS失败: " + uploadPath);
                        next(err);
                    }else{
                        doResponse(req, res, {message: "ok"});
                    }
                });
            });
        });
    });
}

function downloadImageZip(req, res, next){

    var enterpriseId = req.headers["x-enterpriseid"];

    var ossPath = enterpriseId + ".zip";
    var localRdbPath = global.appdir + "data/oss_cache/" + ossPath;

    oss.getObject("client-images", ossPath, localRdbPath, function(err) {

        // key不存在时，此处err有值
        if(err){
            console.log("从OSS获取文件失败");
            next(err);
            return;
        }

        res.download(localRdbPath, "images.zip", function(err){

            if(err){
                console.log("下载rdb文件失败");
                console.log(err);
            }

            fs.unlink(localRdbPath, function(unlinkError){

                if(unlinkError){
                    console.log("删除文件失败: " + localRdbPath);
                }
            });
        });
    });
}