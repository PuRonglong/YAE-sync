var fs = require("fs");
var oss = require(FRAMEWORKPATH + "/utils/ossClient");

exports.uploadImageZip = uploadImageZip;

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