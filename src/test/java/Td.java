
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;


import java.io.File;

public class Td {

    public static void main(String[] args){
            File file = new File("D:\\2-MyTemp\\panand.jpg");
            PostMethod filePost = new PostMethod("http://ivk01.gtmap.cn/surveyplat-proof-api-file/uploadSqliteFile");
            HttpClient client = new HttpClient();
            try {
                Part[] parts = { new FilePart("file", file) };
                filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));
                client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
                int status = client.executeMethod(filePost);
                if (status == 200) {
                    System.out.println("上传成功");
                } else {
                    System.out.println("上传失败");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                filePost.releaseConnection();
            }

    }

}
