public class InitConfReader{
private String confFileUrl:
public Map<String , String>  getConfs(List<String> keys){
Map<String  ,String>result=new HashMap<String, String>();
Properties properties=new Properties();
try{
properties.load(new FileReader(new File(confFileUrl));
}
catch(FileNotFoundException e){
e.printStackTrace();
}
catch£¨IOException e£©{
e.printStackTrace();
}
for(String key:kwys){
String value=(String)properties.get(key);
result.put(key, value);
}
return result;
}
}
