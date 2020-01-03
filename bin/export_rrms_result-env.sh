# 导出数据起始日期相对于传入参数日期的间隔，比如设置month=2,传入脚本日期为20191101，那么导出的数据的起始日期就是20190901
# 如果想按天，则写为 day=xxx
# 如果不指定，默认为一个月
month=1

#数据库 host
host=10.204.145.156

# 连接用户名
user=root

# 连接密码
password=root

# 数据库名字
database=test

# 数据库端口号，MySQL必须填写，db2择机而定
port=3306

# 导出文件的列分隔符
delimiter=,

# sql语句
statement="select * from rrms_result where trn_time > date_begin and trn_time < date_end;"
