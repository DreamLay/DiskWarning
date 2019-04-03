
CREATE TABLE cdh_bak_info(
  id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'ID',
  insert_time VARCHAR(20) COMMENT '入库时间',
  expire_time VARCHAR(20) COMMENT '过期时间',
  alert_id VARCHAR(200) COMMENT '告警id',
  alert_bak_path VARCHAR(200) COMMENT '备份告警文件路径',
  jmx_bak_path VARCHAR(200) COMMENT '备份jmx文件路径'
  alert_bak_size VARCHAR(20) COMMENT 'alert文件大小'
  jmx_bak_size VARCHAR(20) COMMENT 'jmx文件大小'
) COMMENT='cdh告警信息备份'


CREATE TABLE cdh_host(
  id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'ID',
  hostname VARCHAR(20) COMMENT '主机名',
  ip VARCHAR(20) COMMENT '带外ip',
  work_ip VARCHAR(20) COMMENT '业务ip'
) COMMENT='cdhIp对应关系'