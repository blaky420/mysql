import mysql.connector
mydb=mysql.connector.connect(host="localhost",user="root",password="Root@123",database="kgi_client")
mycursor=mydb.cursor()
mycursor.execute("SELECT symbol, acc_no, price, settlement_currency, exchange, rejection_message, stage, status, placed_at, qty, filled_qty, average_price, order_id, exchange_order_id, order_type, expiry_date, validity,order_action,fis_order_no, fis_exch_order_no,tran_type, updated_at, order_source, user_id, user_type, primary_id, msg_seq_no, build FROM kgi_client.client_order_history WHERE DATE(updated_at) BETWEEN '2021-07-01' AND '2021-11-30' AND stage = 'OPEN' AND order_action like '%' AND symbol like '%' AND acc_no like '0080217' AND exchange like '%' ORDER BY placed_at DESC LIMIT 40 OFFSET 0")
for i in mycursor:
    print(i)
#mycursor.execute("CREATE TABLE client_order_history(symbol INT,acc_no INT,price INT,exchange INT,stage CHAR(20),status FLOAT,placed_at INT,qty INT,average_price FLOAT,order_id INT AUTO_INCREMENT PRIMARY KEY,exchange_order_id INT,order_type CHAR(20) NOT NULL,expiry_date DATE,validity DATE,order_action CHAR(20),fis_order_no INT,fis_exch_order_no INT,tran_type CHAR(20) NOT NULL,updated_at CHAR(20),order_source CHAR(20),user_id INT,user_type CHAR(20),primary_id INT,msg_seq_no INT,build CHAR(20))")
#mycursor.execute("ALTER TABLE client_order_log_history ADD COLUMN order_id INT ANTO_INCREMENT PRIMARY KEY")    

#insert a record in the table
sql="INSERT INTO client_order_history (symbol,acc_no,price,settlement_currency,exchange,rejection_message,stage,status,placed_at,qty,filled_qty,average_price,order_id,exchange_order_id,order_type,expiry_date,validity,order_action,fis_order_no,fis_exch_order_no,tran_type,updated_at,order_source,user_id,user_type,primary_id,msg_seq_no,build) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
values=[('3%',"0080217","12000",'12000',"3%",'yes',"OPEN","good","chennai","90","89","11000","363163001","363163004","electronics","2021-11-30","2021-11-29","90%","01","03","online","2021-07-03","vechicle","0001","male",'00','000',"store"),
       ('2%','0080218','10000','10000','2%','no','OPEN','poor','delhi','100','80','9000','363163002','363163005','furniture','2021-11-30','2021-11-29','95%','02','04','online','2021-07-05','vechicle','0002','female','11','111','store')]
mycursor.execute("SELECT distinct o.*,  a.tran_type, a.exchange FROM  kgi_client.CLIENT_ORDER_LOG_HISTORY o INNER JOIN kgi_client.CLIENT_ORDER_HISTORY a ON o.order_id = a.order_id WHERE o.primary_id = '20211129/0080217/363163001' ORDER BY o.msg_seq_no")
mydb.commit()
print(mycursor.rowcount, "wasinserted.")