dt <- "16-Apr-16"
stat <- "Cancelled"
query1 <- paste0("select * from file where date = '",dt,"' and status != '", stat,"'")
query2 <- paste0("select * from file where status != '", stat,"'")
#tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = query1)
tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = query2)
tran$date <- as.Date(tran$date,format="%d-%B-%y")

