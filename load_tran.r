dt <- "16-Apr-16"
stat <- "Cancelled"
query <- paste0("select * from file where date = '",dt,"' and status != '", stat,"'")
tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = query)
tran$date <- as.Date(tran$date,format="%d-%B-%y")

