dt <- "12-Jan-16"
query <- paste0("select * from file where date = '",dt,"'")
tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = query)
tran$date <- as.Date(tran$date,format="%d-%B-%y")

