#tran <- read.csv("E:/R/AML_4/Data/transactionDetails.csv")
tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = "select * from file where date = '11-Jan-16'")
tran$date <- as.Date(tran$date,format="%d-%B-%y")
#tran$timeOfTransaction <- as.Date(tran$timeOfTransaction,format="%H:%M %p")
#tran[is.na(tran)] <- 0
