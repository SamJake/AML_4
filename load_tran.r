tran <- read.csv("J:/R/AML_4/Data/transactionDetails.csv")
tran$date <- as.Date(tran$date,format="%d-%B-%y")
tran$timeOfTransaction <- as.character(tran$timeOfTransaction)
#tran[is.na(tran)] <- 0
