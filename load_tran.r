dt <- read.csv("E:/R/AML_4/Data/date.csv")
dt <- dt[1,1]
stat <- "Cancelled"
if(dt == "ALL")
{
  query <- paste0("select * from file where status != '", stat,"'")
  print(query)
}else
{
  query <- paste0("select * from file where date = '",dt,"' and status != '", stat,"'")
  print(query)
}
tran <- read.csv.sql("E:/R/AML_4/Data/transactionDetails.csv",sql = query)
tran$date <- as.Date(tran$date,format="%d-%B-%y")

