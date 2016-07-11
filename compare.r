flag <- data.frame(ACC_ID=integer(0),cluster=integer(0))
n <- 0
for(x in 1:(length(kyc_cluster_list_ord)-1))#x=#clusters-1 --2
{
  for(y in 1:length(kyc_cluster_list_ord[[x]]$primaryAccountNumber))#y=#accounts in kyc cluster --4
  {
    for(z in 1:length(tran_cluster_list_agg_ord[[x+1]]$Group.1))#z=#accounts in next tran cluster (agg) --5
    {
      print(paste(kyc_cluster_list_ord[[x]][y,2],"-", tran_cluster_list_agg_ord[[x+1]][z,1]))
      if (kyc_cluster_list_ord[[x]][y,2] == tran_cluster_list_agg_ord[[x+1]][z,1])
      {
        n <- n+1
        print(paste(kyc_cluster_list_ord[[x]][y,2],"-", tran_cluster_list_agg_ord[[x+1]][z,1]))
        flag[n,1] <- tran_cluster_list_agg_ord[[x+1]][z,1]
        flag[n,2] <- tran_cluster_list_agg_ord[[x+1]][z,4]
      }
    }
  }
}
    