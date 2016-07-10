#Sorting on acc_id internally for each cluster
backup1 <- kyc_cluster_list
for( l in 1:length(kyc_cluster_list))
{
  kyc_cluster_list[[l]] <- kyc_cluster_list[[l]][order(kyc_cluster_list[[l]]$fromAccountNo),]
}

backup2 <- tran_cluster_list_agg
for( l in 1:length(tran_cluster_list_agg))
{
  tran_cluster_list_agg[[l]] <- tran_cluster_list_agg[[l]][order(tran_cluster_list_agg[[l]]$Group.1),]
}



#Calculating means for clusters
tran_cluster_means <- data.frame(cluster=integer(0),means=numeric(0))
for( n in 1:length(tran_cluster_list_agg))
{
  tran_cluster_means[n,1] <- n
  tran_cluster_means[n,2] <- sum(tran_cluster_list_agg[[n]]$x)/length(tran_cluster_list_agg[[n]]$x)
}

kyc_cluster_means <- data.frame(cluster=integer(0),means=numeric(0))
for( n in 1:length(kyc_cluster_list))
{
  kyc_cluster_means[n,1]  <- n
  kyc_cluster_means[n,2]  <- sum(kyc_cluster_list[[n]]$Salary)/length(kyc_cluster_list[[n]]$Salary)
}


#Ordering based on means
kyc_cluster_list_ord <- list()
order1 <- order(kyc_cluster_means$means)
for(g in 1:length(order1))
{
  kyc_cluster_list_ord[[g]] <- kyc_cluster_list[[order1[g]]]
}

tran_cluster_list_agg_ord <- list()
order2 <- order(tran_cluster_means$means)
for(h in 1:length(order2))
{
  tran_cluster_list_agg_ord[[h]] <- tran_cluster_list_agg[[order2[h]]]
}




