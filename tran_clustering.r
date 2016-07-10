set.seed(123)

#Hierarchial Clustering
dist2 <- dist(tran[8], method="euclidean")
clust2 <- hclust(dist2, method="ward.D2")

# plot(clust2)
# jpeg("J:/R/AML_3/Images/Tran_Clusters_Hier.jpeg")
# plot(clust2, labels=FALSE, hang = -1.5)
# rect.hclust(clust2,k=4,border = 2:4)
# dev.off()

tran_cluster <- cutree(clust2,k=3)

# jpeg("J:/R/AML_3/Images/Tran_Clusters_Elbow.jpeg")
# fviz_nbclust(tran,hcut,method = "wss") + geom_vline(xintercept = 3, linetype = 2)
# dev.off()

tran.c <- cbind(tran,tran_cluster)
tran_cluster_list <- list()
k <- 3
for(i in 1:k)
{
  tran_cluster_list[[i]] <- subset(tran.c,tran_cluster==i)
}



#Aggregation - count & means
tran_cluster_list_agg <- list()
for(m in 1:length(tran_cluster_list)) 
{
  acc <- tran_cluster_list[[m]][,3]
  bal <- tran_cluster_list[[m]][,8]
  c <- tran_cluster_list[[m]][,17]
  temp1 <- aggregate(bal,list(acc),mean)#meansdf
  temp2 <- aggregate(bal,list(acc),length)#countdf
  temp3 <- aggregate(c,list(acc),mean)#nth cluster
  temp4 <- cbind(temp1,temp2,temp3)
  temp4 <- temp4[,-3]
  temp4 <- temp4[,-4]
  tran_cluster_list_agg[[m]] <- temp4
}