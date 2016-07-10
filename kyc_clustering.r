set.seed(123)

#Hierarchial Clustering
dist1 <- dist(kyc[4], method="euclidean")
clust1 <- hclust(dist1, method="ward.D2")

# plot(clust1)
# jpeg("J:/R/AML_3/Images/Kyc_Clusters_Hier.jpeg")
# plot(clust1, labels=FALSE, hang = -1)
# rect.hclust(clust1, k=3, border = 2:4)
# dev.off()

kyc_cluster <- cutree(clust1,k=3)

# jpeg("J:/R/AML_3/Images/Kyc_Clusters_Elbow.jpeg")
# fviz_nbclust(kyc,hcut,method = "wss") + geom_vline(xintercept = 3, linetype = 2)
# dev.off()

kyc.c <- cbind(kyc,kyc_cluster)
kyc_cluster_list <- list()
k <- 3
for(i in 1:k)
{
  kyc_cluster_list[[i]] <- subset(kyc.c,kyc_cluster==i)
}