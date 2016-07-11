#package install
packages <- c("devtools","factoextra","cluster","NbClust","ggplot2","sqldf")
#packages <- c("devtools","factoextra","cluster","NbClust","ggplot2","sqldf","RHive")
if(length(setdiff(packages,rownames(installed.packages())))>0)
{
  install.packages(setdiff(packages,rownames(installed.packages())))
}
sapply(packages,library,character.only=TRUE)