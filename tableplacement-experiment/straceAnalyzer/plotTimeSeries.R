library(ggplot2)
library(scales)

args <- commandArgs(TRUE)
file <- args[1]

d <- read.table(file, header=TRUE)

#d=data.frame(x=c(1,2,5,6,8), y=c(3,6,2,8,7), vx=c(1,1.5,0.8,0.5,1.3), vy=c(0.2,1.3,1.7,0.8,1.4))
ggplot() + 
geom_segment(data=d, mapping=aes(x=startTS, y=startPos, xend=endTS, yend=endPos), size=1, color="black") + ylab("File Position (Bytes)") + xlab("Time (s)") + ggtitle("Time Series")
