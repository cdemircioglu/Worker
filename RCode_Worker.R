#Get the library 
library("DBI")
library("dplyr")
library("methods")
library("data.table")
library("XML")


# Get the arguments
#args <- commandArgs(trailingOnly = TRUE)
#args <- "<ShinnyParameters><parameter><name>servercnt</name><value>3</value></parameter><parameter><name>marketInterest</name><value>INVESTING</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runCheck</name><value>2016</value></parameter><parameter><name>runnumber</name><value>20161031102213</value></parameter><parameter><name>runtime</name><value>1000</value></parameter><parameter><name>msisdn</name><value>5348,5599,7143,7461,8583,8717,9447,9453,9639,9937,10042,10166,10327,10385,10570,10746,10749,10900,10930,10940,11013,11288,11413,11460,11537,11549,11654,11682,11920,11943,11996,12003,12033,12055,12096,12105,12379,12380,12446,12643,12676,12720,12808,12862,12899,12917,12985,13037,13053,13142,13147,13214,13337,13342,13346,13390,13407,13420,13501,13521,13555,13677,13684,13734,13787,13788,13800,13855,13870,13901,13906,13935,13953,13965,14101,14125,14141,14145,14153,14227,14240,14291,14352,14385,14390,14422,14435,14447,14495,14566,14708,14714,14806,14868,14875,14931,14941,14944,14975,15006</value></parameter><parameter><name>marketinterestid</name><value>5</value></parameter><parameter><name>sprayprayuptake</name><value>0.5</value></parameter></ShinnyParameters>"
#args <- "<ShinnyParameters><parameter><name>servercnt</name><value>10</value></parameter><parameter><name>marketInterest</name><value>MOVIES</value></parameter><parameter><name>perceivedValue</name><value>70</value></parameter><parameter><name>costtoDeliver</name><value>40</value></parameter><parameter><name>runCheck</name><value>1</value></parameter><parameter><name>runnumber</name><value>20161123055954</value></parameter><parameter><name>runtime</name><value>2144253</value></parameter><parameter><name>msisdn</name><value>03629,303630,303631,303632,303633,303634,303635,303636,303637,303638,303639,303640,303642,303644,303645,303646,303647,303648,303649,303650,303652,303654,303655,303656,303657,303659,303660,303661,303662,303663,303664,303665,303666,303668,303669,303670,303671,303672,303673,303674,303675,303676,303677,303678,303679,303680,303681,303682,303683,303684,303685,303686,303687,303688,303689,303691,303692,303694,303695,303696,303698,303699,303700,303702,303703,303704,303705,303706,303707,303708,303710,303711,303712,303713,303714,303715,303716,303717,303718,303719,303720,303721,303722,303723,303724,303725,303726,303727,303728,303730,303731,303732,303733,303734,303735,303736,303737,303738,303739,303740</value></parameter><parameter><name>marketinterestid</name><value>13</value></parameter><parameter><name>sprayprayuptake</name><value>0.10</value></parameter><parameter><name>promotionalCost</name><value>1</value></parameter><parameter><name>captiveMonths</name><value>10</value></parameter></ShinnyParameters>"
#args <- "<ShinnyParameters><parameter><name>servercnt</name><value>1</value></parameter><parameter><name>marketInterest</name><value>MOVIES</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runCheck</name><value>4337</value></parameter><parameter><name>promotionalCost</name><value>2</value></parameter><parameter><name>captiveMonths</name><value>6</value></parameter><parameter><name>churnRate</name><value>2</value></parameter><parameter><name>runnumber</name><value>20161221004337</value></parameter><parameter><name>runtime</name><value>2144253</value></parameter><parameter><name>msisdn</name><value>51546,151547,151548,151550,151552,151553,151554,151555,151556,151557,151558,151560,151561,151563,151564,151565,151566,151567,151568,151569,151570,151571,151572,151573,151574,151576,151577,151578,151579,151580,151581,151582,151583,151584,151586,151587,151589,151590,151592,151593,151594,151595,151596,151597,151598,151599,151600,151601,151602,151603,151604,151605,151606,151607,151608,151609,151611,151612,151613,151614,151615,151616,151617,151618,151622,151623,151624,151625,151627,151628,151630,151631,151632,151633,151637,151638,151639,151640,151641,151642,151643,151645,151646,151649,151650,151651,151652,151653,151654,151655,151656,151657,151658,151660,151661,151662,151663,151664,151665,151666</value></parameter><parameter><name>marketinterestid</name><value>13</value></parameter><parameter><name>sprayprayuptake</name><value>0.10</value></parameter></ShinnyParameters>"

args <- "RRRR"

# Give the input file name to the function.
xmlDoc <- xmlParse(args, asText=TRUE)

# Read the parameters from xml
parameterName <- xpathSApply(xmlDoc,'//parameter/name',xmlValue)
parameterValue<- xpathSApply(xmlDoc,'//parameter/value',xmlValue)  

# Create the parameters
servercnt <- parameterValue[1]
marketInterest <- parameterValue[2]
perceivedValue <- as.numeric(parameterValue[3])
costtoDeliver <- as.numeric(parameterValue[4])
runCheck <- parameterValue[5]
promotionalCost <<- as.numeric(parameterValue[6])
captiveMonths <<- as.numeric(parameterValue[7])
churnRate <<- as.numeric(parameterValue[8])
runnumber <- parameterValue[9]
runTime <- parameterValue[10]
msisdn <- parameterValue[11]
marketInterestID <- parameterValue[12]
sprayprayUptake <- as.numeric(parameterValue[13])

#New parameters
monthlyPrice <<- perceivedValue
monthlyCost <<- costtoDeliver
growthRate <<- 0
#churn <<- 0.05
variableMonths <<- captiveMonths


#Create the connection string
#con <-  dbConnect(RMySQL::MySQL(),username = "root", password = "KaraburunCe2", host = "hwcontrol.cloudapp.net", port = 3306, dbname = "openroads")
con <-  dbConnect(RMySQL::MySQL(),username = "root", password = "KaraburunCe2", host = "localhost", port = 3306, dbname = "openroads")

#Create the query for xdr records
src_query <- ("SELECT MSISDN, CREDITSCORE, HHINCOME, ASSETS, DOWNLOADBYTES FROM fct_marketinterest WHERE MSISDN IN (aaa) AND MARKETINTERESTID = 'bbb'")
src_query <- gsub("aaa",msisdn,src_query)
src_query <- gsub("bbb",marketInterestID,src_query)

#Get the records for xdr
src_xdr <<- dbGetQuery(con, src_query)

#Group the records
src_xdr <- data.table(src_xdr)
src_xdr <- data.frame(src_xdr[,list(CREDITSCORE=mean(CREDITSCORE),HHINCOME=mean(HHINCOME),ASSETS=mean(ASSETS),DOWNLOADBYTES = sum(DOWNLOADBYTES)),by=MSISDN])

#Disconnect from the database
dbDisconnect(con)

####FUNCTIONS####

#Churn function definition
fun_churn <- function(captiveMonths)
{
  p <- 0
  for(ii in 1:captiveMonths)
  {
    p <- p + (1-churn)^ii
  }
  p
}
#LCV function
fun_LCV <- function(variableMonths,data)
{
  #Churn rate
  #lifecyleYears <- (1/(log(100,base=exp(1))-log((100 - churnRate),base=exp(1))))/12.0
  
  #variableMonths
  #data$CURRENTNUMBEROFMONTHSINPLAN <- data[,5]%%40+1
  
  #Result set
  result <- data.frame(LCV=numeric())
  
  #for(i in 1:nrow(data))
  {
    #divisionConstant <- if(lifecyleYears-data$CURRENTNUMBEROFMONTHSINPLAN[i]-variableMonths/12 == 0) 1 else 0 #avoid division by zero error.
    
    r <- (
      (monthlyPrice-monthlyCost+5.01)*captiveMonths+ #(OP - OC + 5)*CM
        (-promotionalCost-monthlyCost) #(- PC - OC)
      
      #(1+growthRate)^((lifecyleYears-data$CURRENTNUMBEROFMONTHSINPLAN[i]/12))*12* #(1-GR)^(LY-CP)
      #(lifecyleYears+captiveMonths/12) #(LP-CP-CM)
    ) #(-MP-PC) #Moved after P(i) calculation
    
    #result <- rbind(result,r)
    r
  }
  #result
}




#Afflunce function 
fun_Afflunce <- function()
{
  #Split the column into multiple columns
  #src_profile <- within(src_profile, PROFILE<-data.frame(do.call('rbind', strsplit(as.character(PROFILE), '|', fixed=TRUE))))
  
  #Calculations
  #x <- (creditScore/860*(assets + 3*houseHoldIncome + 100000))/100000
  src_xdr["AFFLUNCE"] <<- src_xdr[,2]/860*(src_xdr[,4]+3*src_xdr[,3])/100000
  
  #Find the max affluence per group
  max_affluence <- max(src_xdr["AFFLUNCE"])
  
  #Find the max download bytes 
  max_download <- max(src_xdr["DOWNLOADBYTES"])
  
  #Normalize the affluence
  src_xdr["AFFLUNCE"] <<- src_xdr["AFFLUNCE"]/max_affluence
  
  #Calculate the response
  #src_xdr["RESPONSE"] <<- sqrt(as.numeric(src_xdr$DOWNLOADBYTES))*sqrt(src_xdr$AFFLUNCE)*perceivedValue*sprayprayUptake/1000
  src_xdr["RESPONSE"] <<- src_xdr$AFFLUNCE*sprayprayUptake*src_xdr$DOWNLOADBYTES/max_download*6
  
  #Adjust the response
  mres <- min(src_xdr["RESPONSE"])
  src_xdr["RESPONSE"] <<- src_xdr["RESPONSE"] - mres
  
  #Calculate the number of months in the plan
  #src_xdr["CURRENTNUMBEROFMONTHSINPLAN"] <<- src_xdr[,1]%%29+1
  
  #Set the economic benefit
  src_xdr["ECONOMICBENEFIT"] <<- (fun_LCV(captiveMonths,src_xdr)*src_xdr["RESPONSE"])#+(-monthlyPrice-promotionalCost) #- fun_LCV(0,src_xdr) 
  
  #Find the users who would take the offer Response > 0.5
  #src_xdr$ECONOMICBENEFIT[src_xdr$RESPONSE<0.01] <<- -promotionalCost#*src_xdr["RESPONSE"] #The people, not affluent enough, to take the offer.
  src_xdr$ECONOMICBENEFIT[src_xdr$RESPONSE<0.01] <<- -0.0001 #-promotionalCost#*src_xdr["RESPONSE"] #The people, not affluent enough, to take the offer.
  #src_xdr$ECONOMICBENEFIT[src_xdr$RESPONSE<0.015 && src_xdr$RESPONSE > 0.01] <<-unlist( -1*promotionalCost*src_xdr["RESPONSE"]) #*src_xdr["RESPONSE"] #Customers who use the offer just for a month. 
  #src_xdr$ECONOMICBENEFIT[src_xdr$RESPONSE<0.020 && src_xdr$RESPONSE > 0.015] <<- - monthlyCost - promotionalCost + 5 #People who take the offer only for one month
  
  #src_xdr$ECONOMICBENEFIT[src_xdr$ECONOMICBENEFIT< -200] <<- -200 #Lowend
  #src_xdr$ECONOMICBENEFIT[src_xdr$ECONOMICBENEFIT> 1000] <<- 1000 #Highend
  #src_xdr[which(src_xdr$RESPONSE>0.05 && src_xdr$ECONOMICBENEFIT<0, arr.ind=TRUE),9] <<- 0 #The people we don't care to work with.
  
  #Calculate the economic benefit
  #src_xdr["ECONOMICBENEFIT"] <<- (perceivedValue*src_xdr["RESPONSE"])/max(src_xdr["RESPONSE"]*1.2) - costtoDeliver
  #src_xdr["ECONOMICBENEFIT"] <<- (perceivedValue*1.8*src_xdr["RESPONSE"])/abs(max(src_xdr["RESPONSE"])) - costtoDeliver
  #src_xdr["ECONOMICBENEFIT"] <<- fun_LCV() - fun_LCV(0)
}



#Run the monte carlo 
fun_MC <- function()
{
  for (n in src_xdr$ECONOMICBENEFIT[src_xdr$ECONOMICBENEFIT != -1*promotionalCost*src_xdr$RESPONSE]) {
    randomVector <- sample.int(2, size = 100, replace = TRUE, prob = NULL)-1
    #tresult <- sapply(1:(length(randomVector)), function(j) trunc(sum(n*randomVector[j]),prec = 2))
    tresult <- sapply(1:(length(randomVector)), function(j) sum(n*randomVector[j]))
    fresult <<- fresult + tresult
  }
}



####CALL FUNCTIONS####
fun_Afflunce()
#fun_Response()
#fun_EconomicBenefit()

#Remove the NA's from the data frame
src_xdr <- src_xdr[complete.cases(src_xdr),]

#Find the people who would take up the offer
src_ori <- src_xdr #Keep the original
src_xdr <- src_xdr[src_xdr$ECONOMICBENEFIT != -0.0001,]
src_xdr <- src_xdr[is.finite(src_xdr$ECONOMICBENEFIT),]

#Reset the factor
fresult <<- seq(1,100)*0
fun_MC()

#Create the buckets
maxb <- max(src_xdr$ECONOMICBENEFIT)
minb <- min(src_xdr$ECONOMICBENEFIT)
byb <- ((maxb-minb)/30)
byb <- if(byb == 0) 0.01 else byb
buckets <- c(0,seq(minb, maxb, by=byb))
if (maxb-minb > 1)
{
  buckets <- sort(unique(as.integer(buckets)))
} else {
  buckets <- sort(unique(floor(buckets*1000)/1000))
}


#Create the bucketing logic
finalset <- transform(src_xdr, LABEL=cut(ECONOMICBENEFIT,breaks=buckets,labels=buckets[1:length(buckets)-1]))
finalset <- finalset[complete.cases(finalset),] #Remove na figures, if any

#Calculate the counts
finalset <- finalset %>%
  group_by(LABEL) %>% 
  summarise_each(funs(n()),MSISDN)

#Per customer 
mcValue <- paste(round(2*fresult/nrow(src_xdr),digits=0),collapse=" ")

#Write the src_xdr records
#write.table(src_xdr, file="eggs.csv", append=TRUE, row.names=TRUE, col.names=FALSE,  sep=",")

#Create the dataset
finalset <- as.data.frame(finalset)
finalset["HOST"] <- "na.com"
finalset <- finalset[complete.cases(finalset),] #Remove na figures, if any
#finalset["MCValue"] <- mcValue


#Create the arguments string
arg <- ""

#Loop through 
for (row in 1:nrow(finalset)) {
  ####
  # Output string
  output <- 'aaa,bbb,ccc,ddd,eee,fff'
  output <- gsub("aaa",runnumber,output) #This is the run number
  output <- gsub("bbb",finalset[row,1],output) #Bucket
  output <- gsub("ccc",finalset[row,2],output) #Count
  output <- gsub("ddd",runTime,output) #Runtime
  output <- gsub("eee",finalset[row,3],output) #Website
  output <- gsub("fff",runCheck,output) #RunCheck
  #output <- gsub("07:57:22",format(Sys.time(), format = "%H:%M:%S"),output) #Repace the time with actual time
  arg <- paste(arg,output,sep="|")
}

#Recast the data
output <- substring(arg, 2) # Remove the first pipe

#Command start
cmdString <- paste("python send.py '", paste(mcValue,runCheck,output,sep="_MM_"),"'", sep="")

#Send the message
system(cmdString)
