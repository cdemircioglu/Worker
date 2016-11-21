#Get the library 
library("DBI")
library("dplyr")
library("methods")
library("data.table")
library("XML")


# Get the arguments
#args <- commandArgs(trailingOnly = TRUE)
#args <- "<ShinnyParameters><parameter><name>servercnt</name><value>3</value></parameter><parameter><name>marketInterest</name><value>INVESTING</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runCheck</name><value>2016</value></parameter><parameter><name>runnumber</name><value>20161031102213</value></parameter><parameter><name>runtime</name><value>1000</value></parameter><parameter><name>msisdn</name><value>5348,5599,7143,7461,8583,8717,9447,9453,9639,9937,10042,10166,10327,10385,10570,10746,10749,10900,10930,10940,11013,11288,11413,11460,11537,11549,11654,11682,11920,11943,11996,12003,12033,12055,12096,12105,12379,12380,12446,12643,12676,12720,12808,12862,12899,12917,12985,13037,13053,13142,13147,13214,13337,13342,13346,13390,13407,13420,13501,13521,13555,13677,13684,13734,13787,13788,13800,13855,13870,13901,13906,13935,13953,13965,14101,14125,14141,14145,14153,14227,14240,14291,14352,14385,14390,14422,14435,14447,14495,14566,14708,14714,14806,14868,14875,14931,14941,14944,14975,15006</value></parameter><parameter><name>marketinterestid</name><value>5</value></parameter><parameter><name>sprayprayuptake</name><value>0.5</value></parameter></ShinnyParameters>"
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
runnumber <- parameterValue[6]
runTime <- parameterValue[7]
msisdn <- parameterValue[8]
marketInterestID <- parameterValue[9]
sprayprayUptake <- as.numeric(parameterValue[10])


#Create the connection string
con <-  dbConnect(RMySQL::MySQL(),username = "root", password = "KaraburunCe2", host = "hwcontrol.cloudapp.net", port = 3306, dbname = "openroads")

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

#Afflunce function 
fun_Afflunce <- function()
{
  #Split the column into multiple columns
  #src_profile <- within(src_profile, PROFILE<-data.frame(do.call('rbind', strsplit(as.character(PROFILE), '|', fixed=TRUE))))
  
  #Calculations
  #x <- (creditScore/860*(assets + 3*houseHoldIncome + 100000))/100000
  
  src_xdr["AFFLUNCE"] <<- src_xdr[,2]/860*(src_xdr[,4]+3*src_xdr[,3])/100000
  src_xdr["RESPONSE"] <<- sqrt(as.numeric(src_xdr$DOWNLOADBYTES))*sqrt(src_xdr$AFFLUNCE)*perceivedValue*sprayprayUptake/1000
  src_xdr["ECONOMICBENEFIT"] <<- (perceivedValue*src_xdr["RESPONSE"])/max(src_xdr["RESPONSE"]*1.2) - costtoDeliver
  
  #src_profile["AFFLUNCE"] <<- strtoi(src_profile[,2][,1])/860*(strtoi(src_profile[,2][,2]) + 3*strtoi(src_profile[,2][,3]) + 100000)/100000
  #strsplit(profile, "|")[1] ##CreditScore
  #strsplit(profile, "|")[2] ##Assets
  #strsplit(profile, "|")[3] ##Household Income
}


#Response function
#fun_Response <- function() 
#{
#The function is 
# SQRT(MarketInterest) * SQRT(Afflunce) * Perceived_Value * SprayPray
#allRecordsMSISDN <- src_xdr[which(src_xdr$MSISDN == MSISDN),] #All market interests per MSISDN
#downloadBytes <- allRecordsMSISDN[which(allRecordsMSISDN$IAB_TIER2 == marketInterest),10] #Get the downloaded bytes
#f_marketInterest <- sum(downloadBytes)/NROW(downloadBytes)/10099
#f_afflunce <-  #Calculate afflunce based on MSISDN number
#f_sprayPray <- src_marketinterest[1,1] #Calculate spray and pray factor based on market interest
#src_profile["RESPONSE"] <<- sqrt(src_xdr$DOWNLOAD_BYTES)*sqrt(src_profile$AFFLUNCE)*as.numeric(perceivedValue)*f_sprayPray/1000
#}  

#Economic Benefit function 
#fun_EconomicBenefit <- function()
#{
#src_profile["ECONOMICBENEFIT"] <<- (as.numeric(perceivedValue)*src_profile["RESPONSE"]) - as.numeric(costtoDeliver)
#}


#Run the monte carlo 
fun_MC <- function()
{
  for (n in src_xdr$ECONOMICBENEFIT) {
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

#Reset the factor
fresult <<- seq(1,100)*0
fun_MC()

#Create the buckets
buckets <- seq(-50, 300, by=2)

#Create the bucketing logic
finalset <- transform(src_xdr, LABEL=cut(ECONOMICBENEFIT,breaks=buckets,labels=buckets[1:175]))
finalset <- finalset[complete.cases(finalset),] #Remove na figures, if any

#Calculate the counts
finalset <- finalset %>%
  group_by(LABEL) %>% 
  summarise_each(funs(n()),MSISDN)

#Per customer 
mcValue <- paste(round(0.5*fresult/nrow(finalset),digits=0),collapse=" ")

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
