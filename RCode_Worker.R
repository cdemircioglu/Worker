#Get the library 
library(DBI)
library(dplyr)
# Load the package required to read XML files.
library("XML")
# Also load the other required package.
library("methods")

# Get the arguments
#args <- commandArgs(trailingOnly = TRUE)
#args <- "<ShinnyParameters><parameter><name>runtime</name><value>1000</value></parameter><parameter><name>servercnt</name><value>3</value></parameter><parameter><name>marketInterest</name><value>INVESTING</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runnumber</name><value>20161027174608</value></parameter><parameter><name>msisdn</name><value>95030611,95089837,95230764,95241826,95320597,95452031,95512677,95519835,95673236,95917936</value></parameter></ShinnyParameters>"
#args <- "<ShinnyParameters><parameter><name>servercnt</name><value>3</value></parameter><parameter><name>marketInterest</name><value>INVESTING</value></parameter><parameter><name>perceivedValue</name><value>40</value></parameter><parameter><name>costtoDeliver</name><value>10</value></parameter><parameter><name>runnumber</name><value>20161031102213</value></parameter><parameter><name>runtime</name><value>1000</value></parameter><parameter><name>msisdn</name><value>258493,1259860,1267845,1268277,1268509,1269925,1270078,1270582,1274345,1274776</value></parameter></ShinnyParameters>"
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

#Create the connection string
con <-  dbConnect(RMySQL::MySQL(),username = "root", password = "KaraburunCe2", host = "127.0.0.1", port = 3306, dbname = "testdata")

#Create the query for xdr records
src_query <- ("SELECT MSISDN, DOWNLOAD_BYTES,HOST FROM src_xdr WHERE MSISDN IN (aaa) AND IAB_TIER1 = 'bbb'")
src_query <- gsub("aaa",msisdn,src_query)
src_query <- gsub("bbb",marketInterest,src_query)

#Get the records for xdr
src_xdr <<- dbGetQuery(con, src_query)

#Create the query for xdr records
src_query <- ("SELECT * FROM fct_profile WHERE MSISDN IN (aaa)")
src_query <- gsub("aaa",msisdn,src_query)

#Get the records for xdr
src_profile <<- dbGetQuery(con, src_query)

#Create the query for xdr records
src_query <- ("SELECT SPRAYPRAYUPTAKE FROM src_marketinterest WHERE MARKETINTEREST = 'bbb'")
src_query <- gsub("bbb",marketInterest,src_query)

#Get the records for xdr
src_marketinterest <<- dbGetQuery(con, src_query)

#Create the query for xdr records
src_query <- ("SELECT MSISDN, SUM(DOWNLOAD_BYTES)/COUNT(1) AS DOWNLOAD_BYTES FROM src_xdr WHERE MSISDN IN (aaa) GROUP BY MSISDN")
src_query <- gsub("aaa",msisdn,src_query)

#Get the records for xdr
src_xdr_bytes <<- dbGetQuery(con, src_query)

#Disconnect from the database
dbDisconnect(con)

####FUNCTIONS####

#Afflunce function 
fun_Afflunce <- function()
{
  #Split the column into multiple columns
  src_profile <- within(src_profile, PROFILE<-data.frame(do.call('rbind', strsplit(as.character(PROFILE), '|', fixed=TRUE))))
  
  #Calculations
  #x <- (creditScore/860*(assets + 3*houseHoldIncome + 100000))/100000
  
  src_profile["AFFLUNCE"] <<- strtoi(src_profile[,2][,1])/860*(strtoi(src_profile[,2][,2]) + 3*strtoi(src_profile[,2][,3]) + 100000)/100000
  
  #strsplit(profile, "|")[1] ##CreditScore
  #strsplit(profile, "|")[2] ##Assets
  #strsplit(profile, "|")[3] ##Household Income
}

#Response function
fun_Response <- function() 
{
  #The function is 
  # SQRT(MarketInterest) * SQRT(Afflunce) * Perceived_Value * SprayPray
  #allRecordsMSISDN <- src_xdr[which(src_xdr$MSISDN == MSISDN),] #All market interests per MSISDN
  #downloadBytes <- allRecordsMSISDN[which(allRecordsMSISDN$IAB_TIER2 == marketInterest),10] #Get the downloaded bytes
  #f_marketInterest <- sum(downloadBytes)/NROW(downloadBytes)/10099
  #f_afflunce <-  #Calculate afflunce based on MSISDN number
  f_sprayPray <- src_marketinterest[1,1] #Calculate spray and pray factor based on market interest
  f_common <- base::merge(src_profile,src_xdr_bytes)
  src_profile["RESPONSE"] <<- (ifelse(sqrt(f_common$DOWNLOAD_BYTES)>100, runif(1, 70, 90), sqrt(f_common$DOWNLOAD_BYTES)))*sqrt(f_common$AFFLUNCE)*as.numeric(perceivedValue)*f_sprayPray/1000
}  

#Economic Benefit function 
fun_EconomicBenefit <- function()
{
  src_profile["ECONOMICBENEFIT"] <<- (as.numeric(perceivedValue)*src_profile["RESPONSE"]) - as.numeric(costtoDeliver)
}

#Run the monte carlo 
fun_MC <- function()
{
  for (n in src_profile$ECONOMICBENEFIT) {
    randomVector <- sample.int(2, size = 100, replace = TRUE, prob = NULL)-1
    #tresult <- sapply(1:(length(randomVector)), function(j) trunc(sum(n*randomVector[j]),prec = 2))
    tresult <- sapply(1:(length(randomVector)), function(j) sum(n*randomVector[j]))
    fresult <<- fresult + tresult
  }
}



####CALL FUNCTIONS####
fun_Afflunce()
fun_Response()
fun_EconomicBenefit()

#Reset the factor
fresult <<- seq(1,100)*0
fun_MC()

#Create the buckets
buckets <- seq(-50, 300, by=2)

#Create the bucketing logic
finalset <- transform(src_profile, LABEL=cut(ECONOMICBENEFIT,breaks=buckets,labels=buckets[1:175]))
finalset <- finalset[complete.cases(finalset),] #Remove na figures, if any

#Calculate the counts
finalset <- finalset %>%
  group_by(LABEL) %>% 
  summarise_each(funs(n()),MSISDN)

#Per customer 
mcValue <- paste(round(fresult/(nrow(finalset)*50),digits=0),collapse=" ")


#Create the dataset
finalset <- as.data.frame(finalset)
finalset["HOST"] <- gsub("https:","",gsub("\r","",src_xdr[1:nrow(finalset),3]))
finalset <- finalset[complete.cases(finalset),] #Remove na figures, if any
#finalset["MCValue"] <- mcValue

#Create the arguments string
arg <- ""

#Loop through 
for (row in 1:nrow(finalset)) {
  ####
  # Output string
  output <- '"2016-09-27","07:57:22",12345,98765,x86_64,"linux-gnu","BH","1.60.0-2","DE",23657'
  #output <- gsub("DE",mcValue,output) #Replace the package with a website
  #output <- gsub("linux-gnu",round(runif(1, 1, 25)),output) #Replace the package with a website
  output <- gsub("x86_64",runTime,output) #Replace the package with a website
  output <- gsub("BH",finalset[row,3],output) #Replace the package with a website
  #output <- gsub("07:57:22",format(Sys.time(), format = "%H:%M:%S"),output) #Repace the time with actual time
  output <- gsub("23657",runCheck,output) #Replace the package with a website
  output <- gsub("12345",finalset[row,1],output) #Replace the package with a website
  output <- gsub("98765",finalset[row,2],output) #Replace the package with a website
  
  
  arg <- paste(arg,output,sep="|")
  
  #look up stuff using data from the row
  #write stuff to the file
  print(output)
}

#Recast the data
output <- substring(arg, 2) # Remove the first pipe

# Command start
cmdString <- paste("python send.py '", paste(mcValue,runCheck,output,sep="_MM_"),"'", sep="")
#cmdString <- paste("python send.py '", paste(mcValue,output,sep="_MM_"),"'", sep="")
#cmdString <- paste('c:\\Python27\\python.exe send.py ', output, sep="")

# Send the message
system(cmdString)

