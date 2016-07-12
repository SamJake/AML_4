Sys.setenv(HIVE_HOME='/usr/lib/hive')
Sys.setenv(HADOOP_HOME='/usr/lib/hadoop')

library(data.table)
library(sqldf)
library(stringdist)
library(phonics)
library(stringr)
library(RHive)

# Set local working directory
setwd("/home/cloudera")

# Initialize Hive connection
rhive.init()
rhive.connect()

searchAKA <- function(x)
{
  # Search 'Other Fields' in SDN file for alias
  # split the text using '; ' as the delimiter
  temp=unlist(strsplit(x, '; '))
  
  # extract only those strings which have the text 'a.k.a.' as the first 6 characters 
  temp1=temp[lapply(temp, FUN = function(x) {substr(x, 1, 6)})=='a.k.a.']
  
  # clean the string by removing the text 'a.k.a.' and other special characters
  temp2=gsub("'", "", (gsub("'.", "", gsub("a.k.a. '", "", temp1))))
  return (temp2)
}

searchPassport <- function(x)
{
  # Search 'Other Fields' in SDN file for passport
  # split the text using '; ' as the delimiter
  temp=unlist(strsplit(x, '; '))
  
  # extract only those strings which have the text 'Passport'  
  temp1=temp[lapply(temp, FUN = function(x) {grep('Passport', x)})=='1']
  
  # clean the string by removing the text 'Passport ' and other special characters
  temp2=gsub("alt. ", "", gsub("Passport ", "", temp1))
  temp3=sapply(strsplit(temp2, ' '), "[", 1)
  return (temp3)
}

applyMetaphone <- function(x)
{
  # get the Metaphone code for the given text
  temp1=lapply(lapply(x,toupper), metaphone, maxCodeLen=1000)
  
  # replace NA with text 'NA'
  temp2=sapply(temp1, function(y) {ifelse(is.na(y),'NA',y)})
  return (temp2)
}

standardizeName <- function(name)
{
  # remove special characters and multiple whitespaces from the name
  name = ifelse(is.na(name), 'NA', name) 	
  name = str_replace_all(name, "[',.;]", "")
  name = str_replace_all(name, "[-]", " ")
  name = str_trim(name, side = "both")
  name = gsub("\\s+", " ", name)
  name = toupper(name)
  return (name)
}

matchName <- function(allNames, metaPhones, searchName, searchPattern, algorithm)
{
  matches=list()
  
  # Find phonetic matches between names
  phoneticMatrix = stringdistmatrix(searchPattern, metaPhones, algorithm)
  
  # Find edit distances between names
  distanceMatrix = stringdistmatrix(searchName, allNames, algorithm)
  
  # Find the least distance in each row in the phoenetic distance matrix
  minPhonetic = apply(phoneticMatrix, 1, function(x) { min(x)*10 } )
  
  # Find the least distance in each row in the edit distance matrix
  minDistince = apply(distanceMatrix, 1, function(x) { min(x)*10 } )
  
  # Loop through the distances and check if they are below threshold values (closest matches)
  for (mCounter in 1:length(searchPattern))
  {
    matches[mCounter]=''
    found = 0
    
    # check for matches based on phonetic and edit distances
    if (minPhonetic[mCounter] < 2.5 & minDistince[mCounter] < 1.2)
    {
      matches[mCounter] = minDistince[mCounter]
      found = 1
    }
    
    # if there are no matches, check if the name is present as a substring in the SDN file
    # this logic is applied for names with length > 10
    if (found == 0 & nchar(searchName[mCounter]) > 10)
    {
      inString = length(grep(searchName[mCounter], allNames))
      if (inString >= 1)
        matches[mCounter] = 9
    }
    
  }
  
  return(matches)
}

standardizeAddress <- function(address1, address2="", city="", zip="", state="", country="")
{
  # paste address1, address2, city, zip, state & country into one field
  address = paste(address1, address2, city, zip, state, country)
  
  # remove special characters and multiple whitespaces from the address
  address = str_replace_all(address, "[',.;]", "")
  address = str_replace_all(address, "[-]", " ")
  address = str_trim(address, side = "both")
  address = gsub("\\s+", " ", address)
  address = toupper(address)
  return (address)
}

matchAddress <- function(stdSDNAddress, stdSearchAddress, algorithm)
{
  matches=list()
  
  # Find edit distances between addresses
  distanceMatrix = stringdistmatrix(stdSearchAddress, stdSDNAddress, method = algorithm)
  
  # Find the least distance in each row in the edit distance matrix
  minDistince = apply(distanceMatrix, 1, function(x) { min(x)*10 } )
  
  # Loop through the distances and check if they are below threshold values (closest matches)
  for (mCounter in 1:length(stdSearchAddress))
  {
    matches[mCounter]=''
    found = 0
    
    # check for matches based on edit distances
    if (minDistince[mCounter] < 1.5)
    {
      matches[mCounter] = minDistince[mCounter]
      found = 1
    }
    
    # if there are no matches, check if the address is present as a substring in the SDN file
    if (found == 0 & nchar(stdSearchAddress[mCounter]) > 10)
    {
      inString = length(grep(stdSearchAddress[mCounter], stdSDNAddress))
      if (inString >= 1)
        matches[mCounter] = 9
    }
    
  }
  
  return(matches)
}

matchPassport <- function(sdnPassport, tranPassport, sdnIds)
{
  matches=list()
  
  # loop through the passport# in the customer file
  for (mCounter in 1:length(tranPassport))
  {
    matches[mCounter]=''
    
    if (nchar(tranPassport[mCounter]))
    {
      inString = length(grep(tranPassport[mCounter], sdnPassport))
      
      if (inString >= 1)
        matches[mCounter] = inString
    }
  }
  
  return(matches)
}

computeOverallScore <- function(scoreCode)
{
  # Compute score based on the following matrix
  "
  Name	Alias	Address	Passport	ScoreCode	  OverallScore
  Y	    N	    N	      N		      1000		    80
  Y	    Y	    N	      N		      1100		    85
  Y	    N	    Y	      N		      1010		    85
  Y	    Y	    Y	      N		      1110		    90
  Y	    N	    N	      Y		      1001		    90
  Y	    N	    Y	      Y		      1011		    95
  Y	    Y	    N	      Y		      1101		    95
  Y	    Y	    Y	      Y		      1111		    100
  N	    N	    N	      Y		      0001		    85
  N	    Y	    N	      N		      0100		    75
  N	    Y	    Y	      N		      0110		    80
  N	    Y	    N	      Y		      0101		    85
  N	    Y	    Y	      Y		      0111		    90
  "
  score = switch (scoreCode,
                  '1000' =  80,
                  '1100' = 	85,
                  '1010' = 	85,
                  '1110' = 	90,
                  '1001' = 	90,
                  '1011' = 	95,
                  '1111' = 	100,
                  '0001' = 	85,
                  '0100' = 	75,
                  '0110' = 	80,
                  '0101' = 	85,
                  '0111' = 	90,
                  '1101' = 	95,
                  '0000' =  0
  )
}


#######################################################################
# Read SDN data (main, address and alias files)
#######################################################################

# read SDN names file
colclass = c("character","character","character","character","character","character","character","character","character","character","character","character")
sdnNames=fread("sdn.del", header=F, sep="@", sep2=";", stringsAsFactors=F, colClasses = colclass)
names(sdnNames) = c("sdnId","name","sdnType","country","title","callSign","vesselType","tonnage","grossRegisteredTonnage","vesselFlag","vesselOwner","otherDetails")

#search AKA names by parsing the 'otherDetails' field and add it to the data frame 
sdnNames$AKA=lapply(sdnNames$otherDetails, searchAKA)

# search Passport numbers by parsing the 'otherDetails' field and add it to the data frame 
sdnNames$Passport=lapply(sdnNames$otherDetails, searchPassport)

# read AKA file
colclass = c("character","character","character","character","character")
akaNames=fread("alt.del", header=F, sep="@", stringsAsFactors=F, colClasses = colclass)
names(akaNames) = c("sdnId","akaId","akaType","akaName","V5")

# drop unwanted columns
akaNames=subset(akaNames, select = c(sdnId, akaName))

# concatenate AKA names from multiple rows
akaNames=aggregate(akaName~sdnId,paste,collapse="@",data=akaNames)

# read Address file
colclass = c("character","character","character","character","character","character")
address=fread("add.del", header=F, sep="@", stringsAsFactors=F, colClasses = colclass)
names(address) = c("sdnId","addrId","addrLine1","cityZip","country","V5")

# concatenate address fields from multiple columns 
t=paste(address$addrLine1, address$cityZip, address$country, sep=' ;')
address$addr1.city.country=gsub("-0-","", t)

# drop unwanted columns
address=subset(address, select = c(sdnId, addr1.city.country))

# concatenate addresses from multiple rows
address=aggregate(addr1.city.country~sdnId,list,data=address)

# join AKA data and address data into the main data frame (sdnNames) 
sdnNames=merge(x=sdnNames, y=akaNames, by='sdnId', all.x=TRUE) 
sdnNames=merge(x=sdnNames, y=address, by='sdnId', all.x=TRUE)

# combine all names into one field
sdnNames$allNames=apply(cbind(strsplit(sdnNames$akaName, "@"),sdnNames$AKA, sdnNames$name),1,unlist)

# cleanup everything by keeping only required fields
sdnNames=subset(sdnNames, select = c(sdnId,allNames,sdnType,country,title,otherDetails,Passport,addr1.city.country))
rm(akaNames,address,t)

# apply metaphonic algorithm on names
sdnNames$namesMPhone=lapply(sdnNames$allNames, applyMetaphone)

#######################################################################
# Read KYC data from HIVE
#######################################################################

tranFile = rhive.query(query = "select 
                       customerId,
                       primaryAccountNumber,
                       balance,
                       firstName,
                       lastName,
                       middleName,
                       concat_ws(' ', lastName, firstName, middleName) as Name,
                       alias,
                       fatherLastName,
                       fatherFirstName,
                       fatherMiddleName,
                       spouseLastName,
                       spouseFirstName,
                       spouseMiddleName,
                       dateOfBirth,
                       marital_Status,
                       addressLine1,
                       addressLine2,
                       addressCity,
                       addressZipCode,
                       addressState,
                       addressCountry,
                       primaryPhone,
                       secondaryPhone,
                       landLinePhone,
                       primaryEmailId,
                       secondaryEmailId,
                       primaryOccupation,
                       primaryOccupationEmployerName,
                       primaryOccupationAddressLine1,
                       primaryOccupationAddressLine2,
                       primaryOccupationAddressCity,
                       primaryOccupationAddressZipCode,
                       primaryOccupationAddressState,
                       primaryOccupationAddressCountry,
                       primaryOccupationPrimaryPhone,
                       primaryOccupationSecondaryPhone,
                       debitCardNo,
                       primaryBranchcode,
                       accountType,
                       passport_no
                       from aml_customerdetails")

# replace quotes from all fields (hive adds quotes for data imported from CSV files)
#tranFile = lapply(tranFile, function(x) {gsub('"', '',x)})

#######################################################################
# Start matching data from KYC file to SDN file
#######################################################################

# Match names
tranFile$namesMPhone=lapply(lapply(as.character(tranFile$name),toupper), metaphone, maxCodeLen=1000)
stdSDNName = lapply(sdnNames$allNames, standardizeName)
tranName = standardizeName(as.character(tranFile$name))
tranFile$nameMatches=matchName(unlist(stdSDNName), unlist(sdnNames$namesMPhone), tranName, tranFile$namesMPhone, 'jw')

# Match aliases
tranFile$aliasMPhone=lapply(lapply(as.character(tranFile$alias),toupper), metaphone, maxCodeLen=1000)
tranAlias = standardizeName(as.character(tranFile$alias))
tranFile$aliasMatches=matchName(unlist(stdSDNName), unlist(sdnNames$namesMPhone), tranAlias, tranFile$aliasMPhone, 'jw')


# Match addresses
sdnAddr = lapply(sdnNames$addr1.city.country, standardizeAddress)
tranAddr = standardizeAddress(tranFile$addressline1, tranFile$addressline2, tranFile$addresscity, tranFile$addresszipcode, tranFile$addressstate, tranFile$addresscountry)
tranFile$addressMatches=matchAddress(unlist(sdnAddr), tranAddr, 'jw')

# Match passport
tranFile$passportMatches=matchPassport(unlist(sdnNames$Passport), as.character(tranFile$passport_no), sdnNames$sdnId)

# Get the overall score code based on the matches identified
tranFile$overallScoreCode = paste(
  ifelse(tranFile$nameMatches!="", '1', '0'), 
  ifelse(tranFile$aliasMatches!="", '1', '0'), 
  ifelse(tranFile$addressMatches!="", '1', '0'),
  ifelse(tranFile$passportMatches!="", '1', '0'),
  sep='')

# Compute the overall score based on the overall score code
tranFile$overallScore = lapply(tranFile$overallScoreCode, computeOverallScore)

#remove housekeeping columns before writing to a file/Hive
tranFile=subset(tranFile, select = 
                  c(customerid,primaryaccountnumber,balance,firstname,lastname,middlename,alias,fatherlastname,fatherfirstname,fathermiddlename,
                    spouselastname,spousefirstname,spousemiddlename,dateofbirth,marital_status,addressline1,addressline2,addresscity,addresszipcode,addressstate,
                    addresscountry,primaryphone,secondaryphone,landlinephone,primaryemailid,secondaryemailid,primaryoccupation,primaryoccupationemployername,
                    primaryoccupationaddressline1,primaryoccupationaddressline2,primaryoccupationaddresscity,primaryoccupationaddresszipcode,
                    primaryoccupationaddressstate,primaryoccupationaddresscountry,primaryoccupationprimaryphone,primaryoccupationsecondaryphone,debitcardno,
                    primarybranchcode,accounttype,passport_no,nameMatches,aliasMatches,addressMatches,passportMatches,overallScore))


# Write the KYC file along with the scores to a file
write.table(lapply(tranFile, as.character), 'CustomersWithFlags.csv', sep="|", row.names = FALSE, quote = FALSE)

# Write to Hive
system("hadoop fs -rm /user/AML_Project/customer/Custo*")
system("hadoop fs -put CustomersWithFlags.csv /user/AML_Project/customer/")

