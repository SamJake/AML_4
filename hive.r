Sys.setenv(HIVE_HOME='/usr/lib/hive')
Sys.setenv(HADOOP_HOME='/usr/lib/hadoop')

setwd("/home/cloudera")

# Initialize Hive connection
rhive.init()
rhive.connect()

kyc = rhive.query(query = "select 
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


write.table(lapply(tranFile, as.character), 'CustomersWithFlags.csv', sep="|", row.names = FALSE, quote = FALSE)

# Write to Hive
system("hadoop fs -rm /user/AML_Project/customer/Custo*")
system("hadoop fs -put CustomersWithFlags.csv /user/AML_Project/customer/")