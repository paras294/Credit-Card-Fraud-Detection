# Credit-Card-Fraud-Detection
Fraud detection solution: This is a feature to detect fraudulent transactions, wherein once a cardmember swipes their card for payment, the transaction is classified as fraudulent or authentic based on a set of predefined rules. If fraud is detected, then the transaction must be declined. Please note that incorrectly classifying a transaction as fraudulent will incur huge losses to the company and also provoke negative consumer sentiment.   
Customer information: The relevant information about the customers needs to be continuously updated on a platform from where the customer support team can retrieve relevant information in real-time to resolve customer complaints and queries.
![image](https://user-images.githubusercontent.com/39978672/143277006-03a91ed2-ac4c-4a99-917b-06fb6d588000.png)


Task 1: Load the transactions history data (card_transactions.csv) in a NoSQL database.

Task 2: Ingest the relevant data from AWS RDS to Hadoop.

Task 3: Create a look-up table with columns specified earlier in the problem statement.

Task 4: After creating the table, you need to load the relevant data in the lookup table.

Task 5: Create a streaming data processing framework that ingests real-time POS transaction data from Kafka. The transaction data is then validated based on the three rules’ parameters (stored in the NoSQL database) discussed previously.

Task 6: Update the transactions data along with the status (fraud/genuine) in the card_transactions table.

Task 7: Store the ‘postcode’ and ‘transaction_dt’ of the current transaction in the look-up table in the NoSQL database if the transaction was classified as genuine.
