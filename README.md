# SafeDB Architecture and Structure -- A move towards Decentralized Internet

## Problem Statement
Imagine there is a world where we have some trusted pods (or hosts) which you choose to save your data on. All services / APIs would be granted permissions to fetch data from this trusted server. Every time your data is queried you are eligible to get paid for it.  
Let's call the person saving the data as the `contractor`, the person giving his data as the `publisher` and services querying the data as the `clients` in our model.

One potential problem in the above idea is, "What if the publisher is not trustworthy?" We want the data to still be queryable by a client but the contractor should not be able to see it. 

I can think of a simple solution to this problem with one assumption. What if we relax the constraint that accurate data has to be retrieved? We can say that our system always returns more than or equal to the number of records that are requested. 

The approach that I am thinking of is as follows: 
The publisher gives encrypted data to the publisher but adds an "id" column to it. The publisher also gives a set of "grouped_projections" to the publisher. The group projections map a range (or a group) to a set of ids. 

e.g. Say we have a table with columns -
| name | age | branch | id (our system adds this column)

We will have 3 grouped_projections - 
  
| name | ids |                                                       
|a*| [1,3,5,6,7]                                 
|b*| [2,9,10,11]                                

| age | ids |  
| 5- 10 | [1, 10, 12]  
| 11-15 | [2, 9, 10]  

| branch | ids |  
| CMSC | [1,5, 6]  
| ENEE | [2, 4, 3]  


The contractor now owns the encrypted data along with the above projections. The contractor does not have complete knowledge of the dataset but still has enough information to run queries. However, the data returned will always be an overestimate which the client can filter.

I believe we can run a lot of standard queries with this structure. Also, the publisher can choose the grouped projections to ensure minimal leakage of additional data. e.g. Actual branch names in the last case.

The client on receiving a filtered encrypted dataset can request the publisher for keys to decrypt and access the data. 

## Implementation Style
Publisher (Runs the key server on port 10002): 
  - Actions:
    - Sends request to register with the contractor. The contractor would create a new database for the publisher on receiving this request.
    - Send DDL queries to modify the structure of the database
    - Encrypt his data and generate a list of projections
    - Send the data and projection set to the contractor.
    - Run a key server to send the key used to lock the data to the clients who request it. 
   
Contractor (Runs on port 10000 in this code base):  
   - Actions:
     - Maintain and run a high performance database
     - Run queries given by the client on projections and return encrypted data. 
     - Aim to return as little data as possible. 
    
Client:  
   - Actions:  
      - Run queries on the Contractor and get encrypted dataset
      - Query to get the key from the key server
      - Decrypt the data in memory to get the final result. 
    
## How to run this project? 
Contractor:  
   - Simulate the contractor by running the `runserver.py`
   - The system assumes that Postgres is running on port 5432. 
    
Publisher:  
   - Run the key_server.py in the Publisher to simulate the key server.
   - Run the publisher_contracor_api.py to simulate the actions of a publisher.
       - The code includes calling APIs for creating a new database, adding a new table, encrypting and adding a row of data.
        
Client:  
   - Once the data is added, you can simulate the client by running client_api.py where the client hits the publisher to get encrypted data
    and the key server to get the key. It ultimately decrypts the entire data to see the actual logs. 
    
    
## Interesting things!
I worked on creating a new language (format) for dynamically creating encrypted tables in Postgres using SQLalchemy. In case you are interested, check the new way of defining schema in `schema.py` and the actual implemenation in the `publisher's code.`
    