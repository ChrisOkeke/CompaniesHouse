# Companies house api call for 'broker' information.

> broker.py
```
  script calls the Companies House API for information on up to 200 companies using the search term 'broker'. The response is then piped into a mysql database using two related tables - address and company - also provisioned by the script.
```
> test.py
```
  script connects to mysql db and runs sample test queries. 
```

Repro steps:

* Browse to [Companies House](https://developer.companieshouse.gov.uk/developer/applications/register) website and
  1. create an account
  2. register an application
  3. generate Api key
  4. paste your public IP address into the 'restricted IPs' box, and
  5. save

* Clone repository to your local directory
  ```
  $ mkdir companies_house
  $ cd companies_house
  $ git clone https://github.com/ChrisOkeke/companiesHouse.git
  ```
* Refactor credentials_sample.py to credentials.py, and
* Update the api key and database credentials
* Navigate to your local git folder
```
  $ pwd `to check that you are in the right location`
  $ cd ~/Desktop/broker
```
* Run broker.py script
> Command Line
```
  $ python3 broker.py
  $ python3 test.py
```
> REPL
```
  $ python3
  $ import broker, test
  $ from broker import get_broker_data, create_tables, insert_data
```
* Alternatively execute the scripts using your favourite IDE.