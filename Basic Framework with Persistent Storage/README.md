# ds_assignment_1

Distributed Systems Assignment 1 
-Abhishek Kumar Sah
-Aditya Piyush
-Jayesh
-Saumyak Raj

## Code structure
* `DatabaseManage`-> Manages database
* `Library` -> Library implementation of consumer and producer
* `Server` -> Flask Application for part A and part B
* `test_asgn1`-> Contains provided test cases .
* `tests` -> Contains Log files and shell script for testing part A and part B


### Setting up Environment
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### Setting up database
```bash
sudo systemctl start postgresql
sudo -iu postgres psql < DatabaseManage/StartPostgresDatabase.sql
```

## Testing
* For part A
```bash
python Server/PartA.py
./tests/TestServerA.py
```

* For Part B
```bash
python Server/PartB.py
./tests/TestServerB.py
```
Report - https://docs.google.com/document/d/1jadrp822a1gdUzYjc8qj9vuJ-OLo27MWRkpWv2BHfJs/edit
    
