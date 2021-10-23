# Exercise_submit
## Technology used:
- Python3.8
- FastAPI
- Pymongo
- Pandas
- ...
## API doc
- http://localhost:8000/docs
## Mongo Info:
- Database: university
- user: admin
- pass: 123456a
- collection: 
    *  yearly: for every year university
    *  universities: for latest data info for university
    *  top_university: for latest data top.
    
- Index by year.
## Data profiling
- [analyst_report.html]
## How to run:
- pip install -r requirements.txt
- uvicorn main:app --host 0.0.0.0 --port 8000
## What will need to do:
- Develop unittest
- Optimize time
- Dockerize
