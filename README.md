# Fictional Guacamole

Subscribe to gdax and poloniex exchange websockets and save aggregated orderbooks and trades into the db

## PREREQUISITES
What things you need to install the software and how to install them

```
python3.6
virtualenv
redis-server
```


## Installing
A step by step series of examples that tell you have to get a development env running

1. Clone the Project Repo Using `git clone`

2. Create VirtualEnv Using Python3 `virtualenv --no-site-packages venv -p python3.6`

3. Activate Environment `source venv/bin/activate`

4. Change dir `cd fictional_guacamole`

5. Run `pip install -r requirements.txt`

6. Ubuntu install redis-server Run `sudo apt-get install redis-server`

5. Run worker `arq redis_worker.py`

6. Run gdax - In new tab activate the env and run `python real_guac_async.py`

7. Run poloniex - In new tab activate the env and run `python polo_ws_async.py`
