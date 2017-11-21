```
virtualenv --python=python3 env
source env/bin/activate
pip install -r requirements.txt
# Edit config and polo_config with desired products to track
python real_guac.py #GDAX data
python polo_ws.py #Poloniex data
```