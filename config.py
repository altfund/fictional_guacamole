# Common place for all configuration needed for the project


GDAX_PRODUCT_IDS = ["ETH-USD", "ETH-EUR"]
POLO_PRODUCT_IDS = ["BTC_ETH", "BTC_XMR"]

DATABASE = {
    "GDAX": {
        "DATABASE": "websocket_data_6.db",
        "SCHEMA": "schema/gdax_schema.sql",
    },
    "POLO": {
        "DATABASE": "websocket_data_6.db",
        "SCHEMA": "schema/polo_schema.sql",
    }
}