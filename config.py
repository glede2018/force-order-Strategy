# config.py

DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'sunny123',
    'database': 'binance_data'
}


# 配置 WebSocket 订阅的事件类型及其参数
WEBSOCKET_CONFIG = {
    'symbol': 'btcusdt',  # 需要订阅的symbol
    'kline': {
        #'intervals': ['1m']
        'intervals': ['15m','1h','4h','1d']  # 需要订阅的kline时间间隔
    },
    'liquidation_order': {
        
    }
}
