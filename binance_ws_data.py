import mysql.connector
import logging
import json  # Add this line to import the json module
from binance.lib.utils import config_logging
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import config
import time

config_logging(logging, logging.DEBUG)

class BinanceWebSocketClient:
    def __init__(self):
        self.ws_client = None
        self.is_connected = False
        self.current_event_type = None

        self.db_connection = mysql.connector.connect(
            host=config.DATABASE_CONFIG['host'],
            database=config.DATABASE_CONFIG['database'],
            user=config.DATABASE_CONFIG['user'],
            password=config.DATABASE_CONFIG['password'],
        )
        self.db_cursor = self.db_connection.cursor()

    def on_message(self, ws, msg):
        #print(f"Message received: {msg}")

        self.store_data_in_mysql(msg)

    def store_data_in_mysql(self, msg):
        msg = json.loads(msg)
        if (msg.get('e') == 'kline'):
            symbol = msg['s']
            kline = msg['k']
            #只处理完结的K线
            if kline['x'] == False:
                return
            intervals = kline['i'] # 时间间隔
            start_time = kline['t'] # 开盘时间
            close_time = kline['T'] # 收盘时间
            open_price = kline['o'] # 开盘价
            close_price = kline['c'] # 收盘价
            high_price = kline['h'] # 最高价
            low_price = kline['l']  # 最低价
            volume = kline['v'] # 交易量
            number = kline['n'] # 交易次数
            
            sql = "INSERT INTO kline (symbol, intervals, start_time, close_time, open_price, close_price, high_price, low_price, volume, number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (symbol, intervals, start_time, close_time, open_price, close_price, high_price, low_price, volume, number)

            self.db_cursor.execute(sql, val)
            self.db_connection.commit()

        elif (msg.get('e') == 'forceOrder'):
            data = msg['o']
            symbol = data['s']
            side = data['S']
            price = data['p']
            quantity = data['q']
            status = data['X']
            time = data['T']
           
            sql = "INSERT INTO liquidation_order (symbol, side, price, quantity, status, time) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (symbol, side, price, quantity, status, time)
            self.db_cursor.execute(sql, val)
            self.db_connection.commit()
            
    def on_error(self, ws, error):
        print(f"Error: {error}")
        self.is_connected = False
        self.reconnect(self.current_event_type)

    def on_close(self, ws):
        print("Connection closed")
        self.is_connected = False
        self.reconnect(self.current_event_type)

    def connect(self, event_type='kline'):
        symbol = config.WEBSOCKET_CONFIG['symbol']
        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_message)

        self.current_event_type = event_type  # 保存当前的连接类型

        if event_type == 'kline':
            intervals = config.WEBSOCKET_CONFIG['kline']['intervals']
            for interval in intervals:
                self.ws_client.kline(
                    symbol=symbol,
                    id=f"kline_{interval}",
                    interval=interval
                )
        elif event_type == 'liquidation_order':
            self.ws_client.liquidation_order(
                symbol=symbol,
            )
        else:
            print(f"Unsupported event type: {event_type}")
            return

        self.is_connected = True
        self.ws_client.on_error = self.on_error
        self.ws_client.on_close = self.on_close

    def reconnect(self, event_type):
        if not self.is_connected:
            print(f"Attempting to reconnect to {event_type}...")
            time.sleep(5)  # Wait before reconnecting
            self.connect(event_type)

    def stop(self):
        if self.ws_client:
            self.ws_client.stop()
            self.is_connected = False

def main():
    client = BinanceWebSocketClient()

    client.connect(event_type='liquidation_order')
    client.connect(event_type='kline')

if __name__ == "__main__":
    main()
