#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сборщик данных только для акций (TQBR)
Запускается 1 раз в день в 23:00
"""

import requests
import json
import time
from datetime import datetime
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shares_collector.log'),
        logging.StreamHandler()
    ]
)

class SharesDataCollector:
    def __init__(self, data_folder="moex_data"):
        self.data_dir = data_folder
        os.makedirs(os.path.join(self.data_dir, 'trades', 'shares'), exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MOEX Shares Collector/1.0'})

    def get_shares_list(self):
        """Получение списка всех акций с MOEX"""
        try:
            shares_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?iss.meta=off&securities.columns=SECID,SHORTNAME"
            response = self.session.get(shares_url)
            response.raise_for_status()
            data = response.json()

            shares = [
                {'ticker': item[0], 'name': item[1]}
                for item in data['securities']['data']
                if len(item) >= 2
            ]

            logging.info(f"Получено {len(shares)} акций")
            return shares

        except Exception as e:
            logging.error(f"Ошибка при загрузке списка акций: {e}")
            return []

    def get_trades_data(self, ticker):
        """Получение всех данных о сделках для акции за день (с пагинацией и удалением дубликатов)."""
        all_data = None
        start = 0
        page_size = 1000
        seen_tradeno = set()  # Для отслеживания уникальных сделок
        
        while True:
            try:
                url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/trades.json?start={start}'
                response = self.session.get(url)
                response.raise_for_status()
                data = response.json()
                
                trades = data.get('trades', {})
                if not trades or not trades.get('data'):
                    break
                
                # Фильтруем дубликаты по TRADENO
                unique_trades = []
                for trade in trades['data']:
                    tradeno = trade[0]  # TRADENO - первый элемент
                    if tradeno not in seen_tradeno:
                        seen_tradeno.add(tradeno)
                        unique_trades.append(trade)
                
                if all_data is None:
                    all_data = data
                    all_data['trades']['data'] = unique_trades
                else:
                    all_data['trades']['data'].extend(unique_trades)
                
                logging.info(f"Страница {start//page_size + 1}: получено {len(trades['data'])} сделок, уникальных {len(unique_trades)}")
                
                if len(trades['data']) < page_size:
                    break
                start += page_size
                
            except Exception as e:
                logging.error(f'Ошибка при получении данных по сделкам для {ticker}: {e}')
                break
        
        if all_data:
            logging.info(f"Итого для {ticker}: {len(all_data['trades']['data'])} уникальных сделок")
        
        return all_data

    def save_data(self, data, ticker, data_type='trades'):
        """Сохранение данных акций в файл с датой."""
        market_folder = os.path.join(self.data_dir, data_type, 'shares')
        os.makedirs(market_folder, exist_ok=True)

        # Добавляем дату к имени файла
        today = datetime.now().strftime('%Y-%m-%d')
        filename = os.path.join(market_folder, f"{ticker}_{data_type}_{today}.json")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logging.info(f'Данные акции {ticker} сохранены в файл: {filename}')
        except Exception as e:
            logging.error(f'Ошибка при сохранении файла {filename}: {e}')

    def collect_shares_data(self):
        """Сбор данных по всем акциям"""
        logging.info("Начало сбора данных по акциям...")
        shares = self.get_shares_list()
        
        if not shares:
            logging.error("Не удалось получить список акций")
            return

        total_shares = len(shares)
        for i, share in enumerate(shares, 1):
            ticker = share['ticker']
            logging.info(f"Обрабатывается {ticker} ({i}/{total_shares})...")

            data = self.get_trades_data(ticker)
            if data:
                self.save_data(data, ticker, 'trades')
            else:
                logging.warning(f"Нет данных для акции {ticker}")

            # Небольшая задержка между запросами
            time.sleep(1)

        logging.info("Сбор данных по акциям завершен")

def main():
    """Основная функция для запуска из bat файла"""
    collector = SharesDataCollector()
    collector.collect_shares_data()

if __name__ == '__main__':
    main() 