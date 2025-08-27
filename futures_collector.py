#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сборщик данных только для фьючерсов (FORTS)
Запускается 2 раза в день: 18:25 (дневная сессия) и 23:25 (вечерняя сессия)
Создает файлы с временными метками для последующего объединения
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
        logging.FileHandler('futures_collector.log'),
        logging.StreamHandler()
    ]
)

class FuturesDataCollector:
    def __init__(self, data_folder="moex_data"):
        self.data_dir = data_folder
        os.makedirs(os.path.join(self.data_dir, 'trades', 'futures'), exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MOEX Futures Collector/1.0'})

    def get_futures_list(self):
        """Получение списка всех фьючерсов с MOEX"""
        try:
            futures_url = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off&securities.columns=SECID,SECNAME,MATDATE"
            response = self.session.get(futures_url)
            response.raise_for_status()
            data = response.json()

            futures = [
                {'ticker': item[0], 'name': item[1], 'expiration': item[2] if len(item) > 2 else None}
                for item in data['securities']['data']
                if len(item) >= 1
            ]

            logging.info(f"Получено {len(futures)} фьючерсов")
            return futures

        except Exception as e:
            logging.error(f"Ошибка при загрузке списка фьючерсов: {e}")
            return []

    def get_trades_data(self, ticker):
        """Получение всех данных о сделках для фьючерса за день (с пагинацией и удалением дубликатов)."""
        all_data = None
        start = 0
        page_size = 1000
        seen_tradeno = set()  # Для отслеживания уникальных сделок
        
        while True:
            try:
                url = f'https://iss.moex.com/iss/engines/futures/markets/forts/securities/{ticker}/trades.json?start={start}'
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
        """Сохранение данных фьючерсов в файл с датой и временем."""
        market_folder = os.path.join(self.data_dir, data_type, 'futures')
        os.makedirs(market_folder, exist_ok=True)

        # Добавляем дату и время к имени файла
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        time_str = now.strftime('%H-%M')
        
        # Определяем сессию по времени
        if now.hour < 19:
            session = "day"  # Дневная сессия (до 19:00)
        else:
            session = "evening"  # Вечерняя сессия (после 19:00)
        
        filename = os.path.join(market_folder, f"{ticker}_{data_type}_{date_str}_{session}_{time_str}.json")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logging.info(f'Данные фьючерса {ticker} ({session} сессия) сохранены в файл: {filename}')
        except Exception as e:
            logging.error(f'Ошибка при сохранении файла {filename}: {e}')

    def collect_futures_data(self):
        """Сбор данных по всем фьючерсам"""
        logging.info("Начало сбора данных по фьючерсам...")
        futures = self.get_futures_list()
        
        if not futures:
            logging.error("Не удалось получить список фьючерсов")
            return

        total_futures = len(futures)
        for i, future in enumerate(futures, 1):
            ticker = future['ticker']
            logging.info(f"Обрабатывается {ticker} ({i}/{total_futures})...")

            data = self.get_trades_data(ticker)
            if data:
                self.save_data(data, ticker, 'trades')
            else:
                logging.warning(f"Нет данных для фьючерса {ticker}")

            # Небольшая задержка между запросами
            time.sleep(1)

        logging.info("Сбор данных по фьючерсам завершен")

def main():
    """Основная функция для запуска из bat файла"""
    collector = FuturesDataCollector()
    collector.collect_futures_data()

if __name__ == '__main__':
    main() 