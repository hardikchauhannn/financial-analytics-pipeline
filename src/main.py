"""
main.py - Main execution script for Financial Analytics Pipeline
Hardik Chauhan
"""

from database import DatabaseManager, StockPrice
from data_fetcher import StockDataFetcher
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FinancialAnalyticsPipeline:
    def __init__(self):
        logger.info("Initializing Financial Analytics Pipeline...")
        
        self.db_manager = DatabaseManager()
        self.data_fetcher = StockDataFetcher()
        
        self.symbols = ['JPM', 'BAC', 'WFC', 'GS', 'MS']
        logger.info(f"Tracking {len(self.symbols)} symbols: {', '.join(self.symbols)}")
    
    def setup_database(self):
        logger.info("Setting up database...")
        
        if not self.db_manager.test_connection():
            logger.error("Database connection failed")
            return False
        
        if not self.db_manager.create_tables():
            logger.error("Table creation failed")
            return False
        
        logger.info("✅ Database setup complete")
        return True
    
    def fetch_and_store_data(self):
        logger.info("Starting data fetch and storage...")
        
        all_data = self.data_fetcher.fetch_multiple_symbols(self.symbols)
        
        session = self.db_manager.get_session()
        total_stored = 0
        total_skipped = 0
        
        try:
            for symbol, records in all_data.items():
                logger.info(f"Storing data for {symbol}...")
                
                for record in records:
                    existing = session.query(StockPrice).filter_by(
                        symbol=record['symbol'],
                        date=record['date']
                    ).first()
                    
                    if existing:
                        total_skipped += 1
                        continue
                    
                    stock_price = StockPrice(**record)
                    session.add(stock_price)
                    total_stored += 1
                
                session.commit()
                logger.info(f"✅ Stored data for {symbol}")
            
            logger.info(f"\n📊 Summary:")
            logger.info(f"   Records stored: {total_stored}")
            logger.info(f"   Records skipped: {total_skipped}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing data: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def generate_analytics_report(self):
        logger.info("\n" + "=" * 70)
        logger.info("FINANCIAL ANALYTICS REPORT")
        logger.info("=" * 70)
        
        session = self.db_manager.get_session()
        
        try:
            stats = self.db_manager.get_table_stats()
            
            if stats:
                print(f"\n📊 Database Overview:")
                print(f"   Total Records: {stats['total_records']:,}")
                print(f"   Symbols Tracked: {stats['unique_symbols']}")
                print(f"   Date Range: {stats['earliest_date']} to {stats['latest_date']}")
            
            from sqlalchemy import func
            
            print(f"\n💰 Latest Stock Prices:")
            print(f"{'Symbol':<10} {'Date':<12} {'Close Price':<15} {'Volume':<15}")
            print("-" * 60)
            
            for symbol in self.symbols:
                latest = session.query(StockPrice).filter_by(
                    symbol=symbol
                ).order_by(StockPrice.date.desc()).first()
                
                if latest:
                    print(f"{latest.symbol:<10} {latest.date.strftime('%Y-%m-%d'):<12} "
                          f"${latest.close_price:<14.2f} {latest.volume:>14,}")
            
            print(f"\n📈 Performance Metrics:")
            print(f"{'Symbol':<10} {'Daily Change':<15} {'% Change':<15}")
            print("-" * 45)
            
            for symbol in self.symbols:
                last_two = session.query(StockPrice).filter_by(
                    symbol=symbol
                ).order_by(StockPrice.date.desc()).limit(2).all()
                
                if len(last_two) == 2:
                    current = last_two[0]
                    previous = last_two[1]
                    
                    change = current.close_price - previous.close_price
                    pct_change = (change / previous.close_price) * 100
                    
                    print(f"{symbol:<10} ${change:+.2f:<13} {pct_change:+.2f}%")
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ Report generation complete")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
        finally:
            session.close()
    
    def run(self):
        logger.info("\n" + "=" * 70)
        logger.info("STARTING FINANCIAL ANALYTICS PIPELINE")
        logger.info("=" * 70 + "\n")
        
        if not self.setup_database():
            logger.error("Pipeline aborted")
            return False
        
        if not self.fetch_and_store_data():
            logger.error("Pipeline aborted")
            return False
        
        self.generate_analytics_report()
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 70)
        
        return True


if __name__ == "__main__":
    pipeline = FinancialAnalyticsPipeline()
    success = pipeline.run()
    
    if not success:
        exit(1)
