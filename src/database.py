"""
database.py - Database connection and schema management
"""

import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Index, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
Base = declarative_base()


class StockPrice(Base):
    __tablename__ = 'stock_prices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    
    __table_args__ = (
        Index('idx_symbol_date', 'symbol', 'date', unique=True),
    )
    
    def __repr__(self):
        return f"<StockPrice(symbol={self.symbol}, date={self.date}, close={self.close_price})>"


class DatabaseManager:
    def __init__(self):
        self.db_url = os.getenv('DB_URL')
        if not self.db_url:
            raise ValueError("DB_URL not found in .env file!")
        
        self.engine = create_engine(self.db_url, pool_pre_ping=True, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        logger.info("Database connection established")
    
    def create_tables(self):
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def get_session(self):
        return self.SessionLocal()
    
    def test_connection(self):
        try:
            session = self.get_session()
            session.execute(text("SELECT 1"))
            session.close()
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_table_stats(self):
        try:
            session = self.get_session()
            total_records = session.query(StockPrice).count()
            unique_symbols = session.query(StockPrice.symbol).distinct().count()
            
            from sqlalchemy import func
            date_range = session.query(
                func.min(StockPrice.date).label('earliest'),
                func.max(StockPrice.date).label('latest')
            ).first()
            
            session.close()
            
            return {
                'total_records': total_records,
                'unique_symbols': unique_symbols,
                'earliest_date': date_range.earliest,
                'latest_date': date_range.latest
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return None


if __name__ == "__main__":
    print("Testing Database Module...")
    print("-" * 50)
    
    try:
        db_manager = DatabaseManager()
        print("[SUCCESS] Database manager initialized")
        
        if db_manager.test_connection():
            print("[SUCCESS] Database connection successful")
        else:
            print("[ERROR] Database connection failed")
            exit(1)
        
        if db_manager.create_tables():
            print("[SUCCESS] Tables created/verified")
        else:
            print("[ERROR] Table creation failed")
            exit(1)
        
        stats = db_manager.get_table_stats()
        if stats:
            print("\n[STATS] Database Statistics:")
            print(f"   Total Records: {stats['total_records']}")
            print(f"   Unique Symbols: {stats['unique_symbols']}")
            print(f"   Date Range: {stats['earliest_date']} to {stats['latest_date']}")
        
        print("\n" + "=" * 50)
        print("[SUCCESS] All database tests passed!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        print("\nCheck your .env file")
        exit(1)
