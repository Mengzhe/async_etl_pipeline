# https://www.vinta.com.br/blog/2021/etl-with-asyncio-asyncpg/

import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import select, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import text
from sqlalchemy import Column, String, Integer, Numeric, DateTime
import datetime
import pydantic
from typing import *


DB_URL = "postgresql://postgres:postgres@localhost:5434/stockdata"
Base = declarative_base()

class Record(Base):
    __tablename__ = "records"

    Date = Column(DateTime, primary_key=True)
    Ticker = Column(String, primary_key=True, nullable=False)
    Open = Column(Numeric)
    Close = Column(Numeric)
    High = Column(Numeric)
    Low = Column(Numeric)
    Volume = Column(Integer)

class RecordTransform(Base):
    __tablename__ = "records_transformed"
    Date = Column(DateTime, primary_key=True)
    Ticker = Column(String, primary_key=True, nullable=False)
    DateTicker = Column(String, nullable=False)

class RecordModel_Transformed(pydantic.BaseModel):
    Date: datetime.datetime
    Ticker: str
    DateTicker: Optional[str]

Async_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5434/stockdata"
async_engine = create_async_engine(Async_DB_URL, echo=False)

async_session = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

# async def test_db():
#     async with async_session() as session:
#         query = select(Record).limit(10)
#         query_res = await session.execute(query)
#         query_res = query_res.all()
#         query_res = [(q.Ticker, q.Date.date().strftime("%m/%d/%Y")) for (q,) in query_res]
#         return query_res

async def test_db():
    async with async_session() as session:
        async for record in extract(session):
            record = record[0]
            # print(record.Date, record.Ticker)
            # print(RecordModel_Transformed(Date=record.Date,
            #                   Ticker=record.Ticker))

async def extract(session: AsyncSession):
    # query = "SELECT name, age FROM input_table"
    query = select(Record).limit(100)
    async_result = await session.stream(query)
    # print(type(async_result))
    async for record in async_result:
        yield record

async def producer(queue: asyncio.Queue):
    async with async_session() as session:
        async for record in extract(session):
            record = record[0]
            await queue.put()
        await queue.put(None)

async def etl():
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()



# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(test_db())
# res = asyncio.run(test_db())
# print(res)
# loop.run_until_complete(test_db())

asyncio.run(etl())