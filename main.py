# https://www.vinta.com.br/blog/2021/etl-with-asyncio-asyncpg/

import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import select, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
from sqlalchemy import Column, String, Integer, Numeric, DateTime
import datetime
from typing import *
import time

# Define database
# access database using both synchronous and asynchronous approaches

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

sync_engine = create_engine(DB_URL)
Session = sessionmaker(sync_engine)
syn_session = Session()

# clear table: records_transformed
syn_session.query(RecordTransform).delete()
syn_session.commit()

Base.metadata.create_all(sync_engine)  # create new database/tables

Async_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5434/stockdata"
async_engine = create_async_engine(Async_DB_URL, echo=False)

async_session = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


async def extract(session: AsyncSession, total_size: int = 1000):
    '''
    Extract data from database: async generator
    :param session:
    :param total_size:
    :return:
    '''
    query = select(Record).limit(total_size)
    async_result = await session.stream(query)
    async for record in async_result:
        yield record

async def producer(queue: asyncio.Queue, total_size: int = 1000) -> None:
    '''
    Producer: put extracted items to the queue
    :param queue:
    :return:
    '''
    async with async_session() as session:
        async for record in extract(session, total_size=total_size):
            record = record[0]
            await queue.put(record)
        # append "None" as the last token
        await queue.put(None)
    # return "producer is finished."

def transform(batch: List[Record]) -> List[RecordTransform]:
    '''
    Dummy transformation function (which is assumed to be more time-consuming)
    :param batch:
    :return:
    '''
    res = []
    for record in batch:
        time.sleep(0.005)
        transformed = RecordTransform(Date=record.Date,
                                      Ticker=record.Ticker,
                                      DateTicker=record.Date.date().strftime("%m/%d/%Y") + "_" + record.Ticker)
        res.append(transformed)
    return res

async def consumer(loop,
                   pool: ThreadPoolExecutor,
                   queue: asyncio.Queue,
                   batch_size: int = 32):
    '''
    Consumer: fetch data from queue and create async tranformation tasks
    :param loop:
    :param pool:
    :param queue:
    :param batch_size:
    :return:
    '''
    async with async_session() as session:
        task_set = set()
        batch = []
        while True:
            record = await queue.get()
            queue.task_done()

            if record is not None:
                batch.append(record)

            if len(batch) == batch_size or record is None:
                task = loop.run_in_executor(pool, transform, batch)
                task_set.add(task)
                batch = []
                # ensure that the number of running tasks is less than pool._max_workers
                if len(task_set) >= pool._max_workers:
                    done_set, task_set = await asyncio.wait(task_set,
                                                            return_when=asyncio.FIRST_COMPLETED)

                    if len(done_set)>0:
                        for done_task in done_set:
                            res = await done_task
                            session.add_all(res)
                        await session.commit()

                if record is None:
                    break

        # handle the remaining tasks when the queue is empty
        remaining_results = await asyncio.gather(*task_set)
        for results in remaining_results:
            session.add_all(results)
        await session.commit()
        # print("consumer finished")

async def etl() -> None:
    '''
    Extract-transform-load pipeline
    1) Create threadpool for transformation tasks
    2) Start async producer-consumer program
    :return:
    '''
    with ThreadPoolExecutor(max_workers=2) as pool:
        queue = asyncio.Queue(maxsize=1000)
        loop = asyncio.get_running_loop()
        await asyncio.gather(producer(queue, total_size=1000),
                             consumer(loop, pool, queue),
                             )

asyncio.run(etl())