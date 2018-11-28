import uuid
import time
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base

import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float

DB_URL = 'mysql+pymysql://root:123456@localhost:3306/gdelt?charset=utf8'

Base = automap_base()

engine = create_engine(DB_URL,
                       encoding='utf-8', echo=True, pool_pre_ping=True)
Base.prepare(engine, reflect=True)


GdeltTask = Base.classes.gdelt_task



def get_session():
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()

if __name__ == "__main__":
    session = get_session()
    #新增
    # data1= GdeltTask(url="12345678910")
    # session.add(data1)
    # session.commit()
    # session.close()
    #查询和修改
    data2 = session.query(GdeltTask).filter(GdeltTask.url == "123456789").first()
    print(data2.url)
    print(data2.file_name)
    # data2.file_name = "测试"
    # session.commit()
    # session.query(GdeltTask).filter(GdeltTask.url == "123456789").one()
    # print(data2.url)
    # print(data2.file_name)
    # session.close
