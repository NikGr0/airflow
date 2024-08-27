import datetime as datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
import argparse
import time
import requests


parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date")
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port")
args = parser.parse_args()

print('date = ' + str(args.date))
print('host = ' + str(args.host))
print('dbname = ' + str(args.dbname))
print('user = ' + str(args.user))
print('jdbc_password = ' + str(args.jdbc_password))
print('port = ' + str(args.port))

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)


Base = declarative_base()

class Weather(Base):
    __tablename__ = 'df_weather'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    cities = Column(VARCHAR(100), nullable=False)
    temperature = Column(Float, nullable=False)
    cloudiness = Column(Integer, nullable=False)
    create_time = Column(TIMESTAMP, nullable=False, index=True)

SQLALCHEMY_DATABASE_URI = f"postgresql://{str(v_user)}:{str(v_password)}@{str(v_host)}:{str(v_port)}/{str(v_dbname)}"


engine = create_engine(SQLALCHEMY_DATABASE_URI)

Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

list_cities = ["51.7727,55.0988",
               "59.9386,30.3141",
               "56.8519,60.6122",
               "55.0415,82.9346"]

# получаем данные
weather = []
for i in list_cities:
    r = requests.get(url=URL + f'{API_KEY}&q={i}')
    result = r.json()

    city = result.get('location').get('name')
    temp = result.get('current').get('temp_c')
    cloud = result.get('current').get('cloud')

    new_record = Weather(
                cities=city,
                temperature=temp,
                cloudiness=cloud,
                create_time=datetime.datetime.now()
                )

    session_local.add(new_record)
    time.sleep(1)

session_local.commit()