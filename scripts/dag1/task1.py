import time
import requests
import datetime as datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP


Base = declarative_base()

class Weather(Base):
    __tablename__ = 'df_weather'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    cities = Column(VARCHAR(100), nullable=False)
    temperature = Column(Float, nullable=False)
    cloudiness = Column(Integer, nullable=False)
    create_time = Column(TIMESTAMP, nullable=False, index=True)

API_KEY = 'dcdc9a5e10f14ba092d141240243004'
URL = f'http://api.weatherapi.com/v1/current.json?key='
SQLALCHEMY_DATABASE_URI = f"postgresql://nik:675756@192.168.9.130:5432/test1"


engine = create_engine(SQLALCHEMY_DATABASE_URI)
Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

# # читаем список городов
# with open(r'config\list_cities.yaml') as file:
#     list_cities = yaml.load(file, Loader=yaml.FullLoader)
list_cities = ["- 51.7727,55.0988",
               "- 59.9386,30.3141",
               "- 56.8519,60.6122",
               "- 55.0415,82.9346"]

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