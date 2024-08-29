from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
import datetime as datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
import time
import requests


API_KEY = 'dcdc9a5e10f14ba092d141240243004'
URL = f'http://api.weatherapi.com/v1/current.json?key='
Base = declarative_base()
list_cities = ["51.7727,55.0988",
                       "59.9386,30.3141",
                       "56.8519,60.6122",
                       "55.0415,82.9346"]

# class ForAirflowPlugin(Base):
#     __tablename__ = 'for_airflow_plugin'
#     id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
#     text_field = Column(VARCHAR(100), nullable=False)

class Weather(Base):
    __tablename__ = 'df_weather'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    cities = Column(VARCHAR(100), nullable=False)
    temperature = Column(Float, nullable=False)
    cloudiness = Column(Integer, nullable=False)
    create_time = Column(TIMESTAMP, nullable=False, index=True)

class ExampleOperator(BaseOperator):
    def __init__(self,
                 postgre_conn: Connection,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgre_conn = postgre_conn
        self.SQLALCHEMY_DATABASE_URI = f"postgresql://{postgre_conn.login}:{postgre_conn.password}@{postgre_conn.host}:{str(postgre_conn.port)}/{postgre_conn.schema}"


    def execute(self, context):
        engine = create_engine(self.SQLALCHEMY_DATABASE_URI)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session_local = SessionLocal()



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