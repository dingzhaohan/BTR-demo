from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException

MYSQL_URL = "mysql+mysqlconnector://root:%)(&AxCyEdUuu88112765!!...@localhost:3306/btr"
POOL_SIZE = 20
POOL_RECYCLE = 3600
POOL_TIMEOUT = 30
MAX_OVERFLOW = 10
CONNECT_TIMEOUT = 60


class Database():

    def __init__(self) -> None:
        self.connection_is_active = False
        self.engine = None

    def get_db_connection(self):
        if self.connection_is_active == False:
            connect_args = {"connect_timeout": CONNECT_TIMEOUT}
            try:
                self.engine = create_engine(
                    MYSQL_URL,
                    pool_size=POOL_SIZE,
                    pool_recycle=POOL_RECYCLE,
                    pool_timeout=POOL_TIMEOUT,
                    max_overflow=MAX_OVERFLOW,
                    connect_args=connect_args,
                )
                return self.engine
            except Exception as e:
                print(e)
                raise HTTPException(status_code=500, detail="Database connection error")
        return self.engine

    def get_db_session(self, engine):
        try:
            Session = sessionmaker(bind=engine)
            session = Session()
            
            return session
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail="Database connection error")







