from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response, StreamingResponse
from typing import Any, Dict, List, Literal, Optional, Union
import os, time
import pandas as pd
from db import Database
from model import Base, BTRTable, BTRUser
from sqlalchemy import Column, Integer, String, Table
import matplotlib.pyplot as plt
import io
database = Database()
engine = database.get_db_connection()
session = database.get_db_session(engine)
Base.metadata.create_all(engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

class PlotRequest(BaseModel):
    id: int # table id
    index: List[str]
    column: List[str]


class UploadSingleExcelRequest(BaseModel):
    file: UploadFile
    table_name: str


class CreateTableRequest(BaseModel):
    name: str
    column_list: List[str]


class CreateTableResponse(BaseModel):
    data: str


class DeleteTableRequest(BaseModel):
    id: int


class GetTableResponse(BaseModel):
    data: List[Dict[str, List[str]]]


class DownloadExcelRequest(BaseModel):
    id: int # table id
    column_list: List[str]


class DownloadExcelResponse(BaseModel):
    data: List[Dict[str, Any]]


class HealthResponse(BaseModel):
    status: str
    uptime: float


class HelloResponse(BaseModel):
    message: str


@app.get("/", response_model=HelloResponse)
async def hello():
    return HelloResponse(message="Hello, world!")


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="ok", uptime=time.time())


@app.post('/register')
async def register(username: str, email: str, password: str):
    session = database.get_db_session(engine)
    try:
        session.add(BTRUser(username=username, email=email, hashed_password=password))
        session.commit()
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database connection error")
    return JSONResponse(content={"data": "success"})


@app.post('/login')
async def login(username: str, password: str):
    session = database.get_db_session(engine)
    try:
        result = session.query(BTRUser).filter(BTRUser.username == username).first()
        if result.hashed_password == password:
            return JSONResponse(content={"data": "success"})
        else:
            return JSONResponse(content={"data": "fail"})
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database connection error")


@app.post('/create_table', response_model=CreateTableResponse)
async def create_table(request: CreateTableRequest):
    name = request.name
    column_list = request.column_list
    session = database.get_db_session(engine)
    print(name)
    print(column_list)
    # check table exists
    # sql = f"SELECT * FROM tables WHERE name = '{table_name}'"
    # result = session.execute(sql)
    result = session.query(BTRTable).filter(BTRTable.name == name).all()
    print(result)

    if len(result) > 0:
        return JSONResponse(content={"data": "table exists!"})

    try:
        columns_num = len(column_list)
        new_columns = [
            Column('id', Integer, primary_key=True, index=True),
        ] + [
            Column(name, String(50)) for name in column_list
        ]
        table = Table(name, Base.metadata, *new_columns)

        Base.metadata.create_all(engine)
        session.add(BTRTable(name=name, column_list=",".join(column_list)))
        session.commit()
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database connection error")

    return CreateTableResponse(data="success")


@app.post('/delete_table')
async def delete_table(request: DeleteTableRequest):
    id = request.id
    session = database.get_db_session(engine)
    try:
        name = session.query(BTRTable).filter(BTRTable.id == id).first().name
        session.query(BTRTable).filter(BTRTable.id == id).delete()
        
        session.commit()
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database connection error")
    return JSONResponse(content={"data": "success"})


@app.get('/get_tables')
async def get_tables():
    session = database.get_db_session(engine)
    result = session.query(BTRTable).all()
    data=[{item.name, item.column_list} for item in result]
    print(data)
    return JSONResponse(content={"data":[{"display_name": item.display_name, "owner": item.owner, "deleted": item.deleted, "column_list": item.column_list.split(',')} for item in result] })
    # return GetTableResponse(data=[{"name": item.name, "column_list": item.column_list.split(',')} for item in result])
    # return JSONResponse(content={"data": [item.name for item in result]})


@app.post('/show_table_detail')
async def show_table_detail(table_id: int):
    session = database.get_db_session(engine)
    table_name = session.query(BTRTable).filter(BTRTable.id == table_id).first().name
    df = pd.read_sql_table(table_name, engine)
    print(df)
    return JSONResponse(content={"data": df.to_json()})


# 接收单个excel文件
@app.post("/create_table_from_excel")
async def create_table_from_excel(table_name: str = Form(...), file: UploadFile = File(...)):
    df = ""
    
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="File is not an Excel document.")
    try:
        # 使用pandas读取上传的Excel文件
        df = pd.read_excel(file.file, engine='openpyxl')
        column_list = df.columns
        session = database.get_db_session(engine)
        
        # check table exists
        # sql = f"SELECT * FROM tables WHERE name = '{table_name}'"
        # result = session.execute(sql)
        result = session.query(BTRTable).filter(BTRTable.name == table_name).all()
        print(result)

        if len(result) > 0:
            return JSONResponse(content={"data": "table exists!"})

        try:
            columns_num = len(column_list)
            new_columns = [
                Column('id', Integer, primary_key=True, index=True),
            ] + [
                Column(name, String(50)) for name in column_list
            ]
            table = Table(table_name, Base.metadata, *new_columns)
            Base.metadata.create_all(engine)

            session.add(BTRTable(name=table_name, column_list=",".join(column_list)))
            session.commit()

            df.to_sql(table_name, engine, if_exists='append', index=False)
            session.commit()
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail="Database connection error")

        data = df.to_dict(orient="records")
    except Exception as e:
        # 如果在读取过程中发生错误，返回错误信息
        return HTTPException(status_code=500, detail=str(e))
    
    return JSONResponse(content={"data": df.to_json()})


@app.post("/insert_single_excel_to_table")
async def insert_single_excel_to_table(table_id: int, file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="File is not an Excel document.")
    try:
        session = database.get_db_session(engine)
        table_name = session.query(BTRTable).filter(BTRTable.id == table_id).first().name
        column_list = session.query(BTRTable).filter(BTRTable.id == table_id).first().column_list.split(',')
        # 使用pandas读取上传的Excel文件
        df = pd.read_excel(file.file, engine='openpyxl')
        if df.columns.tolist() != column_list:
            return HTTPException(status_code=500, detail="column list not match")
        # TODO: 检测异常值
        df.to_sql(table_name, engine, if_exists='append', index=False)
        session.commit()
        print(df)
       
    except Exception as e:
        # 如果在读取过程中发生错误，返回错误信息
        return HTTPException(status_code=500, detail=str(e))
  
    # 返回解析后的数据
    return JSONResponse(content={"data": "insert success!"})

@app.post("/insert_multi_excel_to_table")
async def insert_multi_excel_to_table(table_id:int, files: List[UploadFile] = File(...)):
    session = database.get_db_session(engine)
    table_name = session.query(BTRTable).filter(BTRTable.id == table_id).first().name
    column_list = session.query(BTRTable).filter(BTRTable.id == table_id).first().column_list.split(',')
    data = []
    for file in files:
        print(file.filename)
        if not file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="File is not an Excel document.")
        try:
            df = pd.read_excel(file.file, engine='openpyxl')
            print(df)
            if df.columns.tolist() != column_list:
                return HTTPException(status_code=500, detail="column list not match")
            df.to_sql(table_name, engine, if_exists='append', index=False)
            session.commit()
        except Exception as e:
            # 如果在读取过程中发生错误，返回错误信息
            return HTTPException(status_code=500, detail=str(e))
  
    # 返回解析后的数据
    print(data)
    return JSONResponse(content={"data": "success"})

@app.post("/download_excel")
async def download_excel(table_id: int, column_list: List[str] = None):
    # 读取Excel文件
    try:
        table_name = session.query(BTRTable).filter(BTRTable.id == table_id).first().name
        df = pd.read_sql_table(table_name, engine)

        if column_list:
            df = df[column_list]
        print(df)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            #writer.save()

        output.seek(0)

        # def iterfile():
        #     yield from output.getvalue()

        response = StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        response.headers["Content-Disposition"] = "attachment; filename=export.xlsx"
        return response

    except Exception as e:
        # 如果在读取过程中发生错误，返回错误信息
        return HTTPException(status_code=500, detail=str(e))

    # 返回数据
    return JSONResponse(content={"data": data})


@app.post("/plot_scatter")
async def plot_scatter(request: PlotRequest):
    id = request.id
    index = request.index
    column = request.column
    print(index, column)
    session = database.get_db_session(engine)
    try:
        table_name = session.query(BTRTable).filter(BTRTable.id == id).first().name
        print(table_name)
        df = pd.read_sql_table(table_name, engine)
        # all_result = session.query(table_name).all()
        
        # df = pd.DataFrame(all_result)
        print(df)
        plt.scatter(df[column[0]], df[column[1]], c="g",marker="o", edgecolors="k", s=50)
        plt.xlabel(column[0])
        plt.ylabel(column[1])
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        return Response(content=img.getvalue(), media_type="image/png")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database connection error")


@app.post("/calculate_correlation")
async def calculate_correlation(table_id:int, column_list: List[str]):
    session = database.get_db_session(engine)
    table_name = session.query(BTRTable).filter(BTRTable.id == table_id).first().name
    df = pd.read_sql_table(table_name, engine)
    # df = df[column_list]
    df[column_list] = df[column_list].apply(pd.to_numeric, errors='coerce')
    corr = df[column_list[0]].corr(df[column_list[1]])
    print(corr)
    return JSONResponse(content={"data": corr})