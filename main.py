import json
from typing import Set, Dict, List, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, insert, update, delete
from datetime import datetime
from pydantic import BaseModel, field_validator, Field, validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

app = FastAPI()

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)

class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime
    user_id: int

    @classmethod
    @validator("timestamp", pre=True)
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )

class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime
    user_id: int

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


subscriptions: Set[WebSocket] = set()

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    try:
        db = SessionLocal()
        for element in data:
            road_state = element.road_state
            agent_data = element.agent_data
            user_id = agent_data.user_id
            accelerometer = agent_data.accelerometer
            gps = agent_data.gps
            timestamp = datetime.now()

            query = insert(processed_agent_data).values(
                road_state=road_state,
                user_id=user_id,
                x=accelerometer.x,
                y=accelerometer.y,
                z=accelerometer.z,
                latitude=gps.latitude,
                longitude=gps.longitude,
                timestamp=timestamp
            )
            db.execute(query)

            await send_data_to_subscribers(user_id, element.json())

        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
    return {"message": "Data was succesfully created"}


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    try:
        db = SessionLocal()
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        data = db.execute(query).fetchone()
        if data is None:
            raise HTTPException(status_code=404, detail="Not found")
        return data
    finally:
        db.close()


@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    try:
        db = SessionLocal()
        query = select(processed_agent_data)
        data = db.execute(query).fetchall()
        return data
    finally:
        db.close()


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(
        processed_agent_data_id: int,
        data: ProcessedAgentData
):
    try:
        db = SessionLocal()
        road_state = data.road_state
        agent_data = data.agent_data
        accelerometer = agent_data.accelerometer
        gps = agent_data.gps
        user_id = agent_data.user_id
        timestamp = datetime.now()
        update_query = (
            update(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(
                road_state=road_state,
                user_id=user_id,
                x=accelerometer.x,
                y=accelerometer.y,
                z=accelerometer.z,
                latitude=gps.latitude,
                longitude=gps.longitude,
                timestamp=timestamp
            )
        )

        db.execute(update_query)
        db.commit()
        updated_data = db.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)).fetchone()
        return updated_data
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    try:
        db = SessionLocal()

        deleteData = db.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)).fetchone()

        if deleteData is None:
            raise HTTPException(status_code=404, detail="Not found")
        delete_query = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        db.execute(delete_query)
        db.commit()

        return deleteData
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)