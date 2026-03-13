from fastapi import FastAPI

from app.routers.health import router as health_router
from app.routers.job import router as job_router

app = FastAPI()
app.include_router(health_router)
app.include_router(job_router)


def main():
    print("Hello from psycho-pickle!")


if __name__ == "__main__":
    main()
