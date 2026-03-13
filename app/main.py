from fastapi import FastAPI

from app.routers.health import router as health_router

app = FastAPI()
app.include_router(health_router)


def main():
    print("Hello from psycho-pickle!")


if __name__ == "__main__":
    main()
