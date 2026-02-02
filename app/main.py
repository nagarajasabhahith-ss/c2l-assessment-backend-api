import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="Cognos to Looker Migration Assessment Service API",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting {settings.APP_NAME} v{settings.VERSION}")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    
    # Create DB tables (non-blocking: app starts even if DB is unreachable)
    from sqlalchemy.exc import OperationalError
    from app.db.session import engine, Base
    import app.models  # noqa: F401 - Import models to register them

    logger.info("Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created.")
    except OperationalError as e:
        logger.warning(
            "Database unreachable at startup (tables not created). "
            "Check DATABASE_URL and, on Cloud Run, ensure --add-cloudsql-instances is set. Error: %s",
            e,
        )


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application")


@app.get("/")
async def root():
    return {
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT
    }


# Import and include routers
from app.api import auth, assessments, files, errors, bigquery

app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(assessments.router, prefix="/api/assessments", tags=["Assessments"])
app.include_router(files.router, prefix="/api", tags=["Files"])
app.include_router(errors.router, prefix="/api", tags=["Errors"])
app.include_router(bigquery.router, prefix="/api", tags=["BigQuery"])

# Results router
from app.api import results
app.include_router(results.router, prefix="/api", tags=["Results"])

