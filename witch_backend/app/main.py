"""
Witch - AI-powered Data Analyst Application
Entry point for the FastAPI application.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.endpoints import router

app = FastAPI(
    title="Witch",
    description="AI-powered Data Analyst Application",
    version="0.1.0",
)

# CORS Middleware - Allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api")


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running."""
    return {"status": "Witch is alive"}
