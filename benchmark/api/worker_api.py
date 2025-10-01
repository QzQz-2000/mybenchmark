"""FastAPI-based Worker API for distributed benchmark execution."""

import asyncio
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

from ..core.worker import BaseWorker, ProducerTask, ConsumerTask
from ..core.results import WorkerResult
from ..utils.logging import setup_logging, LoggerMixin


# API Models
class ProducerTaskRequest(BaseModel):
    """Producer task request model - Java OMB continuous mode."""
    task_id: str
    topic: str
    message_size: int = Field(ge=1)
    rate_limit: int = Field(default=0, ge=0)
    payload_data: Optional[str] = None  # Base64 encoded
    key_pattern: str = "NO_KEY"
    properties: Dict[str, Any] = Field(default_factory=dict)


class ConsumerTaskRequest(BaseModel):
    """Consumer task request model - Java OMB continuous mode."""
    task_id: str
    topics: List[str]
    subscription_name: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class TaskStatusResponse(BaseModel):
    """Task status response model."""
    task_id: str
    status: str
    result_available: bool


class HealthResponse(BaseModel):
    """Health check response model."""
    worker_id: str
    status: str
    running_tasks: int
    completed_tasks: int
    system_stats: Dict[str, Any]


class WorkerAPI(LoggerMixin):
    """Worker API server."""

    def __init__(self, worker: BaseWorker, host: str = "0.0.0.0", port: int = 8080):
        super().__init__()
        self.worker = worker
        self.host = host
        self.port = port
        self.app = FastAPI(
            title="Python OpenMessaging Benchmark Worker",
            description="Distributed benchmark worker API",
            version="0.1.0"
        )
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes."""

        @self.app.get("/health", response_model=HealthResponse)
        async def health_check():
            """Health check endpoint."""
            return self.worker.health_check()

        @self.app.post("/producer/start")
        async def start_producer_tasks(
            tasks: List[ProducerTaskRequest],
            background_tasks: BackgroundTasks
        ):
            """Start producer tasks (Java OMB style - non-blocking).

            Tasks will run continuously until stop signal is sent.
            This endpoint returns immediately after starting tasks.
            """
            try:
                # Convert API models to internal models
                producer_tasks = []
                for task_req in tasks:
                    payload_data = None
                    if task_req.payload_data:
                        import base64
                        payload_data = base64.b64decode(task_req.payload_data)

                    producer_task = ProducerTask(
                        task_id=task_req.task_id,
                        topic=task_req.topic,
                        message_size=task_req.message_size,
                        rate_limit=task_req.rate_limit,
                        payload_data=payload_data,
                        key_pattern=task_req.key_pattern,
                        properties=task_req.properties
                    )
                    producer_tasks.append(producer_task)

                # Start tasks (returns immediately, tasks run continuously)
                await self.worker.run_producer_tasks(producer_tasks)

                # Return success status immediately
                return {
                    "status": "started",
                    "tasks_count": len(producer_tasks),
                    "message": "Producer tasks started successfully"
                }

            except Exception as e:
                self.logger.error(f"Failed to start producer tasks: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/consumer/start")
        async def start_consumer_tasks(
            tasks: List[ConsumerTaskRequest],
            background_tasks: BackgroundTasks
        ):
            """Start consumer tasks (Java OMB style - non-blocking).

            Tasks will run continuously until stop signal is sent.
            This endpoint returns immediately after starting tasks.
            """
            try:
                # Convert API models to internal models
                consumer_tasks = []
                for task_req in tasks:
                    consumer_task = ConsumerTask(
                        task_id=task_req.task_id,
                        topics=task_req.topics,
                        subscription_name=task_req.subscription_name,
                        properties=task_req.properties
                    )
                    consumer_tasks.append(consumer_task)

                # Start tasks (returns immediately, tasks run continuously)
                await self.worker.run_consumer_tasks(consumer_tasks)

                # Return success status immediately
                return {
                    "status": "started",
                    "tasks_count": len(consumer_tasks),
                    "message": "Consumer tasks started successfully"
                }

            except Exception as e:
                self.logger.error(f"Failed to start consumer tasks: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/task/{task_id}/status", response_model=TaskStatusResponse)
        async def get_task_status(task_id: str):
            """Get task status."""
            status = self.worker.get_task_status(task_id)
            return TaskStatusResponse(**status)

        @self.app.get("/task/{task_id}/result")
        async def get_task_result(task_id: str):
            """Get task result."""
            result = self.worker.get_task_result(task_id)
            if result is None:
                raise HTTPException(status_code=404, detail="Task result not found")

            return self._serialize_result(result)

        @self.app.get("/consumer/{task_id}/ready")
        async def check_consumer_ready(task_id: str):
            """Check if consumer is ready (subscribed and has partition assignment)."""
            status = self.worker.get_consumer_ready_status(task_id)
            if status is None:
                raise HTTPException(status_code=404, detail="Consumer task not found")
            return status

        @self.app.get("/system/stats")
        async def get_system_stats():
            """Get system statistics."""
            return self.worker.get_system_stats()

        @self.app.post("/system/reset")
        async def reset_worker():
            """Reset worker state."""
            try:
                # This could be implemented to clean up completed tasks, etc.
                return {"status": "reset_completed"}
            except Exception as e:
                self.logger.error(f"Failed to reset worker: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/tasks/stop-all")
        async def stop_all_tasks():
            """Stop all running tasks - Java OMB style.

            Triggers stop signal for all producer and consumer processes.
            """
            try:
                # Check if worker has stop_all_tasks method (multiprocess worker)
                if hasattr(self.worker, 'stop_all_tasks'):
                    await self.worker.stop_all_tasks()
                    return {"status": "stop_signal_sent", "message": "Stop signal sent to all tasks"}
                else:
                    return {"status": "not_supported", "message": "Worker does not support stop-all operation"}
            except Exception as e:
                self.logger.error(f"Failed to stop all tasks: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/tasks/wait-completion")
        async def wait_for_completion():
            """Wait for all tasks to complete and collect results.

            Should be called after /tasks/stop-all.
            Returns results from all processes.
            """
            try:
                # Check if worker has wait_for_completion method (multiprocess worker)
                if hasattr(self.worker, 'wait_for_completion'):
                    results = await self.worker.wait_for_completion()
                    return {
                        "status": "completed",
                        "tasks_count": len(results),
                        "results": [self._serialize_result(r) for r in results]
                    }
                else:
                    return {"status": "not_supported", "message": "Worker does not support wait-completion operation"}
            except Exception as e:
                self.logger.error(f"Failed to wait for completion: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.on_event("startup")
        async def startup_event():
            """Startup event handler."""
            await self.worker.start()
            self.logger.info(f"Worker API started on {self.host}:{self.port}")

        @self.app.on_event("shutdown")
        async def shutdown_event():
            """Shutdown event handler."""
            await self.worker.stop()
            self.logger.info("Worker API shutdown")

    def _serialize_result(self, result: WorkerResult) -> Dict[str, Any]:
        """Serialize WorkerResult to JSON-compatible dict."""
        return {
            "worker_id": result.worker_id,
            "worker_url": result.worker_url,
            "task_type": result.task_type,
            "start_time": result.start_time,
            "end_time": result.end_time,
            "duration_seconds": result.duration_seconds,
            "throughput": {
                "total_messages": result.throughput.total_messages,
                "total_bytes": result.throughput.total_bytes,
                "duration_seconds": result.throughput.duration_seconds,
                "messages_per_second": result.throughput.messages_per_second,
                "bytes_per_second": result.throughput.bytes_per_second,
                "mb_per_second": result.throughput.mb_per_second,
            },
            "latency": {
                "count": result.latency.count,
                "min_ms": result.latency.min_ms,
                "max_ms": result.latency.max_ms,
                "mean_ms": result.latency.mean_ms,
                "median_ms": result.latency.median_ms,
                "p50_ms": result.latency.p50_ms,
                "p95_ms": result.latency.p95_ms,
                "p99_ms": result.latency.p99_ms,
                "p99_9_ms": result.latency.p99_9_ms,
            },
            "errors": {
                "total_errors": result.errors.total_errors,
                "error_rate": result.errors.error_rate,
                "error_types": result.errors.error_types,
            },
            "metadata": result.metadata
        }

    async def run(self):
        """Run the worker API server."""
        config = uvicorn.Config(
            app=self.app,
            host=self.host,
            port=self.port,
            log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()


# Utility function for standalone worker server
async def run_worker_server(
    worker: BaseWorker,
    host: str = "0.0.0.0",
    port: int = 8080
):
    """Run worker server."""
    api = WorkerAPI(worker, host, port)
    await api.run()