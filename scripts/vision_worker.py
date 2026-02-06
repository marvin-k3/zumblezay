#!/usr/bin/env python3
"""
Vision worker that claims jobs via the HTTP API, runs YOLO person detection,
and submits results back to the server.

Usage:
  .venv/bin/python scripts/vision_worker.py \\
    --base-url http://127.0.0.1:3011 \\
    --worker-id worker-1
"""

from __future__ import annotations

import argparse
import json
import time
from urllib import request as urlrequest
from urllib.parse import urljoin

import numpy as np
import cv2
from ultralytics import YOLO
import logging
import subprocess

try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        ConsoleSpanExporter,
        SimpleSpanProcessor,
    )

    OTEL_AVAILABLE = True
except Exception:
    trace = None
    OTEL_AVAILABLE = False


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def setup_tracing(service_name: str) -> None:
    if not OTEL_AVAILABLE:
        logging.debug("opentelemetry not available; tracing disabled")
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    processor = SimpleSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urlrequest.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def ffprobe_video(url: str) -> tuple[int, int, float]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,r_frame_rate",
        "-of",
        "json",
        url,
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr.strip()}")
    payload = json.loads(result.stdout)
    streams = payload.get("streams") or []
    if not streams:
        raise RuntimeError("ffprobe returned no streams")
    stream = streams[0]
    width = int(stream["width"])
    height = int(stream["height"])
    rate = stream.get("r_frame_rate", "1/1")
    if "/" in rate:
        num, den = rate.split("/")
        fps = float(num) / float(den)
    else:
        fps = float(rate)
    return width, height, fps


def stream_frames(
    url: str, width: int, height: int, sample_fps: float
) -> tuple[float, np.ndarray]:
    cmd = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-i",
        url,
        "-vf",
        f"fps={sample_fps}",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "bgr24",
        "pipe:1",
    ]
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    frame_size = width * height * 3
    frame_idx = 0
    try:
        assert process.stdout is not None
        while True:
            data = process.stdout.read(frame_size)
            if not data or len(data) < frame_size:
                break
            frame = np.frombuffer(data, dtype=np.uint8).reshape(
                (height, width, 3)
            )
            ts = frame_idx / sample_fps
            yield ts, frame
            frame_idx += 1
    finally:
        if process.stdout:
            process.stdout.close()
        if process.stderr:
            stderr = process.stderr.read().decode("utf-8").strip()
            if stderr:
                logging.debug("ffmpeg stderr: %s", stderr)
            process.stderr.close()
        process.wait()


def sample_yolo_boxes(
    model: YOLO,
    video_url: str,
    base_fps: float,
    max_fps: float,
    conf: float,
    motion_threshold: float,
) -> tuple[dict, dict]:
    width, height, fps = ffprobe_video(video_url)
    frames_out = []
    union = None
    sampled = 0
    last_gray = None
    frame_idx = 0

    if max_fps < base_fps:
        max_fps = base_fps
    interval_frames = max(int(round(max_fps / base_fps)), 1)

    for ts, frame in stream_frames(video_url, width, height, max_fps):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        motion = False
        if last_gray is not None:
            diff = cv2.absdiff(gray, last_gray)
            score = float(np.mean(diff))
            motion = score >= motion_threshold
        last_gray = gray

        keep = motion or (frame_idx % interval_frames == 0)
        frame_idx += 1
        if not keep:
            continue

        results = model.predict(
            source=frame, classes=[0], conf=conf, verbose=False
        )
        boxes = []
        for box in results[0].boxes:
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            score = float(box.conf[0])
            boxes.append(
                {
                    "x": x1 / width,
                    "y": y1 / height,
                    "w": (x2 - x1) / width,
                    "h": (y2 - y1) / height,
                    "score": score,
                    "class": "person",
                }
            )
            if union is None:
                union = [x1, y1, x2, y2]
            else:
                union[0] = min(union[0], x1)
                union[1] = min(union[1], y1)
                union[2] = max(union[2], x2)
                union[3] = max(union[3], y2)

        if boxes:
            frames_out.append({"t": round(ts, 3), "boxes": boxes})
        sampled += 1

    raw = {
        "frames": frames_out,
        "sample_fps": base_fps,
        "max_fps": max_fps,
        "motion_threshold": motion_threshold,
        "video_fps": fps,
        "frame_count": sampled,
    }

    metadata = {}
    if union is not None:
        metadata["crop"] = {
            "x": union[0] / width,
            "y": union[1] / height,
            "w": (union[2] - union[0]) / width,
            "h": (union[3] - union[1]) / height,
        }
    return raw, metadata


def process_job(
    base_url: str,
    worker_id: str,
    job: dict,
    model: YOLO,
    base_fps: float,
    max_fps: float,
    conf: float,
    motion_threshold: float,
    model_name: str,
    version: str,
) -> None:
    job_id = job["job_id"]
    event_id = job["event_id"]
    analysis_type = job["analysis_type"]
    tracer = trace.get_tracer("vision_worker") if OTEL_AVAILABLE else None

    if analysis_type != "yolo_boxes":
        logging.warning(
            "job %s event %s unsupported analysis_type=%s",
            job_id,
            event_id,
            analysis_type,
        )
        post_json(
            urljoin(base_url, "/api/vision/fail"),
            {
                "worker_id": worker_id,
                "job_id": job_id,
                "error": f"unsupported analysis_type {analysis_type}",
            },
        )
        return

    video_url = job["video_url"]
    if video_url.startswith("/"):
        video_url = urljoin(base_url, video_url)

    start = time.time()
    logging.info("job %s event %s running yolo", job_id, event_id)
    if tracer:
        with tracer.start_as_current_span("yolo_infer") as span:
            span.set_attribute("event_id", event_id)
            raw, metadata = sample_yolo_boxes(
                model, video_url, base_fps, max_fps, conf, motion_threshold
            )
    else:
        raw, metadata = sample_yolo_boxes(
            model, video_url, base_fps, max_fps, conf, motion_threshold
        )

    duration_ms = int((time.time() - start) * 1000)
    logging.info(
        "job %s event %s finished in %dms",
        job_id,
        event_id,
        duration_ms,
    )

    post_json(
        urljoin(base_url, "/api/vision/result"),
        {
            "worker_id": worker_id,
            "job_id": job_id,
            "event_id": event_id,
            "analysis_type": analysis_type,
            "duration_ms": duration_ms,
            "raw_response": raw,
            "model": model_name,
            "version": version,
            "hash": None,
            "metadata": metadata or None,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--max-jobs", type=int, default=4)
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--base-fps", type=float, default=1.0)
    parser.add_argument("--max-fps", type=float, default=2.0)
    parser.add_argument("--conf", type=float, default=0.25)
    parser.add_argument("--motion-threshold", type=float, default=5.0)
    parser.add_argument("--model", default="yolov8n.pt")
    parser.add_argument("--model-name", default="yolov8n")
    parser.add_argument("--model-version", default="1")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    setup_logging(args.log_level)
    setup_tracing(f"zumblezay-vision-worker-{args.worker_id}")
    model = YOLO(args.model)

    while True:
        logging.debug("polling for jobs")
        claim = post_json(
            urljoin(base_url, "/api/vision/claim"),
            {
                "worker_id": args.worker_id,
                "max_jobs": args.max_jobs,
                "lease_seconds": args.lease_seconds,
            },
        )
        jobs = claim.get("jobs", [])

        if not jobs:
            logging.debug("no jobs available")
            if args.once:
                break
            time.sleep(args.poll_seconds)
            continue

        for job in jobs:
            try:
                logging.info(
                    "claimed job %s event %s",
                    job.get("job_id"),
                    job.get("event_id"),
                )
                process_job(
                    base_url,
                    args.worker_id,
                    job,
                    model,
                    args.base_fps,
                    args.max_fps,
                    args.conf,
                    args.motion_threshold,
                    args.model_name,
                    args.model_version,
                )
            except Exception as exc:
                logging.exception(
                    "job %s event %s failed: %s",
                    job.get("job_id"),
                    job.get("event_id"),
                    exc,
                )
                post_json(
                    urljoin(base_url, "/api/vision/fail"),
                    {
                        "worker_id": args.worker_id,
                        "job_id": job["job_id"],
                        "error": str(exc),
                    },
                )

        if args.once:
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
