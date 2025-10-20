import subprocess

processes = []

FLASK_PORT = 8080
DJANGO_PORT = 8000


try:
    # Start Django
    django_proc = subprocess.Popen(
        ["python", "front/manage.py", "runserver", "0.0.0.0:8000"]
    )
    processes.append(django_proc)

    # Start Flask
    flask_proc = subprocess.Popen(
        ["python", "-m", "backend.app.app"]
    )
    processes.append(flask_proc)

    print(f"Both servers started successfully!")
    print(f"Django frontend: http://localhost:{DJANGO_PORT}")
    print(f"Flask backend: http://localhost:{FLASK_PORT}")
    print(f"Flask docs: http://localhost:{FLASK_PORT}/docs")
    print("Press Ctrl+C to stop both servers...")   

    # Wait for both
    for p in processes:
        p.wait()

except KeyboardInterrupt:
    print("\nStopping all services...")
    for p in processes:
        p.terminate()