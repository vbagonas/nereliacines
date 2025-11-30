import subprocess
import sys

processes = []

FLASK_PORT = 8080
# DJANGO_PORT = 8000
DJANGO_PORT = 8001


try:
    python_exec = sys.executable

    # Start Django
    django_proc = subprocess.Popen(
        [python_exec, "front/manage.py", "runserver", f"0.0.0.0:{DJANGO_PORT}"]
    )
    processes.append(django_proc)

    # Start Flask
    flask_proc = subprocess.Popen(
        [python_exec, "-m", "backend.app.app"]
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