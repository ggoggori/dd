import subprocess

def run_send_email_batch():
    cmd = [
        "docker", "exec", "django_web",
        "bash", "-lc", "bash /app/scripts/send_email_batch.sh"
    ]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace', bufsize=1)

    assert p.stdout is not None

    for line in p.stdout:
        print(line, end="")
    
    rc=p.wait()
    p.stdout.close()

    if rc!= 0:
        raise RuntimeError(f"batch faield: rc={rc}")

run_send_email_batch()