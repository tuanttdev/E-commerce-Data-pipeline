import subprocess

def get_windows_host_ip():
    # Chạy lệnh shell để lấy dòng default route
    output = subprocess.check_output(['ip', 'route'], text=True)
    for line in output.splitlines():
        if line.startswith('default via'):
            parts = line.split()
            return parts[2]  # IP nằm ở vị trí thứ 3

