import time
import subprocess
import sys

def inject_fault(pg_data_dir):
    try:
        command = ["sudo", "pkill", "-9", "-f", "postgres: .*"]
        
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"数据库关闭成功: {result.stdout}")
        if result.stderr:
            print(f"stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"关闭数据库失败: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print("错误: 'sudo' 或 'pkill' 命令未找到。请确保它们在系统的PATH中。")
        sys.exit(1)