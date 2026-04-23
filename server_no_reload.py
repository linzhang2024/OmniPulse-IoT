import uvicorn
import socket
import subprocess
import os
import sys
import time

DEFAULT_PORT = 8000
BACKUP_PORTS = [8001, 8002, 8003, 8004, 8005]

def is_port_in_use(port: int) -> bool:
    """
    检测端口是否被占用
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            result = s.connect_ex(('127.0.0.1', port))
            return result == 0
        except:
            return False

def get_process_using_port(port: int):
    """
    获取占用指定端口的进程信息（Windows 平台）
    """
    try:
        result = subprocess.run(
            ['netstat', '-ano', '-p', 'TCP'],
            capture_output=True,
            text=True,
            timeout=5
        )
        lines = result.stdout.split('\n')
        
        for line in lines:
            parts = line.strip().split()
            if len(parts) >= 5:
                local_addr = parts[1]
                if f':{port}' in local_addr:
                    pid = parts[-1]
                    return pid
    except Exception as e:
        print(f"[Port Check] 获取进程信息失败: {e}")
    
    return None

def kill_process(pid: str) -> bool:
    """
    终止指定 PID 的进程
    """
    try:
        subprocess.run(
            ['taskkill', '/F', '/PID', pid],
            capture_output=True,
            timeout=5
        )
        print(f"[Port Check] 已终止进程 PID: {pid}")
        time.sleep(1)
        return True
    except Exception as e:
        print(f"[Port Check] 终止进程失败: {e}")
        return False

def find_available_port(start_port: int = DEFAULT_PORT) -> tuple:
    """
    查找可用端口
    返回: (port, is_backup)
    """
    print(f"\n{'='*60}")
    print(f"[Port Check] 端口检测启动")
    print(f"{'='*60}")
    
    if is_port_in_use(start_port):
        print(f"[Port Check] 端口 {start_port} 已被占用")
        
        pid = get_process_using_port(start_port)
        if pid:
            print(f"[Port Check] 占用进程 PID: {pid}")
            
            print(f"[Port Check] 尝试终止占用进程...")
            if kill_process(pid):
                if not is_port_in_use(start_port):
                    print(f"[Port Check] 端口 {start_port} 已释放成功")
                    return (start_port, False)
                else:
                    print(f"[Port Check] 端口 {start_port} 仍然被占用")
            else:
                print(f"[Port Check] 无法终止进程，将尝试备用端口")
        else:
            print(f"[Port Check] 无法确定占用进程，将尝试备用端口")
        
        for port in BACKUP_PORTS:
            print(f"[Port Check] 尝试备用端口: {port}")
            if not is_port_in_use(port):
                print(f"[Port Check] 备用端口 {port} 可用")
                return (port, True)
        
        print(f"[Port Check] 所有备用端口都被占用")
        print(f"[Port Check] 请手动关闭占用端口的程序后重试")
        return (None, False)
    else:
        print(f"[Port Check] 端口 {start_port} 可用")
        return (start_port, False)

def run_server():
    """
    启动服务器
    """
    port, is_backup = find_available_port(DEFAULT_PORT)
    
    if port is None:
        print(f"\n[ERROR] 无法找到可用端口，服务器启动失败")
        print(f"[ERROR] 请检查以下端口是否被占用: {DEFAULT_PORT}, {BACKUP_PORTS}")
        input("按 Enter 键退出...")
        sys.exit(1)
    
    if is_backup:
        print(f"\n[WARNING] 使用备用端口: {port}")
        print(f"[WARNING] 原端口 {DEFAULT_PORT} 被占用")
    
    print(f"\n{'='*60}")
    print(f"[Server] 服务器启动配置")
    print(f"{'='*60}")
    print(f"[Server] 主机: 127.0.0.1")
    print(f"[Server] 端口: {port}")
    print(f"[Server] 访问地址: http://127.0.0.1:{port}")
    print(f"{'='*60}\n")
    
    try:
        uvicorn.run(
            "server.main:app",
            host="127.0.0.1",
            port=port,
            reload=False
        )
    except KeyboardInterrupt:
        print(f"\n[Server] 服务器已停止")
    except Exception as e:
        print(f"\n[ERROR] 服务器启动失败: {e}")
        input("按 Enter 键退出...")
        sys.exit(1)

if __name__ == "__main__":
    run_server()
