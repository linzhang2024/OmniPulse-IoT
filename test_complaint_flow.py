import sys
import os
import time
import uuid
import hashlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from server.models import (
    Base, User, UserRole, Complaint, ComplaintStatus, ComplaintReply
)

DATABASE_URL = "sqlite:///./iot_devices.db"
BASE_URL = "http://127.0.0.1:8000"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def generate_unique_username():
    return f"user_{uuid.uuid4().hex[:12]}"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

class TestUser:
    def __init__(self, username: str, password: str, user_id: str = None, role: str = None):
        self.username = username
        self.password = password
        self.user_id = user_id
        self.role = role

async def register_user(client: httpx.AsyncClient, username: str, password: str) -> TestUser:
    response = await client.post(
        f"{BASE_URL}/users/register",
        json={"username": username, "password": password}
    )
    response.raise_for_status()
    data = response.json()
    return TestUser(
        username=username,
        password=password,
        user_id=data["user_id"],
        role=data["role"]
    )

async def login_user(client: httpx.AsyncClient, username: str, password: str) -> TestUser:
    response = await client.post(
        f"{BASE_URL}/users/login",
        json={"username": username, "password": password}
    )
    response.raise_for_status()
    data = response.json()
    return TestUser(
        username=username,
        password=password,
        user_id=data["user_id"],
        role=data["role"]
    )

async def create_complaint(
    client: httpx.AsyncClient,
    user: TestUser,
    title: str,
    content: str
) -> dict:
    response = await client.post(
        f"{BASE_URL}/complaints",
        json={"title": title, "content": content},
        headers={"X-User-ID": user.user_id}
    )
    response.raise_for_status()
    return response.json()

async def get_complaints(
    client: httpx.AsyncClient,
    user: TestUser
) -> list:
    response = await client.get(
        f"{BASE_URL}/complaints",
        headers={"X-User-ID": user.user_id}
    )
    response.raise_for_status()
    return response.json()

async def get_complaint_detail(
    client: httpx.AsyncClient,
    user: TestUser,
    complaint_id: str
) -> dict:
    response = await client.get(
        f"{BASE_URL}/complaints/{complaint_id}",
        headers={"X-User-ID": user.user_id}
    )
    response.raise_for_status()
    return response.json()

async def update_complaint(
    client: httpx.AsyncClient,
    user: TestUser,
    complaint_id: str,
    reply_content: str = None,
    status: str = None
) -> dict:
    body = {}
    if reply_content is not None:
        body["reply_content"] = reply_content
    if status is not None:
        body["status"] = status
    
    response = await client.patch(
        f"{BASE_URL}/complaints/{complaint_id}",
        json=body,
        headers={"X-User-ID": user.user_id}
    )
    return response

async def test_complete_flow():
    print("=" * 70)
    print("测试投诉咨询完整流程")
    print("=" * 70)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("\n" + "=" * 70)
        print("第 1 步：创建默认管理员账户")
        print("=" * 70)
        
        try:
            response = await client.post(f"{BASE_URL}/users/create-admin")
            data = response.json()
            print(f"    管理员创建结果: {data.get('message', '已创建')}")
        except Exception as e:
            print(f"    管理员账户可能已存在: {e}")
        
        print("\n" + "=" * 70)
        print("第 2 步：注册并登录游客账户")
        print("=" * 70)
        
        guest_username = generate_unique_username()
        guest_password = "guest123"
        
        print(f"\n    注册游客账户: {guest_username}")
        guest_user = await register_user(client, guest_username, guest_password)
        print(f"    注册成功 - 用户ID: {guest_user.user_id}, 角色: {guest_user.role}")
        
        print(f"\n    登录游客账户")
        logged_in_guest = await login_user(client, guest_username, guest_password)
        print(f"    登录成功 - 用户ID: {logged_in_guest.user_id}, 角色: {logged_in_guest.role}")
        
        print("\n" + "=" * 70)
        print("第 3 步：游客提交投诉")
        print("=" * 70)
        
        complaint_title = "设备数据异常问题"
        complaint_content = "我发现设备温度传感器数据经常出现异常波动，有时候会突然跳到很高的数值，希望技术人员能够检查一下设备的连接状态和传感器校准情况。"
        
        print(f"\n    提交投诉:")
        print(f"        标题: {complaint_title}")
        print(f"        内容: {complaint_content[:80]}...")
        
        complaint = await create_complaint(
            client,
            logged_in_guest,
            complaint_title,
            complaint_content
        )
        
        print(f"    投诉创建成功:")
        print(f"        投诉ID: {complaint['complaint_id']}")
        print(f"        状态: {complaint['status']}")
        
        complaint_id = complaint["complaint_id"]
        
        print("\n" + "=" * 70)
        print("第 4 步：登录管理员账户")
        print("=" * 70)
        
        admin_username = "admin"
        admin_password = "admin123"
        
        print(f"\n    登录管理员账户: {admin_username}")
        admin_user = await login_user(client, admin_username, admin_password)
        print(f"    登录成功 - 用户ID: {admin_user.user_id}, 角色: {admin_user.role}")
        
        print("\n" + "=" * 70)
        print("第 5 步：管理员查询所有投诉")
        print("=" * 70)
        
        print(f"\n    管理员获取投诉列表:")
        complaints = await get_complaints(client, admin_user)
        print(f"    投诉数量: {len(complaints)}")
        
        target_complaint = None
        for c in complaints:
            if c["complaint_id"] == complaint_id:
                target_complaint = c
                break
        
        if target_complaint:
            print(f"\n    找到目标投诉:")
            print(f"        投诉ID: {target_complaint['complaint_id']}")
            print(f"        标题: {target_complaint['title']}")
            print(f"        状态: {target_complaint['status']}")
            print(f"        提交人: {target_complaint.get('username', '-')}")
        else:
            print(f"    警告: 未找到目标投诉")
        
        print("\n" + "=" * 70)
        print("第 6 步：测试异常情况 - 空回复拦截")
        print("=" * 70)
        
        print(f"\n    测试提交空回复 (应该返回 422 错误):")
        response = await update_complaint(
            client,
            admin_user,
            complaint_id,
            reply_content="",
            status="in_progress"
        )
        
        if response.status_code == 422:
            error_data = response.json()
            print(f"    [OK] 成功拦截空回复!")
            print(f"        状态码: {response.status_code}")
            print(f"        错误信息: {error_data.get('detail', 'Unknown')}")
        else:
            print(f"    [FAIL] 未正确拦截空回复!")
            print(f"        状态码: {response.status_code}")
        
        print("\n" + "=" * 70)
        print("第 7 步：管理员提交回复并切换状态")
        print("=" * 70)
        
        reply_content = """感谢您的反馈！我们已经安排技术人员对设备进行检查。根据初步分析，可能是以下原因导致的：
1. 传感器接线松动
2. 环境温度波动
3. 传感器需要重新校准

我们会在 24 小时内完成检查并给您回复。再次感谢您的反馈！"""
        
        print(f"\n    提交回复并切换状态为 '处理中':")
        print(f"        回复内容: {reply_content[:100]}...")
        
        response = await update_complaint(
            client,
            admin_user,
            complaint_id,
            reply_content=reply_content,
            status="in_progress"
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"    [OK] 回复提交成功!")
            print(f"        状态: {data['status']}")
            print(f"        回复数量: {len(data.get('replies', []))}")
        else:
            print(f"    [FAIL] 回复提交失败!")
            print(f"        状态码: {response.status_code}")
            return False
        
        print(f"\n    切换状态为 '已解决':")
        response = await update_complaint(
            client,
            admin_user,
            complaint_id,
            status="resolved"
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"    [OK] 状态更新成功!")
            print(f"        新状态: {data['status']}")
        else:
            print(f"    [FAIL] 状态更新失败!")
            print(f"        状态码: {response.status_code}")
            return False
        
        print("\n" + "=" * 70)
        print("第 8 步：游客查看投诉历史记录")
        print("=" * 70)
        
        print(f"\n    游客获取投诉详情 (查看管理员回复):")
        complaint_detail = await get_complaint_detail(
            client,
            logged_in_guest,
            complaint_id
        )
        
        print(f"    投诉详情:")
        print(f"        投诉ID: {complaint_detail['complaint_id']}")
        print(f"        标题: {complaint_detail['title']}")
        print(f"        状态: {complaint_detail['status']}")
        
        replies = complaint_detail.get('replies', [])
        print(f"        回复数量: {len(replies)}")
        
        for i, reply in enumerate(replies, 1):
            print(f"\n        回复 {i}:")
            print(f"            内容: {reply['content'][:100]}...")
            print(f"            时间: {reply['created_at']}")
        
        print("\n" + "=" * 70)
        print("第 9 步：验证最终状态")
        print("=" * 70)
        
        final_complaint = await get_complaint_detail(
            client,
            logged_in_guest,
            complaint_id
        )
        
        all_checks_passed = True
        
        print(f"\n    验证 1: 投诉状态应为 'resolved'")
        if final_complaint['status'] == 'resolved':
            print(f"        [OK] 通过: 状态为 'resolved'")
        else:
            print(f"        [FAIL] 失败: 状态为 '{final_complaint['status']}'")
            all_checks_passed = False
        
        print(f"\n    验证 2: 应有至少 1 条回复")
        if len(final_complaint.get('replies', [])) >= 1:
            print(f"        [OK] 通过: 有 {len(final_complaint.get('replies', []))} 条回复")
        else:
            print(f"        [FAIL] 失败: 没有回复")
            all_checks_passed = False
        
        print(f"\n    验证 3: 游客查看自己的投诉 (权限测试)")
        try:
            await get_complaint_detail(client, logged_in_guest, complaint_id)
            print(f"        [OK] 通过: 游客可以查看自己的投诉")
        except Exception as e:
            print(f"        [FAIL] 失败: {e}")
            all_checks_passed = False
        
        print("\n" + "=" * 70)
        print("第 10 步：权限测试 - 游客尝试处理投诉")
        print("=" * 70)
        
        print(f"\n    测试游客是否可以更新投诉 (应该返回 403 错误):")
        response = await update_complaint(
            client,
            logged_in_guest,
            complaint_id,
            reply_content="游客尝试回复",
            status="in_progress"
        )
        
        if response.status_code == 403:
            print(f"    [OK] 成功拦截游客操作!")
            print(f"        状态码: {response.status_code}")
        else:
            print(f"    [WARNING] 注意: 游客操作返回状态码: {response.status_code}")
        
        print("\n" + "=" * 70)
        if all_checks_passed:
            print("所有测试通过! 投诉咨询流程完整实现")
        else:
            print("部分测试失败，请检查代码")
        print("=" * 70)
        
        return all_checks_passed

async def main():
    print("\n" + "=" * 70)
    print("投诉咨询模块自动化测试")
    print("=" * 70)
    print(f"\n测试说明:")
    print("  1. 创建默认管理员账户 (admin/admin123)")
    print("  2. 注册并登录游客账户")
    print("  3. 游客提交投诉")
    print("  4. 管理员登录并查询投诉")
    print("  5. 测试空回复拦截 (422 错误)")
    print("  6. 管理员提交回复并切换状态")
    print("  7. 游客查看投诉历史记录")
    print("  8. 验证最终状态")
    print("  9. 权限测试")
    
    try:
        success = await test_complete_flow()
        return 0 if success else 1
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import asyncio
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
