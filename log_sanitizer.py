import os
import re
import json
from datetime import datetime

def sanitize_phone_number(match):
    """
    手机号脱敏：保留前3位和后4位，中间用****替换
    例如：13812345678 -> 138****5678
    """
    phone = match.group(0)
    if len(phone) == 11:
        return phone[:3] + "****" + phone[-4:]
    return phone

def sanitize_id_card(match):
    """
    身份证号脱敏：
    - 15位身份证：保留前6位和后3位，中间用******替换
    - 18位身份证：保留前6位和后4位，中间用********替换
    例如：110101199001011234 -> 110101********1234
    """
    id_card = match.group(0)
    if len(id_card) == 15:
        return id_card[:6] + "******" + id_card[-3:]
    elif len(id_card) == 18:
        return id_card[:6] + "********" + id_card[-4:]
    return id_card

def process_log_file(file_path, output_path):
    """
    处理单个日志文件，脱敏敏感信息并保存到输出路径
    返回该文件中发现的敏感信息数量
    """
    # 定义正则表达式模式
    # 手机号：11位数字，以1开头，第二位为3-9
    phone_pattern = re.compile(r'\b1[3-9]\d{9}\b')
    
    # 身份证号：15位或18位数字（18位最后一位可以是X）
    id_card_pattern = re.compile(r'\b\d{17}[\dXx]\b|\b\d{15}\b')
    
    sensitive_count = 0
    
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 统计敏感信息数量
        phone_matches = phone_pattern.findall(content)
        id_card_matches = id_card_pattern.findall(content)
        sensitive_count = len(phone_matches) + len(id_card_matches)
        
        # 脱敏处理
        # 先处理手机号
        sanitized_content = phone_pattern.sub(sanitize_phone_number, content)
        # 再处理身份证号
        sanitized_content = id_card_pattern.sub(sanitize_id_card, sanitized_content)
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 保存脱敏后的内容
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(sanitized_content)
        
        print(f"处理完成: {os.path.basename(file_path)} -> 发现 {sensitive_count} 条敏感信息")
        
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
        return 0
    
    return sensitive_count

def main():
    """
    主函数：处理 logs/ 目录下的所有 .log 文件
    """
    # 定义目录路径
    input_dir = os.path.join(os.getcwd(), 'logs')
    output_dir = os.path.join(os.getcwd(), 'dist')
    
    # 确保输入目录存在
    if not os.path.exists(input_dir):
        print(f"错误: 输入目录不存在: {input_dir}")
        return
    
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 统计信息
    total_files = 0
    total_sensitive = 0
    processed_files = []
    
    # 查找所有 .log 文件
    log_files = [f for f in os.listdir(input_dir) if f.endswith('.log')]
    
    if not log_files:
        print(f"在 {input_dir} 中未找到 .log 文件")
    else:
        print(f"找到 {len(log_files)} 个日志文件，开始处理...")
        print("=" * 50)
        
        # 处理每个文件
        for filename in log_files:
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)
            
            # 处理文件
            sensitive_count = process_log_file(input_path, output_path)
            
            # 更新统计
            total_files += 1
            total_sensitive += sensitive_count
            processed_files.append({
                "filename": filename,
                "sensitive_count": sensitive_count
            })
        
        print("=" * 50)
    
    # 生成 summary.json
    summary = {
        "processing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_directory": input_dir,
        "output_directory": output_dir,
        "total_files_processed": total_files,
        "total_sensitive_information_found": total_sensitive,
        "files_detail": processed_files
    }
    
    summary_path = os.path.join(output_dir, 'summary.json')
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"统计信息已保存到: {summary_path}")
    except Exception as e:
        print(f"保存 summary.json 时出错: {str(e)}")
    
    # 打印最终统计
    print("\n" + "=" * 50)
    print("处理完成！")
    print(f"处理文件数量: {total_files}")
    print(f"发现敏感信息总数: {total_sensitive}")
    print("=" * 50)

if __name__ == "__main__":
    main()