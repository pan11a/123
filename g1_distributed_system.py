"""
G1动捕数据记录和转发系统
场景1: 在G1本地记录关节数据(CSV/JSON)
场景2: 转发数据到Ubuntu PC的ROS2
场景3: 同时记录+转发
"""

import json
import time
import socket
import struct
import pickle
import sys
import csv
from datetime import datetime
from pathlib import Path
from enum import Enum

# ============================================================================
# 场景1: G1本地数据记录器 (g1_data_recorder.py)
# 在G1 PC上运行,记录BVH和关节数据到文件
# ============================================================================

class DataRecorder:
    """
    在G1 PC上运行的数据记录器
    功能:
    1. 接收BVH数据并重定向
    2. 记录到CSV/JSON文件
    3. 支持按episode分组
    """
    
    def __init__(self, 
                 retarget_config='./retarget.json',
                 output_dir='./recorded_data',
                 record_format='csv'):  # 'csv' or 'json' or 'both'
        """
        Args:
            retarget_config: 重定向配置文件
            output_dir: 数据输出目录
            record_format: 记录格式 ('csv', 'json', 'both')
        """
        from mocap_robotapi import (
            MCPRobot, MCPApplication, MCPSettings,
            MCPBvhRotation, MCPBvhData, MCPEventType, MCPAvatar
        )
        
        # 创建输出目录
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建episode目录
        self.episode_num = self._get_next_episode_num()
        self.episode_dir = self.output_dir / f"episode_{self.episode_num:04d}"
        self.episode_dir.mkdir(exist_ok=True)
        
        self.record_format = record_format
        
        # 加载配置
        with open(retarget_config, 'r') as f:
            self.config = json.load(f)
        
        # 创建机器人实例
        self.robot = MCPRobot(json.dumps(self.config))
        
        # 创建应用
        self.app = MCPApplication()
        settings = MCPSettings()
        settings.set_udp(7012)
        settings.set_bvh_rotation(MCPBvhRotation.XYZ)
        settings.set_bvh_data(MCPBvhData.String)
        self.app.set_settings(settings)
        
        self.joint_names = self.config['urdfJointNames']
        
        # 初始化CSV文件
        if record_format in ['csv', 'both']:
            self.csv_file = None
            self.csv_writer = None
            self._init_csv()
        
        # JSON数据缓冲
        if record_format in ['json', 'both']:
            self.json_buffer = []
        
        self.MCPEventType = MCPEventType
        self.MCPAvatar = MCPAvatar
        
        print(f"[Recorder] ========================================")
        print(f"[Recorder] 数据记录器初始化完成")
        print(f"[Recorder] Episode: {self.episode_num}")
        print(f"[Recorder] 输出目录: {self.episode_dir}")
        print(f"[Recorder] 记录格式: {record_format}")
        print(f"[Recorder] 关节数量: {len(self.joint_names)}")
        print(f"[Recorder] ========================================")
    
    def _get_next_episode_num(self):
        """获取下一个episode编号"""
        existing = [int(d.name.split('_')[1]) 
                   for d in self.output_dir.glob('episode_*') 
                   if d.is_dir()]
        return max(existing, default=-1) + 1
    
    def _init_csv(self):
        """初始化CSV文件"""
        csv_path = self.episode_dir / 'joint_data.csv'
        self.csv_file = open(csv_path, 'w', newline='')
        
        # CSV列: timestamp + 所有关节位置
        headers = ['timestamp', 'frame_id'] + self.joint_names
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=headers)
        self.csv_writer.writeheader()
        
        print(f"[Recorder] CSV文件: {csv_path}")
    
    def record_frame(self, joint_positions, timestamp, frame_id):
        """记录一帧数据"""
        
        # 记录到CSV
        if self.record_format in ['csv', 'both']:
            row = {
                'timestamp': timestamp,
                'frame_id': frame_id
            }
            for joint_name in self.joint_names:
                row[joint_name] = joint_positions.get(joint_name, 0.0)
            
            self.csv_writer.writerow(row)
            self.csv_file.flush()  # 立即写入磁盘
        
        # 记录到JSON缓冲
        if self.record_format in ['json', 'both']:
            frame_data = {
                'timestamp': timestamp,
                'frame_id': frame_id,
                'joint_positions': joint_positions
            }
            self.json_buffer.append(frame_data)
    
    def save_json(self):
        """保存JSON数据到文件"""
        if self.record_format not in ['json', 'both']:
            return
        
        json_path = self.episode_dir / 'joint_data.json'
        with open(json_path, 'w') as f:
            json.dump({
                'episode': self.episode_num,
                'joint_names': self.joint_names,
                'frames': self.json_buffer
            }, f, indent=2)
        
        print(f"[Recorder] JSON已保存: {json_path}")
        print(f"[Recorder] 总帧数: {len(self.json_buffer)}")
    
    def run(self, max_frames=None):
        """
        运行数据记录
        
        Args:
            max_frames: 最大记录帧数,None表示无限制
        """
        self.app.open()
        print(f"[Recorder] 开始记录... (按Ctrl+C停止)")
        
        frame_count = 0
        start_time = time.time()
        last_print_time = start_time
        
        try:
            while True:
                if max_frames and frame_count >= max_frames:
                    print(f"[Recorder] 达到最大帧数 {max_frames}")
                    break
                
                events = self.app.poll_next_event()
                
                for evt in events:
                    if evt.event_type == self.MCPEventType.AvatarUpdated:
                        avatar = self.MCPAvatar(evt.event_data.avatar_handle)
                        
                        # 重定向
                        self.robot.update_robot(avatar)
                        self.robot.run_robot_step()
                        
                        # 获取数据
                        json_str, timestamp = self.robot.get_robot_ros_frame_json()
                        data = json.loads(json_str)
                        
                        # 记录
                        self.record_frame(
                            data['joint_positions'],
                            time.time(),
                            frame_count
                        )
                        
                        frame_count += 1
                        
                        # 每秒打印状态
                        current_time = time.time()
                        if current_time - last_print_time >= 1.0:
                            elapsed = current_time - start_time
                            fps = frame_count / elapsed
                            print(f"[Recorder] 帧数: {frame_count}, "
                                  f"FPS: {fps:.1f}, "
                                  f"时长: {elapsed:.1f}s")
                            last_print_time = current_time
                
                time.sleep(0.001)
        
        except KeyboardInterrupt:
            print("\n[Recorder] 停止记录")
        finally:
            self._cleanup(frame_count, time.time() - start_time)
    
    def _cleanup(self, total_frames, duration):
        """清理资源"""
        # 关闭CSV
        if self.csv_file:
            self.csv_file.close()
        
        # 保存JSON
        self.save_json()
        
        # 保存元数据
        metadata = {
            'episode': self.episode_num,
            'total_frames': total_frames,
            'duration_seconds': duration,
            'fps': total_frames / duration if duration > 0 else 0,
            'joint_count': len(self.joint_names),
            'timestamp': datetime.now().isoformat()
        }
        
        meta_path = self.episode_dir / 'metadata.json'
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        self.app.close()
        
        print(f"\n[Recorder] ========================================")
        print(f"[Recorder] 记录完成!")
        print(f"[Recorder] 总帧数: {total_frames}")
        print(f"[Recorder] 时长: {duration:.1f}秒")
        print(f"[Recorder] 平均FPS: {metadata['fps']:.1f}")
        print(f"[Recorder] 数据位置: {self.episode_dir}")
        print(f"[Recorder] ========================================")


# ============================================================================
# 场景2: 数据转发到Ubuntu ROS2 (在G1 PC上运行)
# ============================================================================

class DataForwarder:
    """
    在G1 PC上运行,转发数据到Ubuntu ROS2
    """
    
    def __init__(self,
                 retarget_config='./retarget.json',
                 ubuntu_ip='192.168.123.99',
                 ubuntu_port=9999):
        from mocap_robotapi import (
            MCPRobot, MCPApplication, MCPSettings,
            MCPBvhRotation, MCPBvhData, MCPEventType, MCPAvatar
        )
        
        self.ubuntu_ip = ubuntu_ip
        self.ubuntu_port = ubuntu_port
        
        # 加载配置
        with open(retarget_config, 'r') as f:
            self.config = json.load(f)
        
        self.robot = MCPRobot(json.dumps(self.config))
        
        self.app = MCPApplication()
        settings = MCPSettings()
        settings.set_udp(7012)
        settings.set_bvh_rotation(MCPBvhRotation.XYZ)
        settings.set_bvh_data(MCPBvhData.String)
        self.app.set_settings(settings)
        
        self.joint_names = self.config['urdfJointNames']
        self.server_socket = None
        self.client_socket = None
        
        self.MCPEventType = MCPEventType
        self.MCPAvatar = MCPAvatar
        
        print(f"[Forwarder] 转发器初始化完成")
        print(f"[Forwarder] 等待Ubuntu连接: {ubuntu_port}")
    
    def start_server(self):
        """启动TCP服务器"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.ubuntu_port))
        self.server_socket.listen(1)
        
        print(f"[Forwarder] 等待Ubuntu PC连接...")
        self.client_socket, addr = self.server_socket.accept()
        print(f"[Forwarder] ✓ Ubuntu PC已连接: {addr}")
    
    def send_data(self, joint_positions, timestamp):
        """发送数据"""
        try:
            data = {
                'timestamp': timestamp,
                'joint_names': self.joint_names,
                'joint_positions': joint_positions
            }
            serialized = pickle.dumps(data)
            length = struct.pack('!I', len(serialized))
            self.client_socket.sendall(length + serialized)
            return True
        except:
            return False
    
    def run(self):
        """运行转发器"""
        self.app.open()
        self.start_server()
        
        frame_count = 0
        last_print = time.time()
        
        try:
            while True:
                events = self.app.poll_next_event()
                
                for evt in events:
                    if evt.event_type == self.MCPEventType.AvatarUpdated:
                        avatar = self.MCPAvatar(evt.event_data.avatar_handle)
                        self.robot.update_robot(avatar)
                        self.robot.run_robot_step()
                        
                        json_str, timestamp = self.robot.get_robot_ros_frame_json()
                        data = json.loads(json_str)
                        
                        if not self.send_data(data['joint_positions'], timestamp):
                            print("[Forwarder] 连接断开,重新等待...")
                            self.start_server()
                        
                        frame_count += 1
                        if time.time() - last_print >= 1.0:
                            print(f"[Forwarder] 已转发: {frame_count} 帧")
                            frame_count = 0
                            last_print = time.time()
                
                time.sleep(0.001)
        
        except KeyboardInterrupt:
            print("\n[Forwarder] 停止转发")
        finally:
            if self.client_socket:
                self.client_socket.close()
            if self.server_socket:
                self.server_socket.close()
            self.app.close()


# ============================================================================
# 场景3: 同时记录+转发
# ============================================================================

class RecorderAndForwarder:
    """同时记录到本地文件并转发到Ubuntu"""
    
    def __init__(self,
                 retarget_config='./retarget.json',
                 output_dir='./recorded_data',
                 record_format='csv',
                 ubuntu_port=9999):
        
        self.recorder = DataRecorder(retarget_config, output_dir, record_format)
        self.ubuntu_port = ubuntu_port
        self.client_socket = None
        
        # 启动TCP服务器
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', ubuntu_port))
        self.server_socket.listen(1)
        
        print(f"[RecorderForwarder] 混合模式: 本地记录 + Ubuntu转发")
    
    def wait_for_ubuntu(self):
        """等待Ubuntu连接"""
        print(f"[RecorderForwarder] 等待Ubuntu连接...")
        self.client_socket, addr = self.server_socket.accept()
        print(f"[RecorderForwarder] ✓ Ubuntu已连接: {addr}")
    
    def send_to_ubuntu(self, joint_positions, timestamp):
        """发送到Ubuntu"""
        if not self.client_socket:
            return False
        try:
            data = {
                'timestamp': timestamp,
                'joint_names': self.recorder.joint_names,
                'joint_positions': joint_positions
            }
            serialized = pickle.dumps(data)
            length = struct.pack('!I', len(serialized))
            self.client_socket.sendall(length + serialized)
            return True
        except:
            return False
    
    def run(self):
        """运行混合模式"""
        self.wait_for_ubuntu()
        self.recorder.app.open()
        
        frame_count = 0
        start_time = time.time()
        last_print = start_time
        
        try:
            while True:
                events = self.recorder.app.poll_next_event()
                
                for evt in events:
                    if evt.event_type == self.recorder.MCPEventType.AvatarUpdated:
                        avatar = self.recorder.MCPAvatar(evt.event_data.avatar_handle)
                        self.recorder.robot.update_robot(avatar)
                        self.recorder.robot.run_robot_step()
                        
                        json_str, timestamp = self.recorder.robot.get_robot_ros_frame_json()
                        data = json.loads(json_str)
                        current_time = time.time()
                        
                        # 记录到本地
                        self.recorder.record_frame(
                            data['joint_positions'],
                            current_time,
                            frame_count
                        )
                        
                        # 转发到Ubuntu
                        if not self.send_to_ubuntu(data['joint_positions'], timestamp):
                            print("[RecorderForwarder] Ubuntu断开,重连...")
                            self.wait_for_ubuntu()
                        
                        frame_count += 1
                        if current_time - last_print >= 1.0:
                            print(f"[RecorderForwarder] 帧数: {frame_count}, "
                                  f"FPS: {frame_count/(current_time-start_time):.1f}")
                            last_print = current_time
                
                time.sleep(0.001)
        
        except KeyboardInterrupt:
            print("\n[RecorderForwarder] 停止")
        finally:
            self.recorder._cleanup(frame_count, time.time() - start_time)
            if self.client_socket:
                self.client_socket.close()
            if self.server_socket:
                self.server_socket.close()


# ============================================================================
# Ubuntu端ROS2接收器 (在Ubuntu PC上运行)
# ============================================================================

class UbuntuROS2Receiver:
    """在Ubuntu PC上运行,接收并发布到ROS2"""
    
    def __init__(self, g1_ip='192.168.123.162', g1_port=9999):
        import rclpy
        from rclpy.node import Node
        from sensor_msgs.msg import JointState
        
        rclpy.init()
        self.node = Node("g1_mocap_publisher")
        self.publisher = self.node.create_publisher(JointState, "/joint_states", 10)
        
        self.g1_ip = g1_ip
        self.g1_port = g1_port
        self.socket = None
        
        print(f"[Ubuntu] ROS2接收器初始化")
        print(f"[Ubuntu] 连接目标: {g1_ip}:{g1_port}")
    
    def connect(self):
        """连接到G1"""
        while True:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.g1_ip, self.g1_port))
                print(f"[Ubuntu] ✓ 已连接到G1")
                return
            except Exception as e:
                print(f"[Ubuntu] 连接失败: {e}, 5秒后重试...")
                time.sleep(5)
    
    def receive_data(self):
        """接收数据"""
        try:
            length_data = self.socket.recv(4)
            if not length_data:
                return None
            length = struct.unpack('!I', length_data)[0]
            
            data = b''
            while len(data) < length:
                chunk = self.socket.recv(min(length - len(data), 4096))
                if not chunk:
                    return None
                data += chunk
            
            return pickle.loads(data)
        except:
            return None
    
    def publish(self, data):
        """发布到ROS2"""
        from sensor_msgs.msg import JointState
        
        msg = JointState()
        msg.header.stamp = self.node.get_clock().now().to_msg()
        msg.name = data['joint_names']
        msg.position = [
            data['joint_positions'].get(name, 0.0)
            for name in data['joint_names']
        ]
        self.publisher.publish(msg)
    
    def run(self):
        """运行接收器"""
        import rclpy
        
        self.connect()
        frame_count = 0
        last_print = time.time()
        
        try:
            while True:
                data = self.receive_data()
                if data is None:
                    print("[Ubuntu] 连接断开,重连...")
                    self.connect()
                    continue
                
                self.publish(data)
                frame_count += 1
                
                if time.time() - last_print >= 1.0:
                    print(f"[Ubuntu] 已发布: {frame_count} 帧")
                    frame_count = 0
                    last_print = time.time()
                
                rclpy.spin_once(self.node, timeout_sec=0)
        
        except KeyboardInterrupt:
            print("\n[Ubuntu] 停止")
        finally:
            if self.socket:
                self.socket.close()
            self.node.destroy_node()
            rclpy.shutdown()


# ============================================================================
# 主入口
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='G1动捕数据记录和转发系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:

1. G1本地记录 (仅保存到文件):
   python g1_mocap_system.py record --format csv

2. G1转发到Ubuntu (不记录):
   python g1_mocap_system.py forward --ubuntu-port 9999

3. 同时记录+转发:
   python g1_mocap_system.py both --format csv --ubuntu-port 9999

4. Ubuntu接收:
   python g1_mocap_system.py ubuntu --g1-ip 192.168.123.162
        """
    )
    
    parser.add_argument(
        'mode',
        choices=['record', 'forward', 'both', 'ubuntu'],
        help='运行模式'
    )
    
    parser.add_argument('--config', default='./retarget.json', help='重定向配置')
    parser.add_argument('--output-dir', default='./recorded_data', help='数据输出目录')
    parser.add_argument('--format', choices=['csv', 'json', 'both'], default='csv', help='记录格式')
    parser.add_argument('--ubuntu-port', type=int, default=9999, help='Ubuntu连接端口')
    parser.add_argument('--g1-ip', default='192.168.123.162', help='G1 IP地址')
    parser.add_argument('--max-frames', type=int, help='最大记录帧数')
    
    args = parser.parse_args()
    
    if args.mode == 'record':
        print("\n模式: 本地记录")
        recorder = DataRecorder(
            retarget_config=args.config,
            output_dir=args.output_dir,
            record_format=args.format
        )
        recorder.run(max_frames=args.max_frames)
    
    elif args.mode == 'forward':
        print("\n模式: 转发到Ubuntu")
        forwarder = DataForwarder(
            retarget_config=args.config,
            ubuntu_port=args.ubuntu_port
        )
        forwarder.run()
    
    elif args.mode == 'both':
        print("\n模式: 记录+转发")
        hybrid = RecorderAndForwarder(
            retarget_config=args.config,
            output_dir=args.output_dir,
            record_format=args.format,
            ubuntu_port=args.ubuntu_port
        )
        hybrid.run()
    
    elif args.mode == 'ubuntu':
        print("\n模式: Ubuntu ROS2接收")
        receiver = UbuntuROS2Receiver(
            g1_ip=args.g1_ip,
            g1_port=args.ubuntu_port
        )
        receiver.run()


if __name__ == '__main__':
    main()
