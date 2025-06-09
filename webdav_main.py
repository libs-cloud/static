import os
import io
import json
import traceback
from webdav4.client import Client, HTTPError
import sys
from datetime import datetime
from urllib.parse import urljoin
import time
from urllib.parse import urlparse
if '/www/server/panel/class' not in sys.path:
    sys.path.insert(0, '/www/server/panel/class')
import public

class webdav_main:
    _title = "WebDAV存储"
    _name = "webdav"

    default_backup_path = ""
    config_file = "/www/server/panel/plugin/webdav/config.conf"
    __aes_status = '/www/server/panel/plugin/webdav/aes_status'
    __before_error_msg = "ERROR: 检测到有*号，输入信息为加密信息或者信息输入不正确！请检查设置是否正确!"
    client = None

    def __init__(self):
        try:
            res = public.readFile('/www/server/panel/class/CloudStoraUpload.py')
            if '/www/server/panel/plugin/webdav' in res: 
                self.get_lib()
        except Exception as e:
            pass

        try:
            if os.path.exists(self.__aes_status) and public.readFile(self.__aes_status).strip() == 'True':
                res = self.get_config()
                self.default_backup_path = res['backup_path']
                self.connect_to_webdav(res['hostname'], res['username'], res['password'])
        except Exception as e:
            print(e)

    def connect_to_webdav(self, hostname, username, password):
        # 解析 URL 以确保协议部分存在
        parsed_url = urlparse(hostname)
        
        if not parsed_url.scheme:
            # 如果没有协议部分，默认使用 http
            hostname = 'http://' + hostname
            parsed_url = urlparse(hostname)
        elif parsed_url.scheme not in ['http', 'https']:
            # 如果协议不是 http 或 https，则返回错误
            return public.returnMsg(False, "不支持的协议类型！请使用 HTTP 或 HTTPS。")
        
        # 检查端口号是否存在并在范围内
        if parsed_url.port is not None and (parsed_url.port < 1 or parsed_url.port > 65535):
            return public.returnMsg(False, "端口号不在有效范围（1-65535）内，请检查输入的 URL。")
        
        # 创建 WebDAV 客户端连接
        self.client = Client(base_url=hostname, auth=(username, password), timeout=3600)
        self.create_directory(self.default_backup_path)

    def create_directory(self, remote_path):
        try:
            parts = remote_path.split('/')
            current_path = ""
            for part in parts:
                current_path = os.path.join(current_path, part)
                if not self.client.exists(current_path):
                    self.client.mkdir(current_path)
        except Exception as e:
            pass

    def get_config(self, get=None):
        if not os.path.exists(self.config_file):
            return {
                'hostname': '',
                'username': '',
                'password': '',
                "backup_path": self.default_backup_path
            }
        if get is not None:
            return self.get_decrypt_config(get)
        conf = public.readFile(self.config_file).split('|')
        return {
            "hostname": conf[0],
            "username": conf[1],
            "password": conf[2],
            "backup_path": conf[3]
        }
        
    def get_decrypt_config(self, get):
        try:
            conf = self.get_config()
            if not conf['hostname'] or not conf['username'] or not conf['password']: return conf
            conf['hostname'] = "{}{}{}".format(conf['hostname'][:5], '*' * 10, conf['hostname'][-5:])
            conf['username'] = "{}{}{}".format(conf['username'][:5], '*' * 10, conf['username'][-5:])
            conf['password'] = "{}{}{}".format(conf['password'][:2], '*' * 10, conf['password'][-2:])
            return conf
        except:
            print(traceback.format_exc())

    def set_config(self, get):
        try:
            if not hasattr(get, 'hostname') or not hasattr(get, 'username') or not hasattr(get, 'password') or not hasattr(get, 'backup_path'):
                return public.returnMsg(False, '参数错误，请检查!')
            hostname = get.hostname.strip()
            username = get.username.strip()
            password = get.password.strip()
            backup_path = get.backup_path.strip()
            if '*' in hostname or '*' in username or '*' in password:
                return public.returnMsg(False, self.__before_error_msg)
            if not backup_path:
                backup_path = "bt_backup"

            self.client = Client(base_url=hostname, auth=(username, password))

            add_str = "{}|{}|{}|{}".format(hostname, username, password, backup_path)
            self.write_config(add_str)
            public.writeFile(self.__aes_status, 'True')
            return public.returnMsg(True, '设置成功!')
        except Exception as e:
            print(traceback.format_exc())
            return public.returnMsg(False, '保存配置失败: {}'.format(str(e)))

    def write_config(self, conf):
        public.writeFile('/www/server/panel/webdav/aes_status', 'True')
        public.writeFile('/www/server/panel/plugin/webdav/config.conf', conf)
        return True   
    def ensure_directories_exist(self, path):
        parts = path.strip('/').split('/')
        current_path = ""
        for part in parts:
            current_path = os.path.join(current_path, part)
            if not self.client.exists(current_path):
                try:
                    self.client.mkdir(current_path)
                except HTTPError as e:
                    if e.response.status_code == 409:
                        continue
                    else:
                        # print("创建目录 {} 失败: {}".format(current_path, str(e)))
                        return False
        return True

    def upload_file(self, file_name, object_name, *args, **kwargs):
        # print("file_name", file_name)
        if "/site/" in file_name or "/database/" in file_name or "/path/" in file_name:
            if "/database/" in file_name:
                db_index = file_name.index("database") + len("database")
                object_name = file_name[db_index:].strip('/')
                object_name = "database/" + object_name.split('/')[0]
            elif "/path/" in file_name:
                object_name = "path/"
            
            file_subpath = self.get_last_two_parts_of_path(file_name)
            upload_path = os.path.join(self.default_backup_path, object_name, file_subpath)
        else:
            upload_path=object_name
        file_size = os.path.getsize(file_name)
        upload_speed = int(self.get_upload_speed_config()['msg']['upload_speed']) * 1024 * 1024  # 将上传速度从 MB/s 转换为 bps
    
        return self.upload_file_in_chunks(file_name, upload_path, chunk_size=1 * 1024 * 1024, upload_speed=upload_speed)

    def upload_file_in_chunks(self, file_path, remote_path, chunk_size=1 * 1024 * 1024, upload_speed=None):
        """
        以分块方式上传文件，并在上传完成后输出耗时和平均速度。支持限速功能。
        """
        import math

        file_size = os.path.getsize(file_path)
        total_chunks = math.ceil(file_size / chunk_size)
        chunk_index = 0
        total_uploaded = 0  # 已上传的字节数

        remote_dir = os.path.dirname(remote_path)
        if not self.ensure_directories_exist(remote_dir):
            print("远程目录 {} 不存在且无法创建".format(remote_dir))
            return False

        start_time_total = time.time()  # 开始上传的时间

        with open(file_path, 'rb') as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break
                chunk_remote_path = "{}.part{}".format(remote_path, chunk_index)
                chunk_file_obj = io.BytesIO(chunk_data)

                start_time = time.time()

                try:
                    if self.client.exists(chunk_remote_path):
                        self.client.remove(chunk_remote_path)
                    
                    self.client.request(
                        method='PUT',
                        path=chunk_remote_path,
                        headers={'Content-Length': str(len(chunk_data))},
                        content=chunk_file_obj.getvalue(),
                        timeout=3600  # 设置单次请求的超时时间，例如 120 秒
                    )

                    if not self.client.exists(chunk_remote_path):
                        print("分块 {} 上传失败，路径 {} 不存在".format(chunk_index, chunk_remote_path))
                        return False

                except Exception as e:
                    print("上传分块 {} 失败: {}".format(chunk_index, str(e)))
                    return False

                total_uploaded += len(chunk_data)
                chunk_index += 1

                if upload_speed:
                    time_elapsed = time.time() - start_time
                    time_to_sleep = len(chunk_data) / upload_speed - time_elapsed
                    if time_to_sleep > 0:
                        time.sleep(time_to_sleep)
            
            # print("上传完成，开始合并分块")
            self.merge_chunks(remote_path, total_chunks)

            duration = time.time() - start_time_total  # 总耗时
            file_size_mb = file_size / (1024 * 1024)
            average_speed = file_size_mb / duration if duration > 0 else 0

            if average_speed < 0.001:
                average_speed = 0.001  # 如果速度低于 0.001 MB/s，则显示为 0.001 MB/s

            print("|-文件上传成功, 耗时: {:.2f} 秒, 上传文件的平均速度为: {:.3f} MB/s".format(duration, average_speed))
            return True

    def merge_chunks(self, remote_path, total_chunks):
        try:
            with open('/tmp/merged_file', 'wb') as merged_file:
                for i in range(total_chunks):
                    chunk_remote_path = "{}.part{}".format(remote_path, i)
                    chunk_file_obj = io.BytesIO()
                    
                    try:
                        self.client.download_fileobj(chunk_remote_path, chunk_file_obj)
                    except Exception as e:
                        print("下载分块 {} 失败: {}".format(i, str(e)))
                        return False

                    chunk_file_obj.seek(0)
                    merged_file.write(chunk_file_obj.read())
                    
                    try:
                        self.client.remove(chunk_remote_path)
                    except Exception as e:
                        print("删除分块 {} 失败: {}".format(i, str(e)))
                        return False
            
            try:
                self.client.upload_file('/tmp/merged_file', remote_path)
            except Exception as e:
                print("上传合并后的文件失败: {}".format(str(e)))
                return False

            os.remove('/tmp/merged_file')
            # print("合并完成")        
        except Exception as e:
            if "403" in str(e):
                error_message = "上传失败: 权限不足，请检查WebDAV服务器的权限配置！"
            elif "413" in str(e):
                error_message = "上传失败: 文件大小超出服务器允许的限制，请尝试修改服务器的配置以确保文件上传成功！"
            else:
                error_message = "文件上传失败: {}".format(str(e))
            print(error_message)
            return False


        

    def delete_file(self, get):
        object_name = get.object_name
        try:
            self.client.remove(object_name)
            return public.returnMsg(True, '删除成功!')
        except Exception as e:
            # print("文件删除失败: ", str(e))
            return public.returnMsg(False, "删除失败！")

    def delete_object(self, object_name, data_type=None):
        import re
        try:
            if "path_" in object_name:
                file_subpath = object_name.split('path_')[-1]
                match = re.match(r'([a-zA-Z0-9_]+)_[0-9]{8}_[0-9]{6}_[a-zA-Z0-9]+\.tar\.gz', file_subpath)
                if match:
                    domain_name = match.group(1)
                    object_name = os.path.join(self.default_backup_path, 'path', domain_name, 'path_{}'.format(file_subpath))
            self.client.remove(object_name)
            return public.returnMsg(True, '删除成功!')
        except Exception as e:
            return public.returnMsg(False, "删除失败！")

    def create_dir(self, get):
        try:
            path = get.path
            self.client.mkdir(path)
            return public.returnMsg(True, '创建成功!')
        except Exception as e:
            return public.returnMsg(False, "创建失败！")

    def download_file(self, get):
        object_name = get.object_name
        filename = os.path.basename(object_name)
        local_path = os.path.join("/tmp", filename)
        try:
            self.client.download_file(from_path=object_name, to_path=local_path)
            return public.returnMsg(True, local_path)
        except Exception as e:
            return public.returnMsg(False, "下载文件失败: {}".format(str(e)))

    def cloud_download_file(self, get):
        file_name = get.filename
        if "/database/" in file_name:
            db_index = file_name.index("database") + len("database")
            object_name = file_name[db_index:].strip('/')
            object_name = "database/" + object_name.split('/')[0]
        elif "/path/" in file_name:
            object_name = "path/"
        elif "/mysql_bin_log/" in file_name:
            bin_log_index = file_name.index("/mysql_bin_log/") + len("/mysql_bin_log/")
            object_name = "mysql_bin_log/" + '/'.join(file_name[bin_log_index:].split('/')[:2])
        else:
            object_name = "site"
        file_subpath = self.get_last_two_parts_of_path(file_name).split("|")[0]
        download_path = os.path.join(self.default_backup_path, object_name, file_subpath)
        filename = os.path.basename(file_subpath)
        local_path = os.path.join("/tmp", filename)
        try:
            self.client.download_file(from_path=download_path, to_path=local_path)
            return public.returnMsg(True, local_path)
        except Exception as e:
            return public.returnMsg(False, "下载文件失败: {}".format(str(e)))

    def get_list(self, path):
        try:
            items = self.client.ls(path, detail=True)
            formatted_list = []
            for item in items:
                if isinstance(item, dict):
                    modified_time = item['modified'].timestamp() if isinstance(item['modified'], datetime) else 'Unknown'
                    base_url = str(self.client.base_url)
                    item_name = str(item['name'])
                    download_url = urljoin(base_url, item_name)
                    formatted_item = {
                        'name': os.path.basename(item['name']),
                        'type': 'directory' if item['type'] == 'directory' else 'file',
                        'size': item.get('content_length', 'Unknown'),
                        'modified': modified_time,
                        'download': download_url
                    }
                    formatted_list.append(formatted_item)
                else:
                    print("预期的字典项，但得到的是:", type(item))
            mlist = {'path': path, 'list': formatted_list}
            return mlist
        except Exception as e:
            return []

    def list_files(self, get):
        try:
            path = get.path
            items = self.client.ls(path, detail=True)
            formatted_list = []
            for item in items:
                if isinstance(item, dict):
                    modified_time = item['modified'].timestamp() if isinstance(item['modified'], datetime) else 'Unknown'
                    formatted_item = {
                        'name': os.path.basename(item['name']),
                        'type': 'directory' if item['type'] == 'directory' else 'file',
                        'size': item.get('content_length', 'Unknown'),
                        'modified': modified_time
                    }
                    formatted_list.append(formatted_item)
                else:
                    print("预期的字典项，但得到的是:", type(item))
            return formatted_list
        except Exception as e:
            print(e)
            return {'status': False, 'msg': '获取文件列表失败', 'list': []}

    def get_last_two_parts_of_path(self, path):
        head, tail = os.path.split(path)
        if head == '':
            return tail
        parent_tail = os.path.split(head)[1]
        if "crontab_backup" in parent_tail:
            return tail
        return os.path.join(parent_tail, tail)

    def get_lib(self):
        import json
        info = {
            "name": self._title,
            "type": "计划任务",
            "ps": "将网站或数据库打包备份到WebDAV存储",
            "status": 'false',
            "opt": "webdav",
            "module": "webdav",
            "script": "webdav",
            "Host": "服务器地址",
            "用户名": "用户名",
            "密码": "登录密码",
            "backup_path": "备份保存路径, 默认是/bt_backup",
            "check": [
                "/www/server/panel/pyenv/bin/python3.7/site-packages/boto3"
            ]
        }
        lib = '/www/server/panel/data/libList.conf'
        lib_dic = json.loads(public.readFile(lib))
        for i in lib_dic:
            if info['name'] in i['name']:
                return True
            else:
                pass
        lib_dic.append(info)
        public.writeFile(lib, json.dumps(lib_dic))
        return lib_dic
    
    def get_upload_speed_config(self,get=None):
        setting_path='{}/plugin/webdav/settings.json'.format(public.get_panel_path())
        try:
            with open(setting_path, 'r') as f:
                settings = json.load(f)
        except:
            settings = {'upload_speed': 100}
            with open(setting_path, 'w') as f:
                json.dump(settings, f)
        upload_speed = settings.get('upload_speed', 100)
        return public.returnMsg(True,{'upload_speed': upload_speed})

    def set_upload_speed_config(self, get):
        setting_path='{}/plugin/webdav/settings.json'.format(public.get_panel_path())
        upload_speed = float(get.upload_speed.strip())  # 将字符串转换为整数
        if upload_speed < 0.1:  # 检查上传速度是否低于最小值
            return public.returnMsg(False, "上传速度不能低于0.1MB/s！")
        if upload_speed > 100:  # 检查上传速度是否高于最大值
            return public.returnMsg(False, "上传速度不能高于100MB/s！")
        settings = {
            'upload_speed': upload_speed
        }
        with open(setting_path, 'w') as f:
            json.dump(settings, f)
        return public.returnMsg(True, "设置成功！")


