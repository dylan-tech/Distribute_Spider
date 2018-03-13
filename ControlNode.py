#coding:utf-8
import time
from Queue import Queue
from multiprocessing import Process
from multiprocessing.managers import BaseManager
from URLManager import URLManager
from DataOutput import DataOutput

class NodeManager(object):
    def start_Manager(self,url_q,result_q):
        """利用register方法，把url_q,resutl_q两个列队注册到网络上，callable参数关联了Queue对象"""
        BaseManager.register('get_task_queue',callable=lambda:url_q)
        BaseManager.register('get_result_queue',callable=lambda:result_q)
        """'绑定商品8001，设置验证口令'baike',这个相当于对象的初始化"""
        manager = BaseManager(address=('',8001),authkey='baike')
        """返回manager对象"""
        return manager

    def url_manager_proc(self,url_q,conn_q,root_url):
        url_manager = URLManager()
        url_manager.add_new_url(root_url)
        while True:
            while (url_manager.has_new_url()):
                new_url = url_manager.get_new_url()
                url_q.put(new_url)
                print 'old_url=',url_manager.old_url_size()
                if (url_manager.old_url_size() > 2000):
                    url_q.put('end')
                    print '控制节点发起结束通知'
                    url_manager.save_progress('new_urls.txt',url_manager.new_urls)
                    url_manager.save_progress('old_urls.tex',url_manager.old_urls)
                    return
            try:
                if not conn_q.empty():
                    urls = conn_q.get()
                    url_manager.add_new_urls(urls)
            except BaseException,e:
                time.sleep(0.1)

    def result_solve_proc(self,result_q,conn_q,store_q):
        while True:
            try:
                if not result_q.empty():
                    content = result_q.get()
                    if content['new_urls'] == 'end':
                        print '结果分析进程接收通知然后结束'
                        store_q.put('end')
                        return
                    conn_q.put(content['new_urls'])
                    store_q.put(content['data'])
                else:
                    time.sleep(0.1)
            except BaseException,e:
                time.sleep(0.1)

    def store_proc(self,store_q):
        output = DataOutput()
        while True:
            if not store_q.empty():
                data = store_q.get()
                if data == 'end':
                    print '存储进程接受通知然后结束'
                    output.output_end(output.filepath)
                    return
                output.store_data(data)
            else:
                time.sleep(0.1)

if __name__ == '__main__':
    """初始化4个队列"""
    url_q = Queue()
    result_q = Queue()
    conn_q = Queue()
    store_q = Queue()
    """创建分布式管理器"""
    node = NodeManager()
    manager = node.start_Manager(url_q,result_q)
    """创建URL管理进程，数据提取进程和数据存储进程"""
    url_manager_proc = Process(target=node.url_manager_proc,args=(url_q,conn_q,\
                            'https://baike.baidu.com/item/网络爬虫',))
    result_solve_proc = Process(target=node.result_solve_proc,args=(result_q,conn_q,store_q,))
    store_proc = Process(target=node.store_proc,args=(store_q,))

    """启动3个进程和分布式管理器"""
    url_manager_proc.start()
    result_solve_proc.start()
    store_proc.start()
    manager.get_server().server_forever()


